import boto3                    #used to connect to aws servies
import json
from io import BytesIO          #used to convert file into bytes in order to unzip
import zipfile                  #used to unzip the incomming file
from mysql.connector import FieldType   #get the mysql field type in case formating is necessary
import mysql.connector
import datetime
import dateutil.tz
import difflib
#from seronetdBUtilities import *
from seronetSnsMessagePublisher import sns_publisher
import csv

DB_MODE = "DB_Mode"
TEST_MODE = "Test_Mode"

def lambda_handler(event, context):
##user defined variables
    s3_client = boto3.client("s3")
    s3_resource = boto3.resource("s3")
    ssm = boto3.client("ssm")

    host_client = ssm.get_parameter(Name="db_host", WithDecryption=True).get("Parameter").get("Value")
    user_name = ssm.get_parameter(Name="lambda_db_username", WithDecryption=True).get("Parameter").get("Value")
    user_password =ssm.get_parameter(Name="lambda_db_password", WithDecryption=True).get("Parameter").get("Value")
    file_dbname = ssm.get_parameter(Name="jobs_db_name", WithDecryption=True).get("Parameter").get("Value")
    pre_valid_db = ssm.get_parameter(Name="Prevalidated_DB", WithDecryption=True).get("Parameter").get("Value")
    TopicArn_Success = ssm.get_parameter(Name="TopicArn_Success", WithDecryption=True).get("Parameter").get("Value")
    TopicArn_Failure = ssm.get_parameter(Name="TopicArn_Failure", WithDecryption=True).get("Parameter").get("Value")
    
    eastern = dateutil.tz.gettz('US/Eastern')                                   #converts time into Eastern time zone (def is UTC)
    file_validation_date = datetime.datetime.now(tz=eastern).strftime("%Y-%m-%d %H:%M:%S")

    list_of_valid_file_names = [("demographic.csv",["Demographic_Data","Comorbidity","Prior_Covid_Outcome","Submission_MetaData"]),
         ("assay.csv",["Assay_Metadata"]),
         ("assay_target.csv",["Assay_Target"]),
         ("biospecimen.csv",["Biospecimen","Collection_Tube"]),
         ("prior_clinical_test.csv",["Prior_Test_Result"]),
         ("aliquot.csv",["Aliquot","Aliquot_Tube"]),
         ("equipment.csv",["Equipment"]),
         ("confirmatory_clinical_test.csv",["Confirmatory_Test_Result"]),
         ("reagent.csv",["Reagent"]),
         ("consumable.csv",["Consumable"]),
         ("submission.csv",[])]
         
    Validation_Type = DB_MODE  
    if 'testMode' in event:
        if event['testMode']=="On":         #if testMode is off treats as DB mode with manual trigger
            Validation_Type = TEST_MODE   #"Test_Mode"
#####################################################################################################################  
## connect to the mySQL jobs table and pull records needed to process
    conn = None
    sql_connect = None
    try:
        conn = mysql.connector.connect(user=user_name, host=host_client, password=user_password, database=file_dbname)
        print("SUCCESS: Connection to RDS mysql instance succeeded for " + file_dbname)
        sql_connect = conn.cursor(prepared=True)
        
        full_conn = mysql.connector.connect(user=user_name, host=host_client, password=user_password, database="INFORMATION_SCHEMA")
        print("SUCCESS: Connection to RDS mysql instance succeeded for INFORMATION_SCHEMA")
        info_sql_connect = full_conn.cursor(prepared=True)
        
        rows, desc, processing_table = get_rows_to_validate(event, conn, sql_connect, Validation_Type)
        if processing_table == 0:
            print('## Database has been checked, NO new files were found to process.  Closing the connections ##')
            sql_connect.close()
            conn.close()
            return {}
        #####################################################################################################################
        column_names_list = [];
        column_type_list = [];
        for col_name in desc:
            column_names_list.append(col_name[0])  # converts tubple names in list of names
            column_type_list.append(FieldType.get_info(col_name[1]))  # type of variable

        file_id_index = column_names_list.index('file_id')
        file_name_index = column_names_list.index('file_name')
        file_location_index = column_names_list.index('file_location')
        file_index = 1;
        display_output = True
        #####################################################################################################################
        for row_data in rows:
            full_name_list = [];
            error_files = [];  # sets an empty file list incase file is not a zip
            validation_status_list = []  # file status for each file with in the submission
            validation_file_location_list = []  # location of each file with in the submission

            current_row = list(row_data)  # Current row function is interating on
            full_bucket_name = current_row[file_location_index]

            zip_file_name = current_row[file_name_index]  # name of the submitted file
            org_file_id = current_row[file_id_index]  # value of orgional ID from SQL table
            name_parts_list = full_bucket_name.split("/")  # parse the file name path
            folder_name = name_parts_list[0]  # bucket name
            CBC_submission_name = name_parts_list[1]  # CBC name
            CBC_submission_date = name_parts_list[2]  # CBC submission date
            CBC_submission_date = str(CBC_submission_date)  # convert submission date to a string for later use

            sub_folder = "submission_%03d_%s" % (file_index, zip_file_name)
            file_index = file_index + 1
            Results_key = CBC_submission_name + '/' + CBC_submission_date + '/' + sub_folder + "/File_Validation_Results/"
            Unzipped_key = CBC_submission_name + '/' + CBC_submission_date + '/' + sub_folder + "/UnZipped_Files/"

            first_folder_cut = full_bucket_name.find('/')  # only seperate on first '/' to get bucket and key info
            if first_folder_cut > -1:
                org_key_name = full_bucket_name[(first_folder_cut + 1):]  # bucket the file is located in
                bucket_name = full_bucket_name[:(first_folder_cut)]  # name of the file with in the bucket
            #####################################################################################################################
            print("## FileName Found    folder name :: " + bucket_name + "    key name :: " + org_key_name)
            submission_error_list = [['File_Name', 'Column_Name', 'Error_Message']]
            
            error_value, meta_error_msg, zip_obj = check_if_zip(s3_resource, bucket_name, org_key_name)

            if error_value > 0:
                lambda_path = write_error_messages("Result_Message.txt", "text", meta_error_msg)
                s3_resource.meta.client.upload_file(lambda_path, folder_name, Results_key)

            if error_value == 0:  # only examime contents of file if sucessfully unziped
                org_file_list = zip_obj.namelist()
                full_name_list = zip_obj.namelist()
                for current_name in full_name_list:
                    # move unziped files from temp storage back into orgional bucket
                    s3_resource.meta.client.upload_fileobj(zip_obj.open(current_name), Bucket=folder_name,Key=Unzipped_key + current_name)

                    if not current_name.endswith('.csv'):
                        submission_error_list.append(
                            [current_name, "All Columns", "File Is not a CSV file, Unable to Process"])
                    indices = [i for i, x in enumerate(full_name_list) if
                               x == current_name]  # checks for duplicate file name entries
                    if len(indices) > 1:
                        submission_error_list.append([current_name, "All Columns", "Filename was found " + str(
                            len(indices)) + " times in submission, Can not process multiple copies"]);
                    wrong_count = len(current_name)
                    check_name_list = [i[0] for i in list_of_valid_file_names]
                    for valid in check_name_list:
                        sequence = difflib.SequenceMatcher(isjunk=None, a=current_name, b=valid).ratio()
                        matching_letters = (sequence / 2) * (len(valid) + len(current_name))
                        wrong_letters = len(current_name) - matching_letters
                        if wrong_letters < wrong_count:
                            wrong_count = wrong_letters
                    if wrong_count == 0:  # perfect match, no errors
                        pass
                    elif wrong_count <= 3:  # up to 3 letters wrong, possible mispelled
                        submission_error_list.append([current_name, "All Columns",
                                                      "Filename was possibly alterted, potenial typo, please correct and resubmit file"])
                    elif wrong_count > 3:  # more then 3 letters wrong, name not recongized
                        submission_error_list.append([current_name, "All Columns",
                                                      "Filename was not recgonized, please correct and resubmit file"])

                submit_CBC, submit_to_file, file_to_submit, submit_validation_type = get_submission_metadata(s3_client,
                                                                                                             folder_name,
                                                                                                             Unzipped_key,
                                                                                                             full_name_list)
          
                full_name_list,error_files = filter_error_list(submission_error_list,full_name_list)
                in_sub_but_wrong = [i for i in full_name_list if ((i not in check_name_list) and (i not in error_files))]
                for i in in_sub_but_wrong:
                    error_msg = "filename is not expected as part of submission."
                    submission_error_list.append([i, "All Columns", error_msg])
                if len(submit_CBC) == 0:
                    error_msg = "submission.csv was not found in submission zip file"
                    submission_error_list.append(["submission.csv", "All Columns", error_msg])
                if len(file_to_submit) > 0:
                    for i in file_to_submit:
                        if i != "submission.csv":
                            error_msg = "file name was found in the submitted zip, but was not checked in submission.csv"
                            submission_error_list.append([i, "All Columns", error_msg])
                if len(submit_to_file) > 0:
                    for i in submit_to_file:
                        error_msg = "file name was checked in submission.csv, but was not found in the submitted zip file"
                        submission_error_list.append([i,"All Columns", error_msg])
                else:
                    meta_error_msg = "File is a valid Zipfile. No errors were found in submission. Files are good to proceed to Data Validation"

                lambda_path = write_error_messages("Result_Message.txt", "text", meta_error_msg)
                s3_resource.meta.client.upload_file(lambda_path, folder_name, Results_key + "Result_Message.txt")

                full_name_list,error_files = filter_error_list(submission_error_list,full_name_list)
 
            result_location = folder_name + "/" + Results_key + "Result_Message.txt"
            if (error_value == 0):
                submission_error_list = check_column_names(s3_client,folder_name,Unzipped_key,submission_error_list,full_name_list,info_sql_connect,pre_valid_db,
                                                            check_name_list,list_of_valid_file_names)
                
                full_name_list,error_files = filter_error_list(submission_error_list,full_name_list)
            if len(submission_error_list) > 1:  # if submission errors are found, write coresponding csv file
                lambda_path = write_error_messages("Error_Results.csv", "csv", submission_error_list)
                s3_resource.meta.client.upload_file(lambda_path, folder_name, Results_key + "Error_Results.csv")
                result_location = folder_name + "/" + Results_key + "Error_Results.csv"
            ############################################################################################################################
            if error_value > 0:
                validation_status_list.append('FILE_VALIDATION_FAILURE')
                validation_file_location_list.append(result_location)
                batch_validation_status = "Batch_Validation_FAILURE"

            if len(submission_error_list) > 1:
                batch_validation_status = "Batch_Validation_FAILURE"
            else:
                batch_validation_status = "Batch_Validation_SUCCESS"

            if Validation_Type == TEST_MODE:  # if in test mode do not write the to the submission file
                print("Validation is being run in TestMode, NOT writting to submission table")
                submission_index = 12345  # from the test case number
            else:
                new_key = CBC_submission_name + '/' + CBC_submission_date + '/' + sub_folder + "/" + zip_file_name
                move_submit_file_to_subfolder(Validation_Type, s3_client, bucket_name, org_key_name, new_key)
                file_location = bucket_name + "/" + new_key

                submission_index = write_submission_table(conn, sql_connect,org_file_id,file_location,
                                                          batch_validation_status, submit_validation_type, result_location)
            if len(submission_error_list) > 1:
                meta_error_msg = ("File is a valid Zipfile. However there were " + str(len(submission_error_list) - 1) +
                    " errors found in the submission.  A CSV file has been created contaning these errors")
            ################################################################################################################
            error_files = [i for i in error_files if i in org_file_list]            #only files that were in orgional submission
            full_name_list = [i for i in full_name_list if i in org_file_list]      #only files that were in orgional submission
            
            validation_status_list, validation_file_location_list = update_validation_status(error_files,
                                                                                             'FILE_VALIDATION_FAILURE',
                                                                                             folder_name, Unzipped_key,
                                                                                             validation_status_list,
                                                                                             validation_file_location_list,
                                                                                             conn, sql_connect,
                                                                                             submission_index,
                                                                                             file_validation_date,
                                                                                             Validation_Type)

            validation_status_list, validation_file_location_list = update_validation_status(full_name_list,
                                                                                             'FILE_VALIDATION_SUCCESS',
                                                                                             folder_name, Unzipped_key,
                                                                                             validation_status_list,
                                                                                             validation_file_location_list,
                                                                                             conn, sql_connect,
                                                                                             submission_index,
                                                                                             file_validation_date,
                                                                                             Validation_Type)
            if display_output:
                if error_value == 0:
                    full_name_list = error_files + full_name_list;
                else:
                    full_name_list = ["Result_Message.txt"]

                #use these two values to control whether or not send email or slack message
                send_slack="yes"
                send_email="yes"
                result = {'Error_Message': meta_error_msg, 'org_file_id': str(org_file_id),
                          'file_status': 'FILE_Processed',
                          'validation_file_location_list': validation_file_location_list,
                          'validation_status_list': validation_status_list, 'full_name_list': full_name_list,
                          'previous_function': "prevalidator", 'org_file_name': zip_file_name,"send_slack": send_slack, "send_email": send_email}
                
                result = json.dumps(result)
                update_jobs_table_write_to_slack(sql_connect,Validation_Type,org_file_id,full_bucket_name,eastern,result,row_data,TopicArn_Success,TopicArn_Failure)
    ###################################################################################################################
    except Exception as e:
        print(e)
        print("Terminating Validation Process")

    finally:
        print('## Closing the connections ##')

        if sql_connect:
            sql_connect.close()
        if conn:
            conn.commit()
            conn.close()


    print('## All Files have been checked')
    return {}

def get_rows_to_validate(event,conn,sql_connect,Validation_Type):
    if Validation_Type == TEST_MODE:
        print("testMode is enabled")
        processing_table = len(event['S3'])
    else:
        table_sql_str = ("SELECT * FROM table_file_remover Where file_status = 'COPY_SUCCESSFUL'")
        sql_connect.execute(table_sql_str)              #executes the sql query
        
        processing_table = sql_connect.rowcount         #how many rows are returned
        rows = sql_connect.fetchall()                   #list of all the data
        
    if processing_table == 1:
        print("##There is %.f file found that needs to be processed"  %processing_table)
    elif processing_table > 1:
        print("##There are %.f files found that need to be processed"  %processing_table)
    if processing_table == 0:
        print('## Database has been checked, NO new files were found to process.  Closing the connections ##')
    if Validation_Type == TEST_MODE:
        rows=[]
        length= len(event['S3'])
        for i in range(0,length):
            contents_list = event['S3'][i].split("/")

            temporary_filename=contents_list[3]
            temporary_filename_contents=temporary_filename.split(".")
            temporary_filetype=temporary_filename_contents[len(temporary_filename_contents)-1]
            rows.append((12345, temporary_filename, event['S3'][i], "testing", "testing", "COPY_SUCCESSFUL", "testing", temporary_filetype, "submit", contents_list[1], "testing"))

            desc=(('file_id', 3, None, 11, 11, 0, False), ('file_name', 253, None, 1020, 1020, 0, True), ('file_location', 253, None, 1020, 1020, 0, True), ('file_added_on', 12, None, 19, 19, 0, True), ('file_last_processed_on', 12, None, 19, 19, 0, True), ('file_status', 253, None, 180, 180, 0, True), ('file_origin', 253, None, 180, 180, 0, True), ('file_type', 253, None, 180, 180, 0, True), ('file_action', 253, None, 180, 180, 0, True), ('file_submitted_by', 253, None, 180, 180, 0, True), ('updated_by', 253, None, 180, 180, 0, True))
    else:
        desc = sql_connect.description                  #tuple list of column names

    return rows,desc,processing_table

def check_if_zip(s3_resource,bucket_name,key_name):
    z = []
    if(str(key_name).endswith('.zip')):                                       #Zip Extension was found
        try:
            zip_obj = s3_resource.Object(bucket_name = bucket_name, key = key_name) #gets file from bucket
            buffer = BytesIO(zip_obj.get()["Body"].read())                  #creates a temp storage for file
            z = zipfile.ZipFile(buffer)                                     #unzips the contents into temp storage
            error_value = 0;
            meta_error_msg = "File was sucessfully unzipped"
        except:
            meta_error_msg = "Zip file was found, but not able to open. Unable to Process Submission"
            error_value = 1;
    else:
        meta_error_msg = "Submitted file is NOT a valid Zip file, Unable to Process Submission"
        error_value = 2

    return error_value,meta_error_msg,z
    
def get_column_names_from_SQL(full_sql_connect,pre_valid_db,current_file,check_name_list,list_of_valid_file_names):
    all_headers = list()
    values_to_ignore = ['Submission_ID','Submission_CBC','Biorepository_ID','Shipping_ID', 'Test_Agreement','Submission_time']
    if current_file in check_name_list:
        list_pos = check_name_list.index(current_file)
        table_names =  list_of_valid_file_names[list_pos][1]
        if len(table_names) > 0:
            for iterT in table_names:
                sql_query = "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = %s AND TABLE_SCHEMA = %s;"
                full_sql_connect.execute(sql_query,(iterT,pre_valid_db,))
                curr_table = full_sql_connect.fetchall()
                curr_table =  [ele[0] for ele in curr_table if ele[0] not in values_to_ignore]
                all_headers = all_headers + curr_table
    return all_headers

def get_submission_metadata(s3_client,folder_name,Unzipped_key,full_name_list):
    submitting_center = []
    submit_to_file = []
    file_to_submit = []
    valid_type = "NULL"
    temp_location = 'dummy_file'
 
    if "submission.csv" in full_name_list:
        s3_client.download_file(folder_name, Unzipped_key + "submission.csv", temp_location)

        file_list_name = []
        file_list_value = []
        with open(temp_location, newline='') as csvfile:
            file_reader = csv.reader(csvfile, delimiter=',', quotechar='|')
            for row in file_reader:
                file_list_name.append(row[0])
                file_list_value.append(row[1])
                submit_list = [file_list_name[i[0]] for i in enumerate(file_list_value) if i[1] == 'X']
        
        valid_type = file_list_value[file_list_name.index("Submission Intent")]
        submitting_center = file_list_value[0]
        
        submit_to_file = [i for i in submit_list if i not in full_name_list]  #in submission, not in zip
        file_to_submit = [i for i in full_name_list if i not in submit_list]  #in zip not in submission metadata

    return submitting_center,submit_to_file,file_to_submit,valid_type
    
def update_validation_status(list_of_filesnames,validation_status,folder_name,Unzipped_key,validation_status_list,validation_file_location_list,
                             conn,sql_connect,submission_index,file_validation_date,Validation_Type):
    for filename in list_of_filesnames:
        file_location = folder_name + '/' + Unzipped_key + filename
        validation_status_list.append(validation_status)          # record each validation status
        validation_file_location_list.append(file_location)

        if Validation_Type == DB_MODE:
            write_validation_status(conn,sql_connect,submission_index,file_location,file_validation_date,validation_status)
    return validation_status_list,validation_file_location_list

def filter_error_list(submission_error_list,full_name_list):
    error_files = []
    if len(submission_error_list) > 1:  # if errors were found, remove these file names before moving to next step
        error_files = [i[1][0] for i in enumerate(submission_error_list)]
        if len(error_files) > 1:
            error_files = error_files[1:]
            error_files = list(set(error_files))
        if len(full_name_list) > 0:
            full_name_list = [i for i in full_name_list if i not in error_files]
    return full_name_list,error_files

def write_validation_status(conn,sql_connect,submission_file_id,file_location,file_validation_date,validation_status):
    query_str = ("INSERT INTO `table_file_validator` (submission_file_id,file_validation_file_location,file_validation_date,file_validation_status)"
                   "VALUES (%s,%s,%s,%s)")
    sql_connect.execute(query_str,(submission_file_id,file_location,str(file_validation_date) ,validation_status,))
    conn.commit()
    
def write_submission_table(conn,sql_connect,org_file_id,file_location,batch_validation_status,submit_validation_type,result_location):
    sql_connect.execute("select current_user();")
    current_user = sql_connect.fetchall()
    
    notification_arn = 'arn:aws:lambda:us-east-1:420434175168:function:nci-seronet-file_validator'
    eastern = dateutil.tz.gettz('US/Eastern')                                   #converts time into Eastern time zone (def is UTC)
    file_validation_date = datetime.datetime.now(tz=eastern).strftime("%Y-%m-%d %H:%M:%S")

    query_auto = ("INSERT INTO `table_submission_validator`(orig_file_id,submission_validation_file_location,submission_validation_result_location,"
    "submission_validation_notification_arn,submission_validation_date,batch_validation_status,submission_validation_type,"
    "submission_validation_updated_by) VALUE (%s,%s,%s,%s,%s,%s,%s,%s)")
       
    sql_connect.execute(query_auto,(org_file_id,file_location,result_location,notification_arn,file_validation_date,batch_validation_status,submit_validation_type,current_user[0][0],))      #mysql command that will update the file-processor table
    conn.commit()
    
    exe="SELECT submission_file_id FROM `table_submission_validator` WHERE orig_file_id = %s"
    sql_connect.execute(exe,(org_file_id,))
    submission_index = sql_connect.fetchone()
    return submission_index[0]
    
def write_error_messages(result_name,file_type,error_output):
    lambda_path = "/tmp/" + result_name
    if file_type == "text":
        with open(lambda_path, 'w+', newline='') as txtfile:
            txtfile.write(error_output)
            txtfile.close()

    elif file_type == "csv":
        with open(lambda_path, 'w+', newline='') as csvfile:    
            csv_writer = csv.writer(csvfile)
            for file_indx in error_output:
                csv_writer.writerow(file_indx)

    return lambda_path

def update_jobs_table_write_to_slack(sql_connect,Validation_Type,org_file_id,full_bucket_name,eastern,result,row_data,TopicArn_Success,TopicArn_Failure):

    if Validation_Type == TEST_MODE:
        file_submitted_by="'"+ row_data[9]+"'"
    else:
        table_sql_str = ("UPDATE table_file_remover  Set file_status = 'FILE_Processed'"
        "Where file_status = 'COPY_SUCCESSFUL' and file_location = %s")
        
        sql_connect.execute(table_sql_str,(full_bucket_name,))           #mysql command that changes the file-action flag so file wont be used again
        processing_table=sql_connect.rowcount

        #get file_submitted_by from the database for the current file_id
        exe="SELECT * FROM table_file_remover  WHERE file_id= %s"
        sql_connect.execute(exe,(org_file_id,))
        sqlresult = sql_connect.fetchone()
        file_submitted_by="'"+sqlresult[9]+"'"

    timestampDB=datetime.datetime.now(tz=eastern).strftime('%Y-%m-%d %H:%M:%S')   # time stamp of when valiation was compelte
    result.update({'validation_date':timestampDB,'file_submitted_by':file_submitted_by})
    sns_publisher(result,TopicArn_Success,TopicArn_Failure)

def move_submit_file_to_subfolder(Validation_Type,s3_client,bucket_name,org_key_name,new_key):
    if Validation_Type == DB_MODE:
        s3_client.copy_object(Bucket=bucket_name, Key=new_key,CopySource={'Bucket':bucket_name, 'Key':org_key_name})
        s3_client.delete_object(Bucket=bucket_name, Key=org_key_name)           # Delete original object
        
def check_column_names(s3_client,folder_name,Unzipped_key,submission_error_list,full_name_list,info_sql_connect,pre_valid_db,
                       check_name_list,list_of_valid_file_names):
    for filename in full_name_list:
        all_headers = get_column_names_from_SQL(info_sql_connect,pre_valid_db,filename,check_name_list,list_of_valid_file_names)
        all_headers = list(set(all_headers))
        if len(all_headers) > 0:
            temp_location = 'dummy_file'
            s3_client.download_file(folder_name, Unzipped_key + filename, temp_location)
            with open(temp_location, newline='',encoding='utf-8=sig') as csvfile:
                file_reader = csv.reader(csvfile, delimiter=',', quotechar='|')
                row1 = list(next(file_reader))
            
            CSV_not_in_SQL = [i for i in row1 if i not in all_headers]
            SQL_not_in_CSV = [i for i in all_headers if i not in row1]
            
            for iterC in CSV_not_in_SQL:
                error_msg = "Column name was found in CSV file, but does not exist in Database"
                submission_error_list.append([filename, iterC, error_msg])
            
            for iterS in SQL_not_in_CSV:
                error_msg = "Column name exists in Database, but is missing from CSV file"
                submission_error_list.append([filename, iterS, error_msg])
    return submission_error_list