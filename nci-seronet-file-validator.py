import boto3                    #used to connect to aws servies
from io import BytesIO          #used to convert file into bytes in order to unzip
import zipfile                  #used to unzip the incomming file
from mysql.connector import FieldType   #get the mysql field type in case formating is necessary
import mysql.connector
import datetime
import dateutil.tz
import difflib
from seronetdBUtilities import *
from seronetSnsMessagePublisher import *
import csv

def lambda_handler(event, context):
##user defined variables
    s3_client = boto3.client("s3")
    s3_resource = boto3.resource("s3")
    ssm = boto3.client("ssm")

    host_client = ssm.get_parameter(Name="db_host", WithDecryption=True).get("Parameter").get("Value")
    user_name = ssm.get_parameter(Name="lambda_db_username", WithDecryption=True).get("Parameter").get("Value")
    user_password =ssm.get_parameter(Name="lambda_db_password", WithDecryption=True).get("Parameter").get("Value")
    file_dbname = ssm.get_parameter(Name="jobs_db_name", WithDecryption=True).get("Parameter").get("Value")
#    pre_valid_db = ssm.get_parameter(Name="Prevalidated_DB", WithDecryption=True).get("Parameter").get("Value")
    TopicArn_Success = ssm.get_parameter(Name="TopicArn_Success", WithDecryption=True).get("Parameter").get("Value")
    TopicArn_Failure = ssm.get_parameter(Name="TopicArn_Failure", WithDecryption=True).get("Parameter").get("Value")
    
    eastern = dateutil.tz.gettz('US/Eastern')                                   #converts time into Eastern time zone (def is UTC)
    file_validation_date = datetime.datetime.now(tz=eastern).strftime("%H-%M-%S-%m-%d-%Y")

    list_of_valid_file_names = [("Demographic_Data.csv",["Demographic_Data","Comorbidity","Prior_Covid_Outcome","Submission_MetaData"]),
         ("Assay_Metadata.csv",["Assay_Metadata"]),
         ("Assay_Target.csv",["Assay_Target"]),
         ("Biospecimen_Metadata.csv",["Biospecimen","Collection_Tube"]),
         ("Prior_Test_Results.csv",["Prior_Test_Result"]),
         ("Aliquot_Metadata.csv",["Aliquot","Aliquot_Tube"]),
         ("Equipment_Metadata.csv",["Equipment"]),
         ("Confirmatory_Test_Results.csv",["Confirmatory_Test_Result"]),
         ("Reagent_Metadata.csv",["Reagent"]),
         ("Consumable_Metadata.csv",["Consumable"]),
         ("Submission_Metadata.csv",[])]
                                
    Validation_Type = "DB_Mode"
    if 'testMode' in event:
        if event['testMode']=="On":         #if testMode is off treats as DB mode with manual trigger
            Validation_Type = "Test_Mode"
#####################################################################################################################        
## connect to the mySQL jobs table and pull records needed to process
    conn = None
    sql_connect = None
    try:
        conn = mysql.connector.connect(user=user_name, host=host_client, password=user_password, database=file_dbname)
        print("SUCCESS: Connection to RDS mysql instance succeeded for " + file_dbname)
        sql_connect = conn.cursor(prepared=True)
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
        display_outout = "Yes"
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
            Results_key = CBC_submission_name + '/' + CBC_submission_date + '/' + sub_folder + "/Validation_Results/"
            Unzipped_key = CBC_submission_name + '/' + CBC_submission_date + '/' + sub_folder + "/UnZipped_Files/"

            first_folder_cut = full_bucket_name.find('/')  # only seperate on first '/' to get bucket and key info
            if first_folder_cut > -1:
                org_key_name = full_bucket_name[(first_folder_cut + 1):]  # bucket the file is located in
                bucket_name = full_bucket_name[:(first_folder_cut)]  # name of the file with in the bucket
            #####################################################################################################################
            print("## FileName Found    folder name :: " + bucket_name + "    key name :: " + org_key_name)
            submission_error_list = [['File_Name', 'Column_Name', 'Error_Message']]
            error_value, meta_error_msg, zip_obj = check_if_zip(s3_resource, bucket_name, org_key_name)

            print("error value: " + str(error_value) + " " + meta_error_msg)
            if error_value > 0:
                lambda_path = write_error_messages(Results_key, "Result_Message.txt", "text", meta_error_msg)
                s3_resource.meta.client.upload_file(lambda_path, folder_name, Results_key)

            if error_value == 0:  # only examime contents of file if sucessfully unziped
                full_name_list = zip_obj.namelist()

                foreign_key_level = [0] * len(
                    full_name_list)  # assign each file a level based on forienn key relationships

                for filename in enumerate(full_name_list):
                    if filename[1] in ['Demographic_Data.csv', 'Assay_Metadata.csv']:
                        foreign_key_level[filename[0]] = 0
                    elif filename[1] in ['Assay_Target.csv', 'Biospecimen_Metadata.csv', 'Prior_Test_Results.csv']:
                        foreign_key_level[filename[0]] = 1
                    elif filename[1] in ['Consumable.csv', 'Equipment_Metadata.csv', 'Reagent_Metadata.csv',
                                         'Aliquot_Metadata.csv', 'Confirmatory_Test_Results.csv']:
                        foreign_key_level[filename[0]] = 2
                    current_name = filename[1]

                    # move unziped files from temp storage back into orgional bucket
                    s3_resource.meta.client.upload_fileobj(zip_obj.open(current_name), Bucket=folder_name,Key=Unzipped_key + current_name)

                    if (current_name.find('.csv') > 0) == False:
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

                sort_idx = sorted(range(len(foreign_key_level)), key=lambda k: foreign_key_level[k])
                sort_idx = [int(l) for l in sort_idx]
                full_name_list = [full_name_list[l] for l in sort_idx]

                if len(
                        submission_error_list) > 1:  # if errors were found, remove these file names before moving to next step
                    error_files = [i[1][0] for i in enumerate(submission_error_list)]
                    error_files = error_files[1:]
                    full_name_list = [i for i in full_name_list if i not in error_files]
                if len(error_files) > 1:
                    error_files = list(set(error_files))

                submit_CBC, submit_to_file, file_to_submit, submit_validation_type = get_submission_metadata(s3_client,
                                                                                                             folder_name,
                                                                                                             Unzipped_key,
                                                                                                             full_name_list)

                if len(submit_CBC) == 0:
                    error_msg = "Submission_Metadata.csv was not found in submission zip file"
                    submission_error_list.append(["submission_metadata.csv", "All Columns", error_msg])
                if len(file_to_submit) > 0:
                    for i in file_to_submit:
                        if i != "Submission_Metadata.csv":
                            error_msg = "file name was found in the submitted zip, but was not checked in submission metadata.csv"
                            submission_error_list.append(["submission_metadata.csv", i, error_msg])
                if len(submit_to_file) > 0:
                    for i in submit_to_file:
                        error_msg = "file name was checked in submission metadata.csv, but was not found in the submitted zip file"
                        submission_error_list.append(["submission_metadata.csv", i, error_msg])

                if len(submission_error_list) > 1:
                    meta_error_msg = "File is a valid Zipfile. However there were " + str(len(
                        submission_error_list) - 1) + " errors found in the submission.  A CSV file has been created contaning these errors"
                else:
                    meta_error_msg = "File is a valid Zipfile. No errors were found in submission. Files are good to proceed to Data Validation"

                lambda_path = write_error_messages(Results_key, "Result_Message.txt", "text", meta_error_msg)
                s3_resource.meta.client.upload_file(lambda_path, folder_name, Results_key)

            if (error_value == 0):
                result_location = "NULL"
            else:
                result_location = folder_name + "/" + Results_key + "Result_Message.txt"

            if len(submission_error_list) > 1:  # if submission errors are found, write coresponding csv file
                lambda_path = write_error_messages(Results_key, "Error_Results.csv", "csv", submission_error_list)
                s3_resource.meta.client.upload_file(lambda_path, folder_name, Results_key)
                result_location = folder_name + "/" + Results_key + "Error_Results.csv"
            ############################################################################################################################
            if error_value > 0:
                validation_status_list.append('FILE_VALIDATION_Failure')
                validation_file_location_list.append(result_location)
                batch_validation_status = "Batch_Validation_FAILURE"

            if len(submission_error_list) > 1:
                batch_validation_status = "Batch_Validation_FAILURE"
            else:
                batch_validation_status = "Batch_Validation_SUCCESS"

            if Validation_Type == "Test_Mode":  # if in test mode do not write the to the submission file
                print("Validation is being run in TestMode, NOT writting to submission table")
                submission_index = 12345  # from the test case number
            else:
                submission_index = write_submission_table(conn, sql_connect,org_file_id,
                                                          batch_validation_status, submit_validation_type, result_location)
            ################################################################################################################
            print(submission_index)

            validation_status_list, validation_file_location_list = update_validation_status(error_files,
                                                                                             'FILE_VALIDATION_Failure',
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
            if display_outout == "Yes":
                new_key = CBC_submission_name + '/' + CBC_submission_date + '/' + sub_folder + "/" + zip_file_name
                move_submit_file_to_subfolder(Validation_Type, s3_client, bucket_name, org_key_name, new_key)

                if error_value == 0:
                    full_name_list = error_files + full_name_list;
                else:
                    full_name_list = ["Result_Message.txt"]

                result = {'Error_Message': meta_error_msg, 'org_file_id': str(org_file_id),
                          'file_status': 'FILE_Processed',
                          'validation_file_location_list': validation_file_location_list,
                          'validation_status_list': validation_status_list, 'full_name_list': full_name_list,
                          'previous_function': "prevalidator", 'org_file_name': zip_file_name}
                update_jobs_table_write_to_slack(sql_connect,Validation_Type,org_file_id,full_bucket_name,eastern,result,row_data,TopicArn_Success,TopicArn_Failure)
    ###################################################################################################################
    except Exception as e: 
        print(e)
        print("ERROR: Unexpected error: Could not connect to MySql instance.")
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
    if Validation_Type == "Test_Mode":
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
    if Validation_Type == "Test_Mode":
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

def import_data_into_table (valid_dbname,table_name,current_row,header_row,sql_connect,conn,CBC_submission_name,CBC_submission_time = "None_Provided"):
    query_str = ("show index from `%s` where Key_name = 'PRIMARY';" %(table_name))
    sql_connect.execute(query_str)
    
    query_res =sql_connect.rowcount     #number of primary keys
    rows = sql_connnect.fetchall()
    
    dup_counts = check_for_dup_primary_keys(sql_connect,valid_dbname,current_row,header_row,table_name,query_res,rows)

    query_str = "select * from `%s`" %(table_name)
    sql_connect.execute(query_str)
    desc = sql_connect.description
    query_res =sql_connect.rowcount

    column_names_list = [];         column_type_list = [];
    for col_name in desc:
        column_names_list.append(col_name[0]);
        column_type_list.append(FieldType.get_info(col_name[1]))

    res_cols = [];          res_head = [];

    for val in enumerate(column_names_list):
        if val[1] in header_row:
            match_idx = header_row.index(val[1])
            res_cols.append(match_idx)

    for val in enumerate(header_row):
        if val[1] in column_names_list:
            match_idx = column_names_list.index(val[1])
            res_head.append(match_idx)

    string_1 =  "INSERT INTO `" + valid_dbname + "`.`" + table_name + "`("
    string_2 =  "VALUE ("
    for i in res_cols:
        if header_row[i] == "Submission_ID":
            string_1 = string_1 + " "
        else:
            string_1 = string_1 + header_row[i] + ","
    string_1 = string_1[:-1] + ")"

    res_head.sort()

    for i in enumerate(res_cols):              #still need to check for boolen and date flags
        column_type = column_type_list[res_head[i[0]]]
        column_value = current_row[res_cols[i[0]]]

        if column_type.upper() == 'DATE':
            column_value = column_value.replace('/',',')
            string_2 = string_2 + "STR_TO_DATE('" +  column_value + "','%m,%d,%Y'),"
        elif column_type.upper() == 'TIME':
            string_2 = string_2 + "TIME_FORMAT('" +  column_value + "','%H:%i'),"
        elif column_type.upper() == 'TINY':
            if column_value == 'T':
                string_2 = string_2 + "1,"
            elif column_value == 'F':
                string_2 = string_2 + "0,"
        else:
            string_2 = string_2 + "'"  + column_value + "',"
    string_2 = string_2[:-1] + ")"
  
    if 'Submission_CBC' in column_names_list:
        string_1 = string_1[:-1] + ",Submission_CBC)"
        string_2 = string_2[:-1] + ",'" + CBC_submission_name + "')"

    if 'Submission_time' in column_names_list:
        string_1 = string_1[:-1] + ",Submission_time)"
        CBC_submission_time = CBC_submission_time.replace('-',',')
        CBC_submission_time = CBC_submission_time.replace('_',',')
        string_2 = string_2[:-1] + ",STR_TO_DATE('" +  CBC_submission_time + "','%H,%i,%S,%m,%d,%Y'))"
 
    query_auto = string_1 + string_2

    try:
        sql_connect.execute(query_auto)
        processing_table = sql_connect.rowcount
        if processing_table == 0:
            print("## error in submission string")
        else:
            conn.commit()
    except:
        print(query_auto)
    return dup_counts
    
def check_for_dup_primary_keys(sql_connect,valid_dbname,current_row,header_row,table_name,query_res,rows):
    dup_counts = 0
    if query_res > 0:
        if query_res == 1:      #table has 1 primary key
            if table_name == "Submission_MetaData":             #primary key does not exist in the file
                pass
            else:
                key_value_1 = current_row[header_row.index(rows[0][4])]
                query_str = ("select * from `" + valid_dbname +"`.`" + table_name + "` where %s = %s")
                sql_connect.execute(query_str,(rows[0][4],key_value_1,))
        elif query_res == 2:      #table has 2 primary keys
            key_value_1 = current_row[header_row.index(rows[0][4])]
            key_value_2 = current_row[header_row.index(rows[1][4])]
            query_str = ("select * from `" + valid_dbname +"`.`" + table_name + "` where %s = %s and %s = %s")
            sql_connect.execute(query_str,(rows[0][4],key_value_1,rows[1][4],key_value_2,))
        elif query_res == 3:      #table has 3 primary keys
            key_value_1 = current_row[header_row.index(rows[0][4])]
            key_value_2 = current_row[header_row.index(rows[1][4])]
            key_value_3 = current_row[header_row.index(rows[2][4])]
            query_str = ("select * from `" + valid_dbname +"`.`" + table_name + "` where %s = %s and %s = %s and %s = %s")
            sql_connect.execute(query_str,(rows[0][4],key_value_1,rows[1][4],key_value_2,rows[2][4],key_value_3,))
        
        if table_name not in ["Submission_MetaData"]:             #primary key does not exist in the file
            if sql_connect.rowcount > 0:
                dup_counts =  1;
    return dup_counts
                
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

def get_submission_metadata(s3_client,folder_name,Unzipped_key,full_name_list):
    submitting_center = [];            submit_to_file = [];                file_to_submit = [];
    validation_type = "NULL"
    temp_location = '/tmp/test_file'
    if "Submission_Metadata.csv" in full_name_list:
        s3_client.download_file(folder_name, Unzipped_key + "Submission_Metadata.csv", temp_location)

        file_list_name = []; file_list_value = []
        with open(temp_location, newline='') as csvfile:
            file_reader = csv.reader(csvfile, delimiter=',', quotechar='|')
            for row in file_reader:
                file_list_name.append(row[0])
                file_list_value.append(row[1])
                submit_list = [file_list_name[i[0]] for i in enumerate(file_list_value) if i[1] == 'X']
        
        validation_type = file_list_value[file_list_name.index("Submission Intent")]
        submitting_center = file_list_value[0]

        submit_to_file = [i for i in submit_list if i not in full_name_list]  #in submission, not in zip
        file_to_submit = [i for i in full_name_list if i not in submit_list]  #in zip not in submission metadata

    return submitting_center,submit_to_file,file_to_submit,validation_type
    
def update_validation_status(list_of_filesnames,validation_status,folder_name,Unzipped_key,validation_status_list,validation_file_location_list,
    conn,sql_connect,submission_index,file_validation_date,Validation_Type):
    for filename in list_of_filesnames:
        file_location = folder_name + '/' + Unzipped_key + filename
        validation_status_list.append(validation_status)          # record each validation status
        validation_file_location_list.append(file_location)

        if Validation_Type == "DB_Mode":
            write_validation_status(conn,sql_connect,submission_index,file_location,file_validation_date,validation_status)
    return validation_status_list,validation_file_location_list

def write_validation_status(conn,sql_connect,submission_file_id,file_location,file_validation_date,validation_status):
    query_str = ("INSERT INTO `table_file_validator` (submission_file_id,file_validation_file_location,file_validation_date,file_validation_status)"
                   "VALUES (%s,%s,%s,%s)")
    sql_connect.execute(query_str,(submission_file_id,file_location,file_validation_date ,validation_status,))
    conn.commit()
    
def write_submission_table(conn,sql_connect,org_file_id,batch_validation_status,submit_validation_type,result_location):
    sql_connect.execute("select current_user();")
    current_user = sql_connect.fetchall()
    
    notification_arn = 'arn:aws:lambda:us-east-1:420434175168:function:nci-seronet-file_validator'
    
    query_auto = ("INSERT INTO `table_submission_validator`(orig_file_id,submission_validation_result_location,submission_validation_notification_arn,"
    "batch_validation_status,submission_validation_type,submission_validation_updated_by) VALUE (%s,%s,%s,%s,%s,%s)")
       
    sql_connect.execute(query_auto,(org_file_id,result_location,notification_arn,batch_validation_status,submit_validation_type,current_user[0][0],))      #mysql command that will update the file-processor table
    conn.commit()
    
    exe="SELECT submission_file_id FROM `table_submission_validator` WHERE orig_file_id = %s"
    sql_connect.execute(exe,(org_file_id,))
    submission_index = sql_connect.fetchone()
    return submission_index[0]
    
def write_error_messages(new_key,result_name,file_type,error_output):
    new_key = new_key + "/" + result_name
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

    if Validation_Type == "Test_Mode":
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
    response=sns_publisher(result,TopicArn_Success,TopicArn_Failure)

def move_submit_file_to_subfolder(Validation_Type,s3_client,bucket_name,org_key_name,new_key):
    if Validation_Type == "DB_Mode":
        s3_client.copy_object(Bucket=bucket_name, Key=new_key,CopySource={'Bucket':bucket_name, 'Key':org_key_name})
        s3_client.delete_object(Bucket=bucket_name, Key=org_key_name)           # Delete original object
