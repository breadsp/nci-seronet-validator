import json
import zipfile                 
import difflib
import os
import mysql.connector

import re
import csv
from datetime import datetime
import boto3                    #used to connect to aws services
from io import BytesIO          #used to convert file into bytes in order to unzip
import dateutil.tz    
###############################################################################################################################    
import file_validator_object 
from prior_test_result_validator import prior_test_result_validator
from demographic_data_validator  import demographic_data_validator
from Biospecimen_validator       import Biospecimen_validator
from other_files_validator       import other_files_validator
###############################################################################################################################    
def lambda_handler(event, context):
    s3_client = boto3.client("s3")
    s3_resource = boto3.resource("s3")
    ssm = boto3.client("ssm")
    
    bucket_name = event["Records"][0]["s3"]["bucket"]["name"]
    key_name = event["Records"][0]["s3"]["object"]["key"]
###############################################################################################################################    
## user variables
    list_of_valid_file_names =(['Demographic_Data.csv','Prior_Test_Results.csv','Confirmatory_Test_Results.csv','Assay_Metadata.csv',
                                'Assay_Target.csv','Biospecimen_Metadata.csv','Aliquot_Metadata.csv','Equipment_Metadata.csv','Reagent_Metadata.csv',
                                'Consumable_Metadata.csv','Submission_Metadata.csv'])
    host_client = ssm.get_parameter(Name="db_host", WithDecryption=True).get("Parameter").get("Value")
    user_name = ssm.get_parameter(Name="lambda_db_username", WithDecryption=True).get("Parameter").get("Value")
    user_password =ssm.get_parameter(Name="lambda_db_password", WithDecryption=True).get("Parameter").get("Value")
    file_dbname = ssm.get_parameter(Name="jobs_db_name", WithDecryption=True).get("Parameter").get("Value")
    pre_valid_db = ssm.get_parameter(Name="Prevalidated_DB", WithDecryption=True).get("Parameter").get("Value")

    output_bucket_name = "data-validation-output-bucket"
    CBC_submission_name = "CBC_Name" 
    eastern = dateutil.tz.gettz('US/Eastern')
###############################################################################################################################
## check if submitted file is a zip and extract contents of file

    CBC_submission_info = datetime.now(tz=eastern).strftime("%Y-%m-%d-%H-%M") + "_" + key_name
  
    try:
        new_key = CBC_submission_name+'/'+ CBC_submission_info +'/' + key_name
        copy_source = {'Bucket': bucket_name,'Key': key_name}
        s3_resource.meta.client.copy(copy_source, output_bucket_name, new_key)      #copy submitted file to output bucket
    except:
        print("why does this crash, second loop?")
        return{}

    if(str(key_name).endswith('.zip')):                                         #if submitted file is a zip, unzip contents to directory
        try:
            zip_obj = s3_resource.Object(bucket_name = bucket_name, key = key_name)
            buffer = BytesIO(zip_obj.get()["Body"].read())
            z = zipfile.ZipFile(buffer)                                 
            listOfFileNames = z.namelist()
    
            for filename in listOfFileNames:                                         #loops over each file in the zip and writes to output bucket                             
                file_info = z.getinfo(filename)
                if(str(filename).endswith('.zip')):                                 #only move files that are not *.zip, orginal zip has different location
                    print('## zip file does not need to be coppied over, not moving')    
                else:
                    new_key = CBC_submission_name+'/'+ CBC_submission_info +'/'+ 'Submitted_Files/'+ filename
                    print("##unziped file location :: " + output_bucket_name + "/" + new_key)
                    response = s3_resource.meta.client.upload_fileobj(z.open(filename),Bucket = output_bucket_name, Key = new_key)
        except:
            s3_file_path = CBC_submission_name+'/'+ CBC_submission_info
            error_msg = "Zip file was found, but not able to open. Unable to Process Submission"
            write_error_message(s3_resource,output_bucket_name,s3_file_path,error_msg)        #if submited file is not a zip, write error message
            return{}
    else:                                   
        s3_file_path = CBC_submission_name+'/'+ CBC_submission_info
        error_msg = "Submitted file is not a valid Zip file, Unable to Process Submission"
        write_error_message(s3_resource,output_bucket_name,s3_file_path)        #if submited file is not a zip, write error message
        return{}
    s3_resource.Object(bucket_name, key_name).delete()                          #once all copying has been done, delete orgional file         
########################################################################################################################
# compare contents of file to valid list of names for spelling or duplicate entries, plus check for csv extensions
    list_copy = [item.lower() for item in listOfFileNames]
    sort_idx = [i[0] for i in sorted(enumerate(list_copy), key=lambda x:x[1])]
    listOfFileNames = [listOfFileNames[i] for i in sort_idx]

    submission_error_list = [['File_Name','Column_Name','Error_Message']]
    for uni_id in listOfFileNames:
        if (uni_id.find('.csv') > 0) == False:
            submission_error_list.append([uni_id,"All Columns","File Is not a CSV file, Unable to Process"])
        indices = [i for i, x in enumerate(listOfFileNames) if x == uni_id]               #checks for duplicate file name entries
        if len(indices) > 1:
            submission_error_list .append([uni_id,"All Columns","Filename was found " + str(len(indices)) + " times in submission, Can not process multiple copies"]);
        wrong_count = len(uni_id)
        for valid in list_of_valid_file_names:
            sequence = difflib.SequenceMatcher(isjunk = None,a = uni_id,b = valid).ratio()    
            matching_letters = (sequence/2)*(len(valid) + len(uni_id))
            wrong_letters = len(uni_id) - matching_letters
           
            if wrong_letters < wrong_count:
                wrong_count = wrong_letters
        if wrong_count == 0:
            pass    #perfect match, no errors
        elif wrong_count <= 3:    #up to 3 letters wrong, possible mispelled
            submission_error_list.append([uni_id,"All Columns","Filename was possibly alterted, potenial typo, please correct and resubmit file"])
        elif wrong_count > 3: #more then 3 letters wrong, name not recongized
            submission_error_list.append([uni_id,"All Columns","Filename was not recgonized, please correct and resubmit file"])
     
    error_count = len(submission_error_list) - 1
    if (error_count) > 0:
        print("Submitted Files Names have been checked for spelling errors, extra files or duplicate entries. " + str(error_count) + " errors were found. \n")             
        print(submission_error_list)
######################################################################################################################
# open each file and compare header list to mysql database to validate all column names exist and spelled correctly
    conn = connect_to_sql_database(host_client,pre_valid_db,user_name,user_password)

    s3_file_path = CBC_submission_name+'/'+ CBC_submission_info + '/' + 'Submission_Errors'
    if conn == 0:
        write_submission_error_csv(s3_resource,output_bucket_name,s3_file_path,"Submission_Error_List.csv",submission_error_list)
        print("Unable to connect to mySQL database to preform column name validation.  Terminating Validation Process")
        return{}   
    
    if len(submission_error_list) > 1:
        error_files = [i[1][0] for i in enumerate(submission_error_list)]
        error_files = error_files[1:]
        listOfFileNames = [i for i in listOfFileNames if i not in error_files]
    
    current_error_count = 0;  current_demo = [];
    for test_name in enumerate(listOfFileNames):
        test_object = file_validator_object.Submitted_file(test_name[1],' ')                                       #create the file object
        test_object.load_csv_file(s3_client,output_bucket_name,CBC_submission_name,CBC_submission_info,test_name[1])
        
        if test_name[1]  == "Prior_Test_Results.csv" :
            mysql_table_list = ['Prior_Test_Result']
        elif test_name[1] == "Demographic_Data.csv":
            mysql_table_list = ["Demographic_Data","Prior_Covid_Outcome","Comorbidity"]   
        elif test_name[1] == "Assay_Metadata.csv":
            mysql_table_list = ["Assay_Metadata"]
        elif test_name[1] == "Assay_Target.csv" :
            mysql_table_list = ["Assay_Target"] 
        elif test_name[1] == "Confirmatory_Test_Results.csv":
            mysql_table_list = ["Confirmatory_Test_Result"]
        elif test_name[1] == "Biospecimen_Metadata.csv":
            mysql_table_list = ["Biospecimen","Collection_Tube"]
        elif test_name[1] == "Aliquot_Metadata.csv":
            mysql_table_list = ["Aliquot","Aliquot_Tube"]
        elif test_name[1] == "Equipment_Metadata.csv":
            mysql_table_list = ["Equipment"]
        elif test_name[1] == "Reagent_Metadata.csv":
            mysql_table_list = ["Reagent"]
        elif test_name[1] == "Consumable_Metadata.csv":      #consumable table does not exist at this time
            mysql_table_list = ["Consumable"] 
        elif test_name[1] == "Submission_Metadata.csv":
            submitting_center = test_object.Column_Header_List[1]
            Number_of_Research_Participants = int(test_object.Data_Table.iloc[1][1]);
            Number_of_Biospecimens = int(test_object.Data_Table.iloc[2][1])
            count_table = test_object.Data_Table[submitting_center].value_counts()
            submit_file_count = count_table[count_table.index == 'X'][0] + 1;           #add one for submision metadata
            
            submit_list = test_object.Data_Table[test_object.Data_Table[submitting_center] == 'X']['Submitting Center'].tolist()
            submit_to_file = [i for i in submit_list if i not in listOfFileNames]  #in submission, not in zip
            file_to_submit = [i for i in listOfFileNames if i not in submit_list]  #in zip not in submission metadata
            continue
        else:
            print(test_name[1] + " was not found, unable to check")
            continue
        if test_name[1] != "submission_metadata.csv":
            test_object.compare_csv_to_mysql(pre_valid_db,mysql_table_list,conn) 
        if test_name[1] == 'Demographic_Data.csv':
            current_demo =  test_object.Data_Table
            current_demo = current_demo['Research_Participant_ID'].tolist()
        
        if len(test_object.header_name_validation) > 1: 
            submission_error_list.append(test_object.header_name_validation[1:][0])
            current_error_count = current_error_count + 1;
    
    if current_error_count > 0:
        print("Column names in each submitted file have been checked for spelling errors,")
        print("extra columns, missing or duplicate entries: " + str(current_error_count) + " errors were found. \n")
        error_count = error_count + current_error_count
        
    current_error_count = 0
    if len(submitting_center) == 0:
        error_msg = "Submission_Metadata.csv was not found in submission zip file"
        submission_error_list.append(["submission_metadata.csv","All Columns",error_msg])
        current_error_count =  current_error_count + 1;
    if len(listOfFileNames) != submit_file_count:
        error_msg = ("Expected: " + str(submit_file_count) + " files. Found " + str(len(listOfFileNames)) + " files in submission")
        submission_error_list.append(["submission_metadata.csv","List of File Names",error_msg])    
        current_error_count =  current_error_count + 1;
        
    if len(file_to_submit) > 0:
        for i in file_to_submit:
            if i == "Submission_Metadata.csv":
                pass
            else:
                error_msg = "file name was found in the submitted zip, but was not checked in submission metadata.csv"
                submission_error_list.append(["submission_metadata.csv",i,error_msg])    
                current_error_count =  current_error_count + 1;
    if len(submit_to_file) > 0:
        for i in submit_to_file:
            error_msg = "file name was checked in submission metadata.csv, but was not found in the submitted zip file"
            submission_error_list.append(["submission_metadata.csv",i,error_msg])    
            current_error_count =  current_error_count + 1;
    if current_error_count > 0:
        print("Submission metadata has been checked, comparing user inputs to actual files found in submission: " +
              str(current_error_count) + " errors were found.\n")
        error_count = error_count + current_error_count

    if error_count > 0:
        print("A Total of " + str(error_count) + " errors were found in the submission file, please correct and Resubmit")
        print("Terminating Validation Process")
        conn.close()
        write_submission_error_csv(s3_resource,output_bucket_name,s3_file_path,"Submission_Error_List.csv",submission_error_list)
        return{}   
    
    print("### Submission validation was sucessfull.  No Errors were found ###")
    print("### Proceeding to check each csv file for validation ###")
    del count_table,current_error_count,file_to_submit,i,indices,list_copy,matching_letters,Number_of_Biospecimens
    del Number_of_Research_Participants,sequence,sort_idx,submit_file_count,submit_list,submit_to_file,test_name
    del test_object,uni_id,valid,wrong_count,wrong_letters
#####################################################################################################
## if no submission errors, pull key peices from sql schema and import cbc id file 
    pos_list,neg_list           = file_validator_object.get_mysql_queries(pre_valid_db,conn,1)
    assay_results,assay_target  = file_validator_object.get_mysql_queries(pre_valid_db,conn,2)
    participant_ids,biospec_ids = file_validator_object.get_mysql_queries(pre_valid_db,conn,3)
    
    if len(current_demo) > 0:
        current_demo = (set(participant_ids['Research_Participant_ID'].tolist() + current_demo))
        current_demo = [x for x in current_demo if x == x]

    sql_connect = conn.cursor()
    table_sql_str = ("SELECT * FROM `" + pre_valid_db + "`.`Seronet_CBC_ID`")
    query_res = sql_connect.execute(table_sql_str)
    rows = sql_connect.fetchall()
    
    valid_cbc_ids = [i[1] for i in enumerate(rows) if rows[i[0]][1] == submitting_center]
    print("## The CBC Name is: " + valid_cbc_ids[0][1] + " and the submission code is: " +  str(valid_cbc_ids[0][0]))
    
    s3_file_path = CBC_submission_name+'/'+ CBC_submission_info + '/' + 'Validation_Errors'
######################################################################################################################################################################################################  
    if "Prior_Test_Results.csv" in listOfFileNames:
        prior_valid_object = file_validator_object.Submitted_file("Prior_Test_Results.csv",'Research_Participant_ID')  
        prior_valid_object.load_csv_file(s3_client,output_bucket_name,CBC_submission_name,CBC_submission_info,'Prior_Test_Results.csv')
        pos_list,neg_list = file_validator_object.split_participant_pos_neg_prior(prior_valid_object,pos_list,neg_list)  
        prior_valid_object = prior_test_result_validator(prior_valid_object,neg_list,pos_list,re,valid_cbc_ids,current_demo)
        prior_valid_object.write_error_file("Prior_Test_Results_Errors_Found.csv",s3_resource,s3_file_path,output_bucket_name)
######################################################################################################################################################################################################  
    if "Demographic_Data.csv" in listOfFileNames:
        demo_data_object = file_validator_object.Submitted_file("Demographic_Data.csv",'Research_Participant_ID') 
        demo_data_object.load_csv_file(s3_client,output_bucket_name,CBC_submission_name,CBC_submission_info,'Demographic_Data.csv')

        demo_data_object = demographic_data_validator(demo_data_object,neg_list,pos_list,re,valid_cbc_ids)
        demo_data_object.write_error_file("Demographic_Data_Errors_Found.csv",s3_resource,s3_file_path,output_bucket_name)
######################################################################################################################################################################################################  
    if "Biospecimen_Metadata.csv" in listOfFileNames:
        Biospecimen_object = file_validator_object.Submitted_file("Biospecimen_Metadata.csv",'Biospecimen_ID')                  
        Biospecimen_object.load_csv_file(s3_client,output_bucket_name,CBC_submission_name,CBC_submission_info,'Biospecimen_Metadata.csv')
        Biospecimen_object.get_pos_neg_logic(pos_list,neg_list)
        biospec_ids = biospec_ids.append(Biospecimen_object.Data_Table[['Biospecimen_ID','Biospecimen_Type']])
        
        Biospecimen_object = Biospecimen_validator(Biospecimen_object,neg_list,pos_list,re,valid_cbc_ids,current_demo)
        Biospecimen_object.write_error_file("Biospecimen_Errors_Found.csv",s3_resource,s3_file_path,output_bucket_name)
############################################################################################################################### 
    if "Aliquot_Metadata.csv" in listOfFileNames:
        Aliquot_object = file_validator_object.Submitted_file("Aliquot_Metadata.csv",['Aliquot_ID','Biospecimen_ID'])                   
        Aliquot_object.load_csv_file(s3_client,output_bucket_name,CBC_submission_name,CBC_submission_info,'Aliquot_Metadata.csv')
        Aliquot_object = other_files_validator(Aliquot_object,re,valid_cbc_ids,biospec_ids,"Aliquot_Errors_Found.csv")
        Aliquot_object.write_error_file("Aliquot_Errors_Found.csv",s3_resource,s3_file_path,output_bucket_name)
###############################################################################################################################
    if "Equipment_Metadata.csv" in listOfFileNames:
        Equipment_object = file_validator_object.Submitted_file("Equipment_Metadata.csv",['Equipment_ID','Biospecimen_ID'])     
        Equipment_object.load_csv_file(s3_client,output_bucket_name,CBC_submission_name,CBC_submission_info,'Equipment_Metadata.csv')
        Equipment_object = other_files_validator(Equipment_object,re,valid_cbc_ids,biospec_ids,"Equipment_Errors_Found.csv")
        Equipment_object.write_error_file("Equipment_Errors_Found.csv",s3_resource,s3_file_path,output_bucket_name)
###############################################################################################################################
    if "Reagent_Metadata.csv" in listOfFileNames:
        Reagent_object = file_validator_object.Submitted_file("Reagent_Metadata.csv",['Biospecimen_ID','Reagent_Name'])    
        Reagent_object.load_csv_file(s3_client,output_bucket_name,CBC_submission_name,CBC_submission_info,'Reagent_Metadata.csv')
        Reagent_object = other_files_validator(Reagent_object,re,valid_cbc_ids,biospec_ids,"Reagent_Errors_Found.csv")
        Reagent_object.write_error_file("Reagent_Errors_Found.csv",s3_resource,s3_file_path,output_bucket_name)
###############################################################################################################################
    if "Consumable_Metadata.csv" in listOfFileNames:
        Consumable_object = file_validator_object.Submitted_file("Consumable_Metadata.csv",['Biospecimen_ID','Consumable_Name'])  
        Consumable_object.load_csv_file(s3_client,output_bucket_name,CBC_submission_name,CBC_submission_info,'Consumable_Metadata.csv')
        Consumable_object = other_files_validator(Consumable_object,re,valid_cbc_ids,biospec_ids,"Consumable_Errors_Found.csv")
        Consumable_object.write_error_file("Consumable_Errors_Found.csv",s3_resource,s3_file_path,output_bucket_name)
###############################################################################################################################
    print("Connection to RDS mysql instance is now closed")
    print("Validation Function is Complete")
    conn.close
    return{}
###############################################################################################################################
def connect_to_sql_database(host_client,dbname,user_name,user_password):       #connect to mySQL database 
    conn = 0
    
    try:
        conn = mysql.connector.connect(user=user_name, host=host_client, password=user_password, database= dbname)
        print("SUCCESS: Connection to RDS mysql instance succeeded for file remover tables")
    except mysql.connector.Error as err:
        print(err)
    return conn                #return connection variable to be used in queries
    

##writes text file if submitted file is not a zip or unable to open the zip file    
def write_error_message(s3_resource,output_bucket_name,s3_file_path,error_msg):
    file_name = "Submission_Error.txt"
    lambda_path = "/tmp/" + file_name
   
    with open(lambda_path, 'w+') as file:
        file.write(error_msg)
        file.close()
    
    s3_file_path = s3_file_path + "/" + file_name    
    s3_resource.meta.client.upload_file(lambda_path, output_bucket_name, s3_file_path)
    print("## Errors were found in the Submitted File. Ending Validation ##")

##writes a csv file containing any errors found in submission validation
def write_submission_error_csv(s3_resource,output_bucket_name,s3_file_path,file_name,list_of_elem):
    lambda_path = "/tmp/" + file_name
    
    with open(lambda_path, 'w+', newline='') as csvfile:      #w is new file, a+ is append to file
         csv_writer = csv.writer(csvfile)
         for file_indx in enumerate(list_of_elem):
             csv_writer.writerow(file_indx[1])  
             
    s3_file_path = s3_file_path + "/" + file_name    
    s3_resource.meta.client.upload_file(lambda_path, output_bucket_name, s3_file_path)