import zipfile
import difflib
import boto3
import file_validator_object
import pandas as pd
import re
import csv
from datetime import datetime
import dateutil.tz
import mysql.connector
import pathlib
from collections import Counter
from prior_test_result_validator import prior_test_result_validator
from demographic_data_validator  import demographic_data_validator
from Biospecimen_validator       import Biospecimen_validator
#####################################################################
def lambda_handler(event, context):
    temp_file_loc = '/tmp'
    s3_client = boto3.client('s3',aws_access_key_id = aws_access_id, aws_secret_access_key = aws_secret_key, region_name='us-east-1')
    s3_resource = boto3.resource("s3",aws_access_key_id = aws_access_id, aws_secret_access_key = aws_secret_key, region_name='us-east-1')
    ssm = boto3.client("ssm",aws_access_key_id = aws_access_id, aws_secret_access_key = aws_secret_key, region_name='us-east-1')
    sns = boto3.client('sns',aws_access_key_id = aws_access_id, aws_secret_access_key = aws_secret_key, region_name='us-east-1')
    host_client = ssm.get_parameter(Name="db_host", WithDecryption=True).get("Parameter").get("Value")
    user_name = ssm.get_parameter(Name="lambda_db_username", WithDecryption=True).get("Parameter").get("Value")
    user_password =ssm.get_parameter(Name="lambda_db_password", WithDecryption=True).get("Parameter").get("Value")
    jobs_dbname = ssm.get_parameter(Name="jobs_db_name", WithDecryption=True).get("Parameter").get("Value")
    TopicArn_Success = ssm.get_parameter(Name="TopicArn_Success", WithDecryption=True).get("Parameter").get("Value")
    TopicArn_Failure = ssm.get_parameter(Name="TopicArn_Failure", WithDecryption=True).get("Parameter").get("Value")
    file_dbname = "seronetdb-Validated"
#####################################################################
## if no submission errors, pull key peices from sql schema and import cbc id file
    jobs_conn,j_status_message   = connect_to_sql_database(jobs_dbname,host_client,user_name,user_password)
    filedb_conn,f_status_message = connect_to_sql_database(file_dbname,host_client,user_name,user_password)

    if (j_status_message == "Connection Failed") or (f_status_message == "Connection Failed"):
        print("Unable to Connect to MYSQL Database")
        print("Terminating File Validation Process")
        close_connections(jobs_conn)
        close_connections(filedb_conn)
        return{}
    del j_status_message,f_status_message
    try:
#############################################################################################################
## get list of IDS for SARS_CoV-2 Positve and Negative Participants, also get all Biospecimen IDS
        pos_list,neg_list = file_validator_object.get_mysql_queries(file_dbname,filedb_conn,1,pd)
        biospec_ids = file_validator_object.get_mysql_queries(file_dbname,filedb_conn,2,pd)
        valid_particiant_ids = pos_list + neg_list

        key_index_dict = {"prior_clinical_test.csv":'Research_Participant_ID',
                          "demographic.csv":'Research_Participant_ID',
                          "biospecimen.csv":'Biospecimen_ID',
                          "aliquot.csv":['Aliquot_ID','Biospecimen_ID'],
                          "equipment.csv":['Equipment_ID','Biospecimen_ID'],
                          "reagent.csv":['Biospecimen_ID','Reagent_Name'],
                          "consumable.csv":['Biospecimen_ID','Consumable_Name']}
#############################################################################################################
## Query the jobs table database and get list of zip files that passed File-Validation
        MY_SQL = "SELECT * FROM table_submission_validator where  batch_validation_status = %s"
        successful_submissions = pd.read_sql(MY_SQL, con=jobs_conn, params=['Batch_Validation_SUCCESS'])
        successful_submissions_ids = successful_submissions['submission_file_id']
## For each file that Passes, get a list of the ziped files that also passed file-validation
        for iterS in successful_submissions_ids:
            print(' ')
            MY_SQL = ("SELECT * FROM table_file_validator where submission_file_id = %s and file_validation_status = %s")
            files_to_check = pd.read_sql(MY_SQL, con=jobs_conn, params=[iterS,'FILE_VALIDATION_SUCCESS'])
            file_names = [pathlib.PurePath(i).name for i in files_to_check['file_validation_file_location']]
            if 'submission.csv' not in file_names:          #code causes errors if this file does not exist
                continue                                    #if submission was successful then it has to exist
            submitting_center = get_submission_metadata(s3_client,temp_file_loc,files_to_check,file_names,file_dbname,jobs_conn)
            all_file_objects = []
            for current_file in file_names:
                if current_file in ['submission.csv']:
                    all_file_objects.append((current_file,[]))
                else:
                    current_object = file_validator_object.Submitted_file(current_file,key_index_dict[current_file])
                    bucket_name,org_key_name = get_bucket_and_key(files_to_check,file_names,current_file)
                    current_object.File_Bucket = bucket_name
                    current_object.Error_dest_key = org_key_name.replace('UnZipped_Files/'+current_file,'Data_Validation_Results')
                    current_object.load_csv_file(s3_client,bucket_name,org_key_name,pd)
                    all_file_objects.append((current_file,current_object))
                if current_file in ['demographic.csv']:
                    current_particiant_ids = valid_particiant_ids.append(current_object.Data_Table[['Research_Participant_ID']])
                    current_particiant_ids = current_particiant_ids['Research_Participant_ID'].tolist()
                if current_file in ['biospecimen.csv']:
                    biospec_ids = biospec_ids.append(current_object.Data_Table[['Biospecimen_ID','Biospecimen_Type']])
##########################################################################################################################################
            if "prior_clinical_test.csv" in file_names:
                current_object = all_file_objects[file_names.index("prior_clinical_test.csv")][1]
                pos_list,neg_list = file_validator_object.split_participant_pos_neg_prior(current_object,pos_list,neg_list,pd)
                current_object = prior_test_result_validator(current_object,neg_list,pos_list,re,submitting_center['CBC_ID'],current_particiant_ids)
                error_count = current_object.write_error_file("Prior_Test_Results_Errors_Found.csv",s3_resource,temp_file_loc)
            if "demographic.csv" in file_names:
                current_object = all_file_objects[file_names.index("demographic.csv")][1]
                current_object.get_pos_neg_logic(pos_list,neg_list)
                current_object.remove_unknown_sars_results_v2()
                current_object = demographic_data_validator(current_object,neg_list,pos_list,re,submitting_center['CBC_ID'])
                error_count = current_object.write_error_file("Demographic_Results_Errors.csv",s3_resource,temp_file_loc)
            if "biospecimen.csv" in file_names:
                current_object = all_file_objects[file_names.index("biospecimen.csv")][1]
                current_object.get_pos_neg_logic(pos_list,neg_list)
                current_object.remove_unknown_sars_results_v2()
                current_object = Biospecimen_validator(current_object,neg_list,pos_list,re,submitting_center['CBC_ID'],current_particiant_ids)
                error_count = current_object.write_error_file("Biospecimen_Results_Errors.csv",s3_resource,temp_file_loc)
##########################################################################################################################################
    except Exception as e:                          #if there are any errors, display and move to finally block
        print(e)
        print("Terminating Validation Process")
    finally:                                        #close all the sql connections if they exist
        print("Connection to RDS mysql instance is now closed")
        close_connections(jobs_conn)
        close_connections(filedb_conn)
        return{}
#####################################################################
def connect_to_sql_database(file_dbname,host_client,user_name,user_password):
    status_message = "Connected"
    conn = []
    try:
        conn = mysql.connector.connect(host = host_client, user=user_name, password=user_password, db=file_dbname, connect_timeout=5)
        print("SUCCESS: Connection to RDS mysql instance succeeded\n")
    except Exception as e:
        print(e)
        status_message = "Connection Failed"
    return conn,status_message
def close_connections(conn):
    if type(conn) == mysql.connector.connection.MySQLConnection:
        conn.close()
def get_bucket_and_key(files_to_check,file_names,current_file):
    current_metadata = files_to_check.loc[file_names.index(current_file)]
    full_bucket_name = current_metadata[current_metadata.index == 'file_validation_file_location'][0]

    first_folder_cut = full_bucket_name.find('/')
    org_key_name = full_bucket_name[(first_folder_cut+1):]
    bucket_name = full_bucket_name[:(first_folder_cut)]
    return bucket_name,org_key_name
def get_submission_metadata(s3_client,temp_file_loc,files_to_check,file_names,file_dbname,jobs_conn):
    bucket_name,org_key_name = get_bucket_and_key(files_to_check,file_names,"submission.csv")

    temp_file_loc = temp_file_loc + '/test_file'
    s3_client.download_file(bucket_name,org_key_name,temp_file_loc)
    Data_Table = pd.read_csv(temp_file_loc,encoding='utf-8',na_filter = False)

    MY_SQL = ("SELECT * FROM `" + file_dbname + "`.CBC Where CBC_Name = %s")
    submitting_center = pd.read_sql(MY_SQL, con = jobs_conn, params=[Data_Table.columns.values[1]])

    return submitting_center