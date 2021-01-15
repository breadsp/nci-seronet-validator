import zipfile                 
#from collections import Counter
import difflib
import os
import pymysql
import file_validator_object
import pandas as pd
import time
import re
import csv
from datetime import datetime
from prior_test_result_validator import prior_test_result_validator
#####################################################################
def connect_to_sql_database():
    host_client = 'database-seronet.cled4oyy9gxy.us-east-1.rds.amazonaws.com'
    port = 3306
    file_dbname = "seronetdb-prevalidated"
    user_name = "patrick.beardsley"
    user_password = '963d3b8de9af9735df91d0db463d175a'
    conn = 0
    try:
        conn = pymysql.connect(host = host_client, user=user_name, password=user_password, db=file_dbname, connect_timeout=5)
        print("SUCCESS: Connection to RDS mysql instance succeeded\n")
    except:
        print("ERROR: Unexpected error: Could not connect to MySql instance.\n")
    return conn,file_dbname
#####################################################################
#sample_file = r'C:\Users\pbrea\Documents\Python Files\Simulated_Data_CSV\Simulated_Data_CSV.zip'
#sample_file = r'C:\Users\pbrea\Documents\Seronet-Work\Validation_Testing_Files\Simulated_Data_CSV\Simulated_Data_CSV.zip'
#sample_file = r'C:\Users\pbrea\Documents\Seronet-Work\Validation_Testing_Files\New_Testing_Data\New_Testing_Data.zip'
sample_file = r'C:\Users\pbrea\Documents\Seronet-Work\Validation_Testing_Files\New_Testing_Data_Dirty\New_Testing_Data_Dirty.zip'


def display_run_time(total_time,t,display_msg):
    elapsed = time.time() - t
    print(display_msg  %elapsed)
    return (total_time + elapsed)

def add_error_row_to_file(file_name,list_of_elem):
     file_name = r"C:\Users\pbrea\Documents\Seronet-Work\Validation_Error_Output\%s" %file_name
     with open(file_name, 'w', newline='') as csvfile:      #w is new file, a+ is append to file
         csv_writer = csv.writer(csvfile)
         for file_indx in enumerate(list_of_elem):
             csv_writer.writerow(file_indx[1])                   

def main_function(sample_file):
    total_time = 0;
    submitting_center = ''
    t = time.time()
    header_error_list = [['CSV_Sheet_Name','Column_Name','Error_message']];
#####################################################################
## Checks to see if the file has a .zip extension, erorrs if not in proper format                        
    try:
        sample_file.find('.zip')
    except:
        print('File submitted is not a zip file\n')
        total_time = display_run_time(total_time,t,"Ending Validation after %f seconds\n")
        #need to add error message for this failure step
        conn.close()
        return{}
########################################################################
## Tries to upzip the file to get contents of the zip, errors if not able to unzip
    submission_error_list = [['File_Name','Column_Name','Error_Message']]
    try:
        with zipfile.ZipFile(sample_file) as zipObj:
            zipObj.extractall(r'C:\Users\pbrea\Documents\Seronet-Work\Unziped_File_Location')
            listOfFileNames = zipObj.namelist()   
    except:
        print('Submitted file is a *.zip but not able to open file\n')
        total_time = display_run_time(total_time,t,"Ending Validation after %f seconds\n")
        #need to add error message for this failure step
        return{}
    total_time = display_run_time(total_time,t,"Submitted file is a valid zip, process took %f seconds\n")
########################################################################
## sorts the submission list adjusting for upper and lower case formating
    list_copy = [item.lower() for item in listOfFileNames]
    sort_idx = [i[0] for i in sorted(enumerate(list_copy), key=lambda x:x[1])]
    listOfFileNames = [listOfFileNames[i] for i in sort_idx]
 #   listOfFileNames = [x.lower() for x in listOfFileNames]

    list_of_valid_file_names = (['Aliquot_Metadata.csv','Assay_Metadata.csv','Assay_Target.csv','Biospecimen_Metadata.csv',
                                 'Confirmatory_Test_Results.csv','Demographic_Data.csv','Equipment_Metadata.csv','Prior_Test_Results.csv',
                                 'Reagent_Metadata.csv','shipping-manifest.csv','Submission_Metadata.csv'])
########################################################################
## compares files in the zip to the list of all valid names to check for extra files, spelling errors or duplicate entries
## also checks to ensure that submitted files are in csv format
    for uni_id in listOfFileNames:
        if (uni_id.find('.csv') > 0) == False:
            submission_error_list .append([uni_id,"All Columns","File Is not a CSV file, Unable to Process"])
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
#######################################################################
## check names and spelling of file headers against expected table name headers in mysql database
    conn,file_dbname = connect_to_sql_database()
    if conn == 0:
        print("Terminating Validation Process")
        conn.close()
        add_error_row_to_file("Submission_Error_List.csv",submission_error_list)
        return{}   
    
    if len(submission_error_list) > 1:
        error_files = [i[1][0] for i in enumerate(submission_error_list)]
        error_files = error_files[1:]
        listOfFileNames = [i for i in listOfFileNames if i not in error_files]
        

    current_error_count = 0; part_count = [];  bio_count = []; 
    for test_name in enumerate(listOfFileNames):
        test_object = file_validator_object.Submitted_file(test_name[1],' ')                                       #create the file object
        test_object.get_csv_table('C:\\Users\\pbrea\\Documents\\Seronet-Work\\Unziped_File_Location\\' + test_name[1])                              #populate object with data
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
            test_object.compare_csv_to_mysql(file_dbname,mysql_table_list,conn)  
        
        if (test_name[1]  == "Demographic_Data.csv") and ("Research_Participant_ID" in test_object.Column_Header_List):
            part_count = part_count + (test_object.Data_Table["Research_Participant_ID"]).tolist()
        if (test_name[1]  == "Biospecimen_Metadata.csv") and ("Biospecimen_ID" in test_object.Column_Header_List):
            bio_count = bio_count + (test_object.Data_Table["Biospecimen_ID"]).tolist()
            
        if len(test_object.header_name_validation) > 1: 
            submission_error_list.append(test_object.header_name_validation[1:][0])
            current_error_count = current_error_count + 1;
    
    if current_error_count > 0:
        print("Column names in each submitted file have been checked for spelling errors,")
        print("extra columns, missing or duplicate entries: " + str(current_error_count) + " errors were found. \n")
        error_count = error_count + current_error_count
        
    uni_part_ids = len(set(part_count))
    uni_bio_ids = len(set(bio_count))
    
    current_error_count = 0
    if len(submitting_center) == 0:
        error_msg = "Submission_Metadata.csv was not found in submission zip file"
        submission_error_list.append(["submission_metadata.csv","All Columns",error_msg])
        current_error_count =  current_error_count + 1;
    if uni_part_ids != Number_of_Research_Participants:
        error_msg = ("Expected: " + str(Number_of_Research_Participants) + " Participants. Found " + str(uni_part_ids) + " unique Participant Ids in submission")
        submission_error_list.append(["submission_metadata.csv","Number_of_Research_Participants",error_msg])
        current_error_count =  current_error_count + 1;
    if uni_bio_ids != Number_of_Biospecimens:
        error_msg = ("Expected: " + str(Number_of_Biospecimens) + " Samples. Found " + str(uni_bio_ids) + " unique Biospecimen Ids in submission")
        submission_error_list.append(["submission_metadata.csv","Number_of_Biospecimens",error_msg])
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

    total_time = display_run_time(total_time,t,"All file names and column names have been validated, process took %f seconds\n")
    if error_count > 0:
        print("A Total of " + str(error_count) + " errors were found in the submission file, please correct and Resubmit")
        print("Terminating Validation Process")
        conn.close()
        add_error_row_to_file("Submission_Error_List.csv",submission_error_list)

        return{}   
    
    del bio_count,count_table,current_error_count,file_to_submit,i,indices,list_copy,matching_letters,Number_of_Biospecimens
    del Number_of_Research_Participants,part_count,sequence,sort_idx,submit_file_count,submit_list,submit_to_file,t,test_name
    del test_object,uni_id,valid,wrong_count,wrong_letters,zipObj,uni_bio_ids,uni_part_ids
#####################################################################################################
## if no submission errors, pull key peices from sql schema and import cbc id file 
    pos_list,neg_list,ukn_list = file_validator_object.get_mysql_queries(file_dbname,conn,1)
    assay_results,assay_target = file_validator_object.get_mysql_queries(file_dbname,conn,2)
    participant_ids,biospec_ids = file_validator_object.get_mysql_queries(file_dbname,conn,3)
    
    if 'Demographic_Data.csv' in listOfFileNames:
        current_demo =  pd.read_csv('C:\\Users\\pbrea\\Documents\\Seronet-Work\\Unziped_File_Location\\Demographic_Data.csv')    
        current_demo = current_demo['Research_Participant_ID'].tolist()
        current_demo = (set(participant_ids['Research_Participant_ID'].tolist() + current_demo))
        current_demo = [x for x in current_demo if x == x]
    else:
        current_demo = []
    
    sql_connect = conn.cursor()
    table_sql_str = ("SELECT * FROM `" + file_dbname + "`.`Seronet_CBC_ID`")
    query_res = sql_connect.execute(table_sql_str)
    rows = sql_connect.fetchall()
    valid_cbc_ids = [i[1] for i in enumerate(rows) if rows[i[0]][1] == submitting_center]
    
    if len(valid_cbc_ids) == 0:
        print("Submitting Center is not a valid entry, unable to validate")
        return{}
########################################################################################################################################### 
    if "Prior_Test_Results.csv" in listOfFileNames:
        prior_valid_object = file_validator_object.Submitted_file("Prior_Test_Results.csv",'Research_Participant_ID')  #create the file object
        prior_valid_object.get_csv_table('C:\\Users\\pbrea\\Documents\\Seronet-Work\\Unziped_File_Location\\Prior_Test_Results.csv')                    #populate object with data
        pos_list,neg_list,unk_list,prior_valid_object = file_validator_object.split_participant_pos_neg_prior(prior_valid_object,pos_list,neg_list)  
        prior_valid_object = prior_test_result_validator(prior_valid_object,neg_list,pos_list,re,valid_cbc_ids,current_demo)
###########################################################################################################################################  
    if "Demographic_Data.csv" in listOfFileNames:
        demo_data_object = file_validator_object.Submitted_file("Demographic_Data.csv",'Research_Participant_ID')  #create the file object
        demo_data_object.get_csv_table('C:\\Users\\pbrea\\Documents\\Seronet-Work\\Unziped_File_Location\\Demographic_Data.csv')                    #populate object with data
        demo_data_object.compare_csv_to_mysql('seronetdb-prevalidated',["Demographic_Data","Prior_Covid_Outcome","Comorbidity"],conn)                       #compare header list to mysql
        demo_data_object.get_pos_neg_logic(pos_list,neg_list)
#        demo_data_object.remove_unknown_sars_results_v2()
        
        for header_name in demo_data_object.Column_Header_List:
            test_column = demo_data_object.Data_Table[header_name];
            missing_logic,has_logic,missing_data_column,has_data_column = demo_data_object.check_data_type(test_column,header_name)
          
            if header_name.find('Research_Participant_ID') > -1:        #checks if Participant ID in valid format
                error_msg = "Value it not a Valid id format, Expecting XX_XXXXXX"
                pattern = re.compile('^[0-9]{2}[_]{1}[0-9]{6}$')    
                [demo_data_object.valid_ID(header_name,i[1],pattern,valid_cbc_ids,error_msg,has_data_column.index[i[0]],'Error') for i in enumerate(has_data_column)]    
                [demo_data_object.is_required(header_name,i[1],"All",missing_data_column.index[i[0]],'Error') for i in enumerate(missing_data_column)] 
            
                matching_values = [i for i in enumerate(test_column) if pattern.match(i[1]) is not None]
                for i in matching_values:
                    if i[1] not in pos_list['Research_Participant_ID'].tolist()+neg_list['Research_Participant_ID'].tolist():
                        error_msg = "ID is valid, however is not found in Prior_Test_Results, No Matching Prior_SARS_CoV-2 Result"
                        demo_data_object.write_error_msg(i[1],header_name,error_msg,i[0],'Error')    
            elif (header_name.find('Age') > -1):
                error_msg = "Value must be a number greater than 0"
                [demo_data_object.is_numeric(header_name,False,i[1],0,error_msg,has_data_column.index[i[0]],'Error') for i in enumerate(has_data_column)]
                [demo_data_object.is_required(header_name,i[1],"All",missing_data_column.index[i[0]],'Error') for i in enumerate(missing_data_column)] 
            elif (header_name in ['Race','Ethnicity','Gender']):
                if (header_name.find('Race') > -1):
                    test_string =  ['White', 'American Indian or Alaska Native', 'Black or African American', 'Asian', 
                                    'Native Hawaiian or Other Pacific Islander', 'Other', 'Multirace','Not Reported', 'Unknown']
                elif (header_name.find('Ethnicity') > -1):
                    test_string = ['Hispanic or Latino','Not Hispanic or Latino']
                elif (header_name.find('Gender') > -1):
                    test_string = ['Male', 'Female', 'Other','Not Reported', 'Unknown']
                error_msg = "Value must be one of the following: " + str(test_string)
                [demo_data_object.in_list(header_name,i[1],test_string,error_msg,has_data_column.index[i[0]],'Error') for i in enumerate(has_data_column)]
                [demo_data_object.is_required(header_name,i[1],"All",missing_data_column.index[i[0]],'Error') for i in enumerate(missing_data_column)]
            elif (header_name.find('Is_Symptomatic') > -1):
                test_string = ['Yes','No']
                error_msg = "Participant is SARS_CoV2 Positive. must be: " + str(test_string)
                pos_test_value = demo_data_object.Data_Table[demo_data_object.pos_list_logic & has_logic][header_name]
                [demo_data_object.in_list(header_name,i[1],test_string,error_msg,pos_test_value.index[i[0]],'Error') for i in enumerate(pos_test_value)]

                error_msg = "Participant is SARS_CoV2 Negative. must be of the following: [No,Unknown,N/A]"
                neg_test_value = demo_data_object.Data_Table[demo_data_object.neg_list_logic & has_logic][header_name]
                [demo_data_object.in_list(header_name,i[1],["No"],error_msg,neg_test_value.index[i[0]],'Error') for i in enumerate(neg_test_value)]
                
                demo_data_object.check_required(missing_logic,header_name,'Warning','Error')                
            elif (header_name.find('Date_of_Symptom_Onset') > -1): 
                error_msg = "Participant has symptomns (Is_Symptomatic == 'Yes'), value must be a valid Date MM/DD/YYYY"
                test_value = demo_data_object.Data_Table[demo_data_object.Data_Table['Is_Symptomatic'] == "Yes"][header_name]
                try:
                    [demo_data_object.is_date_time(header_name,i[1],False,error_msg,test_value.index[i[0]],'Error') for i in enumerate(test_value)] 
                except:
                    print('warning')
                
                error_msg = "Participant does not have symptomns (Is_Symptomatic == 'No'), value must be N/A"
                
                test_value = demo_data_object.Data_Table.iloc[[i[0] for i in enumerate(demo_data_object.Data_Table['Is_Symptomatic']) if i[1] in ['No','Unknown','N/A']]][header_name]
                [demo_data_object.in_list(header_name,i[1],['N/A'],error_msg,test_value.index[i[0]],'Error') for i in enumerate(test_value)]
                
                demo_data_object.check_required(missing_logic,header_name,'Warning','Error')                
            elif (header_name.lower().find('symptoms_resolved') > -1):                   
                test_string = ["Yes","No"]
                error_msg = "Participant previous had symptoms or currently has symptoms (Is_Symptomatic == 'Yes'), value must be: " + str(test_string)
                test_logic = (demo_data_object.Data_Table['Is_Symptomatic'] == "Yes") & (demo_data_object.pos_list_logic)
                test_value = demo_data_object.Data_Table[test_logic][header_name]
                [demo_data_object.in_list(header_name,i[1],test_string,error_msg,test_value.index[i[0]],'Error') for i in enumerate(test_value)] 
                
                error_msg = "Participant does not have symptomns (Is_Symptomatic == 'No'), value must be N/A"                    
                test_value = demo_data_object.Data_Table[(demo_data_object.Data_Table['Is_Symptomatic'] != "Yes")][header_name]
                [demo_data_object.in_list(header_name,i[1],['N/A'],error_msg,test_value.index[i[0]],'Error') for i in enumerate(test_value)] 
                
                demo_data_object.check_required(missing_logic,header_name,'Warning','Error')                
            elif (header_name.find('Date_of_Symptom_Resolution') > -1):
                error_msg = "Participant symptoms have resolved (Symptoms_Resolved == 'Yes'), value must be valid Date MM/DD/YYYY"
                test_value = demo_data_object.Data_Table[(demo_data_object.Data_Table['Symptoms_Resolved'] == "Yes")][header_name]
                [demo_data_object.is_date_time(header_name,i[1],False,error_msg,i[0],'Error') for i in enumerate(test_value)]   
                
                error_msg = "Participant never had symptoms (Is_Symptomatic == 'No'), value must be N/A"
                test_value = demo_data_object.Data_Table[(demo_data_object.Data_Table['Is_Symptomatic'] != "Yes")][header_name]
                [demo_data_object.in_list(header_name,i[1],['N/A'],error_msg,test_value.index[i[0]],'Error') for i in enumerate(test_value)] 
                
                demo_data_object.check_required(missing_logic,header_name,'Warning','Error')
                
            elif (header_name.lower().find('covid_disease_severity') > -1):
                test_string = [1,2,3,4,5,6,7,8,'1','2','3','4','5','6','7','8']
                error_msg = "Participant is SARS_CoV2 Positive. value must be a number [1,2,3,4,5,6,7,8]"
                pos_test_value = demo_data_object.Data_Table[demo_data_object.pos_list_logic & has_logic][header_name]
                [demo_data_object.in_list(header_name,i[1],test_string,error_msg,pos_test_value.index[i[0]],'Error') for i in enumerate(pos_test_value)]
                
                error_msg = "Participant is SARS_CoV2 Negative. value must be 0"
                neg_test_value = demo_data_object.Data_Table[demo_data_object.neg_list_logic & has_logic][header_name]
                [demo_data_object.in_list(header_name,i[1],[0,'0'],error_msg,neg_test_value.index[i[0]],'Error') for i in enumerate(neg_test_value)]
                
                demo_data_object.check_required(missing_logic,header_name,'Warning','Error')
         
            elif (header_name in ["Diabetes_Mellitus","Hypertension","Severe_Obesity","Cardiovascular_Disease","Chronic_Renal_Disease",
                                                 "Chronic_Liver_Disease","Chronic_Lung_Disease","Immunosuppressive_conditions","Autoimmune_condition","Inflammatory_Disease"]):
    
                test_string = ["Yes","No"]
                error_msg = "Participant is SARS_CoV2 Positive. value must be: " + str(test_string)
                pos_test_value = demo_data_object.Data_Table[demo_data_object.pos_list_logic & has_logic][header_name]
                [demo_data_object.in_list(header_name,i[1],test_string,error_msg,pos_test_value.index[i[0]],'Error') for i in enumerate(pos_test_value)]
                
                test_string = ["Yes", "No", "Unknown", "N/A"]
                error_msg = "Participant is SARS_CoV2 Negative. value must be: " + str(test_string)
                neg_test_value = demo_data_object.Data_Table[demo_data_object.neg_list_logic & has_logic][header_name]
                [demo_data_object.in_list(header_name,i[1],test_string,error_msg,neg_test_value.index[i[0]],'Error') for i in enumerate(neg_test_value)]
                
                demo_data_object.check_required(missing_logic,header_name,'Warning','Error')
            elif (header_name in ["Other_Comorbidity"]):
                error_msg = "Invalid or unknown ICD10 code, Value must be Valid ICD10 code or N/A"
                test_value = demo_data_object.Data_Table[demo_data_object.neg_list_logic][header_name]
                [demo_data_object.check_icd10(header_name,i[1],error_msg,has_data_column.index[i[0]],'Error') for i in enumerate(has_data_column)]
                demo_data_object.check_required(missing_logic,header_name,'Warning','Warning')
         

        demo_data_object.write_error_file("Demographic_Data_errors_Found.csv")
        del neg_test_value,pos_test_value,test_column,test_string,test_value,error_count,error_msg
        
  ###############################################################################################################################
    if "Biospecimen_Metadata.csv" in listOfFileNames:
        Biospecimen_object = file_validator_object.Submitted_file("Biospecimen_Metadata.csv",'Biospecimen_ID')                   #create the file object
        Biospecimen_object.get_csv_table('C:\\Users\\pbrea\\Documents\\Seronet-Work\\Unziped_File_Location\\Biospecimen_Metadata.csv')                    #populate object with data
        Biospecimen_object.get_pos_neg_logic(pos_list,neg_list)
        
        biospec_ids = pd.concat([biospec_ids,Biospecimen_object.Data_Table[biospec_ids.columns]])
        biospec_ids.drop_duplicates(inplace=True)
            
        for header_name in Biospecimen_object.Column_Header_List:
            test_column = Biospecimen_object.Data_Table[header_name]; 
            missing_logic,has_logic,missing_data_column,has_data_column = Biospecimen_object.check_data_type(test_column,header_name)
                                                             
            if (header_name in ["Research_Participant_ID"]):
                error_msg = "Value it not a Valid id format, Expecting XX_XXXXXX"
                pattern = re.compile('^[0-9]{2}[_]{1}[0-9]{6}$')    
                [Biospecimen_object.valid_ID(header_name,i[1],pattern,valid_cbc_ids,error_msg,has_data_column.index[i[0]],'Error') for i in enumerate(has_data_column)]    
                [Biospecimen_object.is_required(header_name,i[1],"All",missing_data_column.index[i[0]],'Error') for i in enumerate(missing_data_column)] 
              
                matching_values = [i for i in enumerate(has_data_column) if pattern.match(i[1]) is not None]
                if (len(matching_values) > 0) and (len(current_demo) > 0):
                    error_msg = "Id is not found in database or in submitted demographic file"
                    [Biospecimen_object.in_list(header_name,i[1][1],current_demo,error_msg,has_data_column.index[i[1][0]],'Error') for i in enumerate(matching_values)]
           
            elif (header_name in ["Biospecimen_ID"]):
                error_msg = "Value it not a Valid id format, Expecting XX_XXXXXX_XXX"
                pattern = re.compile('^[0-9]{2}[_]{1}[0-9]{6}[_]{1}[0-9]{3}$')    
                [Biospecimen_object.valid_ID(header_name,i[1],pattern,valid_cbc_ids,error_msg,has_data_column.index[i[0]],'Error') for i in enumerate(has_data_column)]    
                [Biospecimen_object.is_required(header_name,i[1],"All",missing_data_column.index[i[0]],'Error') for i in enumerate(missing_data_column)]     
            elif(header_name in ["Biospecimen_Group"]):
                error_msg = "Participant is SARS_CoV2 Positive. Value must be: Positive Sample"
                test_value = Biospecimen_object.Data_Table[Biospecimen_object.pos_list_logic][header_name]
                [Biospecimen_object.in_list(header_name,i[1],["Positive Sample"],error_msg,test_value.index[i[0]],'Error') for i in enumerate(test_value)]
                
                error_msg = "Participant is SARS_CoV2 Negative. Value must be: Negative Sample"
                test_value = Biospecimen_object.Data_Table[Biospecimen_object.neg_list_logic][header_name]
                [Biospecimen_object.in_list(header_name,i[1],["Negative Sample"],error_msg,test_value.index[i[0]],'Error') for i in enumerate(test_value)]
                
                unknown_prior_sars_test = Biospecimen_object.Data_Table[~(Biospecimen_object.pos_list_logic | Biospecimen_object.neg_list_logic)]
                matching_values = [i for i in enumerate(unknown_prior_sars_test['Research_Participant_ID']) if pattern.match(i[1]) is not None]
                error_msg = "Research Particpant ID is valid, Prior_SARS_CoV-2 Result is Unknown/Missing. Unable to validate Biospecimen Group"
                for i in matching_values:
                    Biospecimen_object.write_error_msg(i[1],header_name,error_msg,unknown_prior_sars_test.index[i[0]],'Error')       
              
                non_matching_values = [i for i in enumerate(unknown_prior_sars_test['Research_Participant_ID']) if pattern.match(i[1]) is None]
                error_msg = "Research Particpant ID has invalid format, No Prior_SARS_CoV-2 Result. Unable to validate Biospecimen Group"
                for i in enumerate(non_matching_values):
                    Biospecimen_object.write_error_msg(i[1],header_name,error_msg,unknown_prior_sars_test.index[i[0]],'Error')                  
            elif(header_name in ["Biospecimen_Type"]):
                test_string = ["Serum", "EDTA Plasma", "PBMC", "Saliva", "Nasal swab"]
                error_msg = "Value must be: " + str(test_string)
                [Biospecimen_object.in_list(header_name,i[1],test_string,error_msg,has_data_column.index[i[0]],'Error') for i in enumerate(has_data_column)]
                [Biospecimen_object.is_required(header_name,i[1],"All",missing_data_column.index[i[0]],'Error') for i in enumerate(missing_data_column)]   
            elif ((header_name.lower().find('date_of') > -1) or (header_name.lower().find('expiration_date') > -1)):
                error_msg = "Value must be a valid Date MM/DD/YYYY"
                [Biospecimen_object.is_date_time(header_name,i[1],False,error_msg,has_data_column.index[i[0]],'Error') for i in enumerate(has_data_column)] 
                [Biospecimen_object.is_required(header_name,i[1],"All",missing_data_column.index[i[0]],'Error') for i in enumerate(missing_data_column)]   
            elif (header_name.lower().find('time_of') > -1):
                error_msg = "Value must be a valid time in 24hour format HH:MM"
                [Biospecimen_object.is_date_time(header_name,i[1],False,error_msg,has_data_column.index[i[0]],'Error') for i in enumerate(has_data_column)] 
                [Biospecimen_object.is_required(header_name,i[1],"All",missing_data_column.index[i[0]],'Error') for i in enumerate(missing_data_column)]   
            elif ((header_name.lower().find('company_clinic') > -1) or (header_name.lower().find('initials') > -1) or 
                (header_name.lower().find('collection_tube_type') > -1)):
                error_msg = "Value must be a string and NOT N/A"
                [Biospecimen_object.is_string(header_name,i[1],False,error_msg,has_data_column.index[i[0]],'Errr') for i in enumerate(has_data_column)] 
                [Biospecimen_object.is_required(header_name,i[1],"All",missing_data_column.index[i[0]],'Error') for i in enumerate(missing_data_column)]       
            elif (header_name.find('Hemocytometer_Count') > -1) or (header_name.find('Automated_Count') > -1):
#                error_msg = "Column is only valid for Biospecimen_Type == PBMC, Value should be N/A"
#                test_value = Biospecimen_object.Data_Table[(Biospecimen_object.Data_Table['Biospecimen_Type'] != "PBMC")][header_name]
#                [Biospecimen_object.in_list(header_name,i[1],["N/A"],error_msg,test_value.index[i[0]],'Error') for i in enumerate(test_value)]
                          
                test_value = Biospecimen_object.Data_Table[(Biospecimen_object.Data_Table['Biospecimen_Type'] == "PBMC")][header_name]
                [Biospecimen_object.is_numeric(header_name,False,i[1],0,error_msg,test_value.index[i[0]],'Error') for i in enumerate(test_value)]
                [Biospecimen_object.is_required(header_name,i[1],"All",missing_data_column.index[i[0]],'Error') for i in enumerate(missing_data_column)]
                if (header_name.find('Total') > -1):
                    current_live_index = header_name.replace('Total_Cells','Live_Cells')
                    live_testing = Biospecimen_object.Data_Table[(Biospecimen_object.Data_Table['Biospecimen_Type'] == "PBMC")][current_live_index]
                    
                    live_logic  = [isinstance(i, float) or isinstance(i, int) for i in live_testing]
                    total_logic = [isinstance(i, float) or isinstance(i, int) for i in test_value]
                    
                    for i in enumerate(total_logic):
                        if (i[1] == True) and (live_logic[i[0]] == True):     #both columns have data
                            if (live_testing.iloc[i[0]] > test_value.iloc[i[0]]):       #live counts higher then total
                                error_msg = "Total Count(" + str(test_value.iloc[i[0]]) + ") is less then live count ( " + str(live_testing.iloc[i[0]]) + ")" 
                                Biospecimen_object.write_error_msg(test_value.iloc[i[0]],header_name,error_msg,test_value.index[i[0]],'Error')    
                        if (i[1] == True) and (live_logic[i[0]] == False):       #has data but live is error
                            error_msg = "Live count ( " + live_testing.iloc[i[0]] + ") is not a number, unable to Validate" 
                            Biospecimen_object.write_error_msg(test_value.iloc[i[0]],header_name,error_msg,test_value.index[i[0]],'Error')    
                elif (header_name.find('Viability') > -1):
                    current_live_index = header_name.replace('Viability','Live_Cells')
                    current_total_index = header_name.replace('Viability','Total_Cells')
                    live_testing = Biospecimen_object.Data_Table[(Biospecimen_object.Data_Table['Biospecimen_Type'] == "PBMC")][current_live_index]
                    total_testing = Biospecimen_object.Data_Table[(Biospecimen_object.Data_Table['Biospecimen_Type'] == "PBMC")][current_total_index]

                    live_logic  = [isinstance(i, float) or isinstance(i, int) for i in live_testing]
                    total_logic = [isinstance(i, float) or isinstance(i, int) for i in total_testing]
                    viable_logic = [isinstance(i, float) or isinstance(i, int) for i in test_value]
                    
                    for i in enumerate(viable_logic):
                        if (i[1] == True) and (live_logic[i[0]] == True) and (total_logic[i[0]] == True):   
                            compare_value = round((live_testing.iloc[i[0]] / total_testing.iloc[i[0]])*100,1)
                            if round(test_value.iloc[i[0]],1) != compare_value:
                                error_msg = "(Live_counts/Total_Counts)*100 is %.1f, make sure numbers are correct" %compare_value
                                Biospecimen_object.write_error_msg(test_value.iloc[i[0]],header_name,error_msg,test_value.index[i[0]],'Error')    
                        if (i[1] == True) and (live_logic[i[0]] == False) and (total_logic[i[0]] == True):       
                            error_msg = "Live count ( " + live_testing.iloc[i[0]] + ") is not a number, unable to Validate" 
                            Biospecimen_object.write_error_msg(test_value.iloc[i[0]],header_name,error_msg,test_value.index[i[0]],'Error')    
                        if (i[1] == True) and (live_logic[i[0]] == True) and (total_logic[i[0]] == False):       
                            error_msg = "Total count ( " + total_testing.iloc[i[0]] + ") is not a number, unable to Validate" 
                            Biospecimen_object.write_error_msg(test_value.iloc[i[0]],header_name,error_msg,test_value.index[i[0]],'Error')    
                        if (i[1] == True) and (live_logic[i[0]] == False) and (total_logic[i[0]] == False):       
                            error_msg = "Total count ( " + total_testing.iloc[i[0]] + ")  and Live count ( " + live_testing.iloc[i[0]] + ") are not numbers, unable to Validate" 
                            Biospecimen_object.write_error_msg(test_value.iloc[i[0]],header_name,error_msg,test_value.index[i[0]],'Error')
            elif(header_name in ["Storage_Time_at_2_8"]):
                error_msg = "Value must be a positive number or N/A"
                [Biospecimen_object.is_numeric(header_name,True,i[1],0,error_msg,has_data_column.index[i[0]],'Error') for i in enumerate(has_data_column)]
                [Biospecimen_object.is_required(header_name,i[1],"All",missing_data_column.index[i[0]],'Error') for i in enumerate(missing_data_column)] 
            elif(header_name in ["Storage_Start_Time_at_2_8","Storage_End_Time_at_2_8","Storage_Start_Time_at_2_8_Initials","Storage_End_Time_at_2_8_Initials"]):
                storage_2_8 = Biospecimen_object.Data_Table["Storage_Time_at_2_8"]
                for i in enumerate(storage_2_8):
                    number_value_2_8 = 'missing';
                    try: 
                        number_value_2_8 = float(i[1])
                    except: 
                        if(i[1] != "N/A"):                    
                            error_msg = "Storage_time_at_2_8 is unknown (value == " + i[1] + "), Unable to Validate Column"
                            Biospecimen_object.write_error_msg(test_column.iloc[i[0]],header_name,error_msg,test_column.index[i[0]],'Error')
                        else:       #value is N/A
                            error_msg = "Storage Time at 2_8 is " + str(i[1])
                            Biospecimen_object.in_list(header_name,test_column.iloc[i[0]],["N/A"],error_msg,i[0],'Error')
                    if number_value_2_8 != 'missing':
                        if(header_name.find('Initials') > -1):
                            error_msg = "Storage Time at 2_8 is " + str(i[1]) + ".  Value must be a string NOT N/A"
                            Biospecimen_object.is_string(header_name,test_column.iloc[i[0]],False,error_msg,test_column.index[i[0]],'Error')    
                        else:
                            error_msg = "Storage Time at 2_8 is " + str(i[1]) + ".  Value must be a datetime MM/DD/YYYY HH:MM"
                            Biospecimen_object.is_date_time(header_name,test_column.iloc[i[0]],False,error_msg,test_column.index[i[0]],'Error')    
            elif(header_name in ["Final_Concentration_of_Biospecimen"]):
                error_msg = "Biospecimen Type == PBMC, Value must be a positive number"                
                test_value = Biospecimen_object.Data_Table[Biospecimen_object.Data_Table['Biospecimen_Type'] == "PBMC"][header_name]
                [Biospecimen_object.is_numeric(header_name,True,i[1],0,error_msg,test_value.index[i[0]],'Error') for i in enumerate(test_value)]
            elif(header_name in ["Centrifugation_Time","RT_Serum_Clotting_Time"]):
                error_msg = "Biospecimen Type == Serum, Value must be a positive number"                
                test_value = Biospecimen_object.Data_Table[Biospecimen_object.Data_Table['Biospecimen_Type'] == "Serum"][header_name]
                [Biospecimen_object.is_numeric(header_name,True,i[1],0,error_msg,test_value.index[i[0]],'Error') for i in enumerate(test_value)]
            elif(header_name in ["Storage_Start_Time_80_LN2_storage"]):
                error_msg = "Biospecimen Type == Serum, Value must be a Time in 24hour format HH:MM"                
                test_value = Biospecimen_object.Data_Table[Biospecimen_object.Data_Table['Biospecimen_Type'] == "Serum"][header_name]    
                [Biospecimen_object.is_date_time(header_name,i[1],False,error_msg,test_value.index[i[0]],'Error') for i in enumerate(test_value)]   
            elif(header_name in ["Initial_Volume_of_Biospecimen"]):
                 error_msg = "Value must be a number greater than 0"
                 [Biospecimen_object.is_numeric(header_name,False,i[1],0,error_msg,has_data_column.index[i[0]],'Error') for i in enumerate(has_data_column)]
                 [Biospecimen_object.is_required(header_name,i[1],"All",missing_data_column.index[i[0]],'Error') for i in enumerate(missing_data_column)]              
###############################################################################################################################    
        test_column = Biospecimen_object.Data_Table["Research_Participant_ID"]; 
        pattern = re.compile('^[0-9]{2}[_]{1}[0-9]{6}$') 
        matching_RPI_values = [i for i in enumerate(test_column) if pattern.match(i[1]) is not None]
        RPI_index,RPI_Value = map(list,zip(*matching_RPI_values))        
        
        test_column = Biospecimen_object.Data_Table["Biospecimen_ID"]; 
        pattern = re.compile('^[0-9]{2}[_]{1}[0-9]{6}[_]{1}[0-9]{3}$')      
        matching_BIO_values = [i for i in enumerate(test_column) if pattern.match(i[1]) is not None]
        BIO_index,BIO_Value = map(list,zip(*matching_BIO_values))
    

        for i in enumerate(matching_RPI_values): 
            if i[1][0] in BIO_index:
                if BIO_Value[BIO_index.index(i[1][0])].find(i[1][1]) == -1:
                    error_msg = "Research_Participant_ID does not agree with Biospecimen ID(" + BIO_Value[BIO_index.index(i[1][0])] + "), first 9 characters should match"
                    print("Research_Participant_ID:: " + i[1][1] + " not a part of  " + BIO_Value[BIO_index.index(i[1][0])])
                    Biospecimen_object.write_error_msg(i[1][1],"Research_Participant_ID",error_msg,i[1][0],'Error') 

            
            
            
            

                
        for x in range(len(matching_RPI_values)-1):
            if RPI_index[x] in BIO_index:
                if BIO_Value[x].find(RPI_Value[x]) == -1:
                    error_msg = "Research_Participant_ID does not agree with Biospecimen ID(" + BIO_Value[x] + "), first 9 characters should match"
                    print(error_msg)
                    Biospecimen_object.write_error_msg(RPI_Value[x],"Research_Participant_ID",error_msg,x,'Error') 
            else:
                print("indexes dont match")
        for x in range(len(matching_BIO_values)-1):
            if x in RPI_index:
                if BIO_Value[x].find(RPI_Value[x]) == -1:
                    error_msg = "Biospecimen ID does not agree with Research_Participant_ID(" + RPI_Value[x] + "), first 9 characters should match"
                    print(error_msg)
                    Biospecimen_object.write_error_msg(BIO_Value[x],"Biospecimen_ID",error_msg,x,'Error')    
            else:
                print("indexes dont match")
                    
                
                    
        Biospecimen_object.write_error_file("Biospecimen_Results_errors_Found.csv")
 ###############################################################################################################################
    print("The entire Validation process took %f seconds. \n" %total_time)
    print("Connection to RDS mysql instance is now closed")
    conn.close

main_function(sample_file)   



