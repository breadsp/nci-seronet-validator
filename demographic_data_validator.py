def demographic_data_validator(demo_data_object,neg_list,pos_list,re,valid_cbc_ids):
    demo_data_object.get_pos_neg_logic(pos_list,neg_list)
    
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
    return demo_data_object
  ###############################################################################################################################
    