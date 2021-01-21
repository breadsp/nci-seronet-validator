########################################################################
def prior_test_result_validator(prior_valid_object,neg_list,pos_list,re,valid_cbc_ids,current_demo):                
    prior_valid_object.get_pos_neg_logic(pos_list,neg_list)
        
    for header_name in prior_valid_object.Column_Header_List:
        test_column = prior_valid_object.Data_Table[header_name];                
        missing_logic,has_logic,missing_data_column,has_data_column = prior_valid_object.check_data_type(test_column,header_name)
                           
        if header_name.find('Research_Participant_ID') > -1:        #checks if Participant ID in valid format
            error_msg = "Value it not a Valid id format, Expecting XX_XXXXXX"
            pattern = re.compile('^[0-9]{2}[_]{1}[0-9]{6}$')    
            [prior_valid_object.valid_ID(header_name,i[1],pattern,valid_cbc_ids,error_msg,has_data_column.index[i[0]],'Error') for i in enumerate(has_data_column)]    
            [prior_valid_object.is_required(header_name,i[1],"All",missing_data_column.index[i[0]],'Error') for i in enumerate(missing_data_column)] 
         
            id_error_list = [i[5] for i in prior_valid_object.error_list_summary if (i[0] == "Error") and (i[4] == "Research_Participant_ID")]
            matching_values = [i for i in enumerate(test_column) if pattern.match(i[1]) is not None]
            if (len(matching_values) > 0) and (len(current_demo) > 0):
                error_msg = "Id is not found in database or in submitted demographic file"
                [prior_valid_object.in_list(header_name,i[1][1],current_demo,error_msg,i[1][0],'Error') for i in enumerate(matching_values)]
        
        elif (header_name.find('Date_of_SARS_CoV_2_PCR_sample_collection') > -1): 
            error_msg = "Value must be a valid date MM/DD/YYYY"
            [prior_valid_object.is_date_time(header_name,i[1],False,error_msg,has_data_column.index[i[0]],'Error') for i in enumerate(has_data_column)]    
            [prior_valid_object.is_required(header_name,i[1],"ALL",missing_data_column.index[i[0]],'Error') for i in enumerate(missing_data_column)]
        elif (header_name.find('Date_of') > -1):                    #checks if column variables are in date format
            error_msg = "Value must be a valid date MM/DD/YYYY or N/A"
            [prior_valid_object.is_date_time(header_name,i[1],True,error_msg,has_data_column.index[i[0]],'Error') for i in enumerate(has_data_column)]  
            [prior_valid_object.is_required(header_name,i[1],"All",missing_data_column.index[i[0]],'Error') for i in enumerate(missing_data_column)] 
        elif (header_name.find('SARS_CoV_2_PCR_Test_Result_Provenance') > -1):
            test_string = ['Self-reported','From Medical Record']
            error_msg = "Value must be one of the following: " + str(test_string)
            [prior_valid_object.in_list(header_name,i[1],test_string,error_msg,has_data_column.index[i[0]],'Error') for i in enumerate(has_data_column)]
            [prior_valid_object.is_required(header_name,i[1],"ALL",missing_data_column.index[i[0]],'Error') for i in enumerate(missing_data_column)]         
        elif (header_name.find('SARS_CoV_2_PCR_Test_Result') > -1):
            test_string = ['Positive', 'Negative']
            error_msg = "Value must be one of the following: " + str(test_string)
            [prior_valid_object.in_list(header_name,i[1],test_string,error_msg,has_data_column.index[i[0]],'Error') for i in enumerate(has_data_column)]
            [prior_valid_object.is_required(header_name,i[1],"ALL",missing_data_column.index[i[0]],'Error') for i in enumerate(missing_data_column)]         
        elif (header_name.find('Test_Result_Provenance') > -1):     #checks result proveance for valid input options  
            test_string = ['Self-reported','From Medical Record','N/A']
            error_msg = "Participant is SARS CoV-2 positive, Value must be one of the following: " + str(test_string)
            pos_test_value = prior_valid_object.Data_Table[prior_valid_object.pos_list_logic & has_logic][header_name]
            [prior_valid_object.in_list(header_name,i[1],test_string,error_msg,pos_test_value.index[i[0]],'Error') for i in enumerate(pos_test_value)]
            
            test_string = ['Self-reported','From Medical Record']
            error_msg = "Participant is SARS CoV-2 negative, Value must be one of the following: " + str(test_string)
            neg_test_value = prior_valid_object.Data_Table[prior_valid_object.neg_list_logic & has_logic][header_name]
            [prior_valid_object.in_list(header_name,i[1],test_string,error_msg,neg_test_value.index[i[0]],'Error') for i in enumerate(neg_test_value)]
            [prior_valid_object.is_required(header_name,i[1],"ALL",missing_data_column.index[i[0]],'Error') for i in enumerate(missing_data_column)]  
        elif (header_name.find('Duration') > -1):                #Is value a number OR is value == N/A    
            if (header_name.find('HAART_Therapy') > -1):
                current_index = 'On_HAART_Therapy'
            else:
                current_index = header_name.replace('Duration_of','Current')
            error_msg = "Participant has " + current_index + " set to Yes. Duration must be a value of 0 or greater"
            test_value = prior_valid_object.Data_Table[(prior_valid_object.Data_Table[current_index] == "Yes")][header_name]
            [prior_valid_object.is_numeric(header_name,False,i[1],0,error_msg,test_value.index[i[0]],'Error') for i in enumerate(test_value)]
                
            error_msg = "Participant has " + current_index + " set to ['No','Unknown','N/A']. Duration must be N/A"
            has_data_column = prior_valid_object.Data_Table.iloc[[i[0] for i in enumerate(prior_valid_object.Data_Table[current_index]) if i[1] in ['No','Unknown','N/A']]][header_name]
            [prior_valid_object.in_list(header_name,i[1],["N/A"],error_msg,has_data_column.index[i[0]],'Error') for i in enumerate(has_data_column)]                   
       
            error_msg = "Unknown value for " + current_index + " for current Participant.  Unable to Validate Duration"
            has_data_column = prior_valid_object.Data_Table.iloc[[i[0] for i in enumerate(prior_valid_object.Data_Table[current_index]) if i[1] not in ['Yes','No','Unknown','N/A']]][header_name]
            [prior_valid_object.in_list(header_name,i[1],[''],error_msg,has_data_column.index[i[0]],'Error') for i in enumerate(has_data_column)]                   
            
            [prior_valid_object.is_required(header_name,i[1],"All",missing_data_column.index[i[0]],'Error') for i in enumerate(missing_data_column)] 
        
        elif (header_name.find('Current') > -1) |  (header_name.find('HAART_Therapy') > -1):    #Is value in [Yes|No|Unknown|N/A]
            test_string = ['Yes','No','Unknown','N/A']
            error_msg = "Participant is SARS_CoV2 Positive. Value must be: " + str(test_string)
            pos_test_value = prior_valid_object.Data_Table[prior_valid_object.pos_list_logic & has_logic][header_name]
            [prior_valid_object.in_list(header_name,i[1],test_string,error_msg,pos_test_value.index[i[0]],'Error') for i in enumerate(pos_test_value)]
            
            test_string = ['Yes','No']
            error_msg = "Participant is SARS_CoV2 Negative. Value must be: " + str(test_string)
            neg_test_value = prior_valid_object.Data_Table[prior_valid_object.neg_list_logic & has_logic][header_name]        
            [prior_valid_object.in_list(header_name,i[1],test_string,error_msg,neg_test_value.index[i[0]],'Error') for i in enumerate(neg_test_value)]
        
            [prior_valid_object.is_required(header_name,i[1],"ALL",missing_data_column.index[i[0]],'Error') for i in enumerate(missing_data_column)]                        
        elif (header_name.lower().find('test_result') > -1) | (header_name.lower().find('seasonal_coronavirus') > -1):      
            test_string = ['Positive','Negative','Equivocal','Not Performed','N/A']
            error_msg = "Participant is SARS_CoV2 Positive. must be: " + str(test_string)
            pos_test_value = prior_valid_object.Data_Table[prior_valid_object.pos_list_logic & has_logic][header_name]
            [prior_valid_object.in_list(header_name,i[1],test_string,error_msg,pos_test_value.index[i[0]],'Error') for i in enumerate(pos_test_value)]
            
            test_string = ['Positive','Negative']
            error_msg = "Participant is SARS_CoV2 Negative. Value must be: " + str(test_string)
            neg_test_value = prior_valid_object.Data_Table[prior_valid_object.neg_list_logic & has_logic][header_name]        
            [prior_valid_object.in_list(header_name,i[1],test_string,error_msg,neg_test_value.index[i[0]],'Error') for i in enumerate(neg_test_value)]
         
            [prior_valid_object.is_required(header_name,i[1],"ALL",missing_data_column.index[i[0]],'Error') for i in enumerate(missing_data_column)]  
         
    prior_valid_object.write_error_file("Prior_Test_Results_Errors_Found.csv")
    return prior_valid_object