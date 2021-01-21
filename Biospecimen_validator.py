def Biospecimen_validator(Biospecimen_object,neg_list,pos_list,re,valid_cbc_ids,current_demo):
    for header_name in Biospecimen_object.Column_Header_List:
        test_column = Biospecimen_object.Data_Table[header_name]; 
        missing_logic,has_logic,missing_data_column,has_data_column = Biospecimen_object.check_data_type(test_column,header_name)
                                                         
        if (header_name in ["Research_Participant_ID"]):
            error_msg = "Value it not a Valid id format, Expecting XX_XXXXXX"
            pattern = re.compile('^[0-9]{2}[_]{1}[0-9]{6}$')    
            [Biospecimen_object.valid_ID(header_name,i[1],pattern,valid_cbc_ids,error_msg,has_data_column.index[i[0]],'Error') for i in enumerate(has_data_column)]    
            [Biospecimen_object.is_required(header_name,i[1],"All",missing_data_column.index[i[0]],'Error') for i in enumerate(missing_data_column)] 
            
            id_error_list = [i[5] for i in Biospecimen_object.error_list_summary if (i[0] == "Error") and (i[4] == "Research_Participant_ID")]
            matching_values = [i for i in enumerate(test_column) if (pattern.match(i[1]) is not None) and (i[1] not in id_error_list)]
            if (len(matching_values) > 0):
                error_msg = "Id is not found in database or in submitted demographic file"
                [Biospecimen_object.in_list(header_name,i[1][1],current_demo,error_msg,i[1][0],'Error') for i in enumerate(matching_values)]

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
            [Biospecimen_object.is_string(header_name,i[1],False,error_msg,has_data_column.index[i[0]],'Error') for i in enumerate(has_data_column)] 
            [Biospecimen_object.is_required(header_name,i[1],"All",missing_data_column.index[i[0]],'Error') for i in enumerate(missing_data_column)]       
        elif (header_name.find('Hemocytometer_Count') > -1) or (header_name.find('Automated_Count') > -1):
            test_value = Biospecimen_object.Data_Table[(Biospecimen_object.Data_Table['Biospecimen_Type'] == "PBMC") & has_logic][header_name]
            missing_data_column = Biospecimen_object.Data_Table[(Biospecimen_object.Data_Table['Biospecimen_Type'] == "PBMC") & missing_logic][header_name]
            [Biospecimen_object.is_required(header_name,i[1],"All",missing_data_column.index[i[0]],'Error') for i in enumerate(missing_data_column)]
             
            [Biospecimen_object.is_numeric(header_name,False,i[1],0,error_msg,test_value.index[i[0]],'Error') for i in enumerate(test_value)]
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
            
            has_data_not_pbmc = (test_column[Biospecimen_object.Data_Table['Biospecimen_Type'] != "PBMC"])
            specimen_type = Biospecimen_object.Data_Table[Biospecimen_object.Data_Table['Biospecimen_Type'] != "PBMC"]['Biospecimen_Type']
            for i in enumerate(has_data_not_pbmc):
                if i[1] == '':
                    error_msg = "Biospecimen type is " + specimen_type.iloc[i[0]] + ", Blank values were found, should be N/A"
                    Biospecimen_object.write_error_msg(i[1],header_name,error_msg,specimen_type.index[i[0]],'Warning')
                elif i[1] != "N/A":
                    error_msg = "Biospecimen type is " + specimen_type.iloc[i[0]] + ", unexpected value was found, expected N/A"
                    Biospecimen_object.write_error_msg(i[1],header_name,error_msg,specimen_type.index[i[0]],'Warning')
        elif(header_name in ["Centrifugation_Time","RT_Serum_Clotting_Time"]):
            error_msg = "Biospecimen Type == Serum, Value must be a positive number"                
            test_value = Biospecimen_object.Data_Table[Biospecimen_object.Data_Table['Biospecimen_Type'] == "Serum"][header_name]
            [Biospecimen_object.is_numeric(header_name,True,i[1],0,error_msg,test_value.index[i[0]],'Error') for i in enumerate(test_value)]

            has_data_not_serum = (test_column[Biospecimen_object.Data_Table['Biospecimen_Type'] != "Serum"])
            specimen_type = Biospecimen_object.Data_Table[Biospecimen_object.Data_Table['Biospecimen_Type'] != "Serum"]['Biospecimen_Type']
            for i in enumerate(has_data_not_serum):
                if i[1] == '':
                    error_msg = "Biospecimen type is " + specimen_type.iloc[i[0]] + ", Blank values were found, should be N/A"
                    Biospecimen_object.write_error_msg(i[1],header_name,error_msg,specimen_type.index[i[0]],'Warning')
                elif i[1] != "N/A":
                    error_msg = "Biospecimen type is " + specimen_type.iloc[i[0]] + ", unexpected value was found, expected N/A"
                    Biospecimen_object.write_error_msg(i[1],header_name,error_msg,specimen_type.index[i[0]],'Warning')
        elif(header_name in ["Storage_Start_Time_80_LN2_storage"]):
            error_msg = "Biospecimen Type == Serum, Value must be a Time in 24hour format HH:MM"                
            test_value = Biospecimen_object.Data_Table[Biospecimen_object.Data_Table['Biospecimen_Type'] == "Serum"][header_name]    
            [Biospecimen_object.is_date_time(header_name,i[1],False,error_msg,test_value.index[i[0]],'Error') for i in enumerate(test_value)]   
        
            has_data_not_serum = (test_column[Biospecimen_object.Data_Table['Biospecimen_Type'] != "Serum"])
            specimen_type = Biospecimen_object.Data_Table[Biospecimen_object.Data_Table['Biospecimen_Type'] != "Serum"]['Biospecimen_Type']
            for i in enumerate(has_data_not_serum):
                if i[1] == '':
                    error_msg = "Biospecimen type is " + specimen_type.iloc[i[0]] + ", Blank values were found, should be N/A"
                    Biospecimen_object.write_error_msg(i[1],header_name,error_msg,specimen_type.index[i[0]],'Warning')
                elif i[1] != "N/A":
                    error_msg = "Biospecimen type is " + specimen_type.iloc[i[0]] + ", unexpected value was found, expected N/A"
                    Biospecimen_object.write_error_msg(i[1],header_name,error_msg,specimen_type.index[i[0]],'Warning')
        elif(header_name in ["Initial_Volume_of_Biospecimen"]):
             error_msg = "Value must be a number greater than 0"
             [Biospecimen_object.is_numeric(header_name,False,i[1],0,error_msg,has_data_column.index[i[0]],'Error') for i in enumerate(has_data_column)]
             [Biospecimen_object.is_required(header_name,i[1],"All",missing_data_column.index[i[0]],'Error') for i in enumerate(missing_data_column)]     
             
    live_count = Biospecimen_object.Data_Table[Biospecimen_object.Data_Table['Biospecimen_Type'] != "PBMC"]['Live_Cells_Hemocytometer_Count']
    total_count = Biospecimen_object.Data_Table[Biospecimen_object.Data_Table['Biospecimen_Type'] != "PBMC"]['Total_Cells_Hemocytometer_Count']
    viabilty_count = Biospecimen_object.Data_Table[Biospecimen_object.Data_Table['Biospecimen_Type'] != "PBMC"]['Viability_Hemocytometer_Count']
    specimen_type = Biospecimen_object.Data_Table[Biospecimen_object.Data_Table['Biospecimen_Type'] != "PBMC"]['Biospecimen_Type']
    
    for i in enumerate(specimen_type):
        if (live_count.iloc[i[0]] in ['N/A']) and (total_count.iloc[i[0]] in ['N/A'])  and (viabilty_count.iloc[i[0]] in ['N/A']):
            pass
        elif (live_count.iloc[i[0]] in ['']) and (total_count.iloc[i[0]] in [''])  and (viabilty_count.iloc[i[0]] in ['']):
            error_msg = "Biospecimen type is " + i[1] + ", Blank values were found, should be N/A"
            Biospecimen_object.write_error_msg('','Hemocytometer_Count_Variables',error_msg,specimen_type.index[i[0]],'Warning')
        else:
            error_msg = "Biospecimen type is " + i[1] + ", not expecting Hemocytometer Counts for this type of biospecimen"
            Biospecimen_object.write_error_msg(total_count.iloc[i[0]],'Hemocytometer_Count_Variables',error_msg,specimen_type.index[i[0]],'Warning')
            
    live_count = Biospecimen_object.Data_Table[Biospecimen_object.Data_Table['Biospecimen_Type'] != "PBMC"]['Live_Cells_Automated_Count']
    total_count = Biospecimen_object.Data_Table[Biospecimen_object.Data_Table['Biospecimen_Type'] != "PBMC"]['Total_Cells_Automated_Count']
    viabilty_count = Biospecimen_object.Data_Table[Biospecimen_object.Data_Table['Biospecimen_Type'] != "PBMC"]['Viability_Automated_Count']
    
    for i in enumerate(specimen_type):
        if (live_count.iloc[i[0]] in ['N/A']) and (total_count.iloc[i[0]] in ['N/A'])  and (viabilty_count.iloc[i[0]] in ['N/A']):
            pass
        elif (live_count.iloc[i[0]] in ['']) and (total_count.iloc[i[0]] in [''])  and (viabilty_count.iloc[i[0]] in ['']):
            error_msg = "Biospecimen type is " + i[1] + ", Blank values were found, should be N/A"
            Biospecimen_object.write_error_msg('','Automated_Count_Variables',error_msg,specimen_type.index[i[0]],'Warning')
        else:
            error_msg = "Biospecimen type is " + i[1] + ", not expecting Automated Counts for this type of biospecimen"
            Biospecimen_object.write_error_msg(total_count.iloc[i[0]],'Automated_Count_Variables',error_msg,specimen_type.index[i[0]],'Warning')
       
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
                Biospecimen_object.write_error_msg(i[1][1],"Research_Participant_ID",error_msg,i[1][0],'Error') 
                
    Biospecimen_object.write_error_file("Biospecimen_Results_errors_Found.csv")
    return Biospecimen_object
