def other_files_validator(data_object,re,valid_cbc_ids,biospec_ids,output_file):
    biospec_ids.drop_duplicates(inplace=True)
    data_object.Data_Table = data_object.Data_Table.merge(biospec_ids, on=["Biospecimen_ID"],how="left")
    
    if data_object.File_name in ["Equipment_Metadata.csv","Reagent_Metadata.csv","Consumable_Metadata.csv"]:
        try:
            warning_data = data_object.Data_Table[data_object.Data_Table['Biospecimen_Type'] != "PBMC"]
        except:
            print('error')
        for i in enumerate(warning_data['Biospecimen_Type']):
            curr_type = warning_data['Biospecimen_Type'][warning_data.index[i[0]]]
            if curr_type == curr_type:
               error_msg ="Unable to Validate, Expecting Biospecimen Type to be PMBC, Was found to be " + curr_type +" instead, please check data"
            else:
                error_msg ="Biospecimen ID not found, unable to determine Biospecimen Type. Please check data"
            data_object.write_error_msg("","All Columns",error_msg,warning_data.index[i[0]],'Warning')     
    
    for header_name in data_object.Column_Header_List:
        test_column = data_object.Data_Table[header_name]; 
        missing_logic,has_logic,missing_data_column,has_data_column = data_object.check_data_type(test_column,header_name)
       
        if (header_name in ["Biospecimen_ID"]):
            error_msg = "Value it not a Valid id format, Expecting XX_XXXXXX_XXX"
            pattern = re.compile('^[0-9]{2}[_]{1}[0-9]{6}[_]{1}[0-9]{3}$')    
            [data_object.valid_ID(header_name,i[1],pattern,valid_cbc_ids,error_msg,has_data_column.index[i[0]],'Error') for i in enumerate(has_data_column)]    
            [data_object.is_required(header_name,i[1],"All",missing_data_column.index[i[0]],'Error') for i in enumerate(missing_data_column)]     

            id_error_list = [i[6] for i in data_object.error_list_summary if (i[0] == "Error") and (i[5] == "Biospecimen_ID")]
            matching_values = [i for i in enumerate(test_column) if (pattern.match(i[1]) is not None) and (i[1] not in id_error_list)]
            if (len(matching_values) > 0):
                error_msg = "Id is not found in database or in submitted demographic file"
                [data_object.in_list(header_name,i[1][1],biospec_ids['Biospecimen_ID'].tolist(),error_msg,i[1][0],'Error') for i in enumerate(matching_values)]


        elif (header_name in ["Aliquot_ID"]):
            error_msg = "Value must be a string and NOT N/A"
            [data_object.is_string(header_name,i[1],False,error_msg,has_data_column.index[i[0]],'Error') for i in enumerate(has_data_column)]
            [data_object.is_required(header_name,i[1],"All",missing_data_column.index[i[0]],'Error') for i in enumerate(missing_data_column)]
        elif (header_name in ["Aliquot_Volume"]):
            error_msg = "Value must be a number greater than zero"
            [data_object.is_numeric(header_name,False,i[1],0,error_msg,has_data_column.index[i[0]],'Error') for i in enumerate(has_data_column)]
            [data_object.is_required(header_name,i[1],"All",missing_data_column.index[i[0]],'Error') for i in enumerate(missing_data_column)]
        elif (header_name in ["Aliquot_Tube_Type_Expiration_Date"]):
            error_msg = "Value must be a valid Date MM/DD/YYYY"
            [data_object.is_date_time(header_name,i[1],False,error_msg,has_data_column.index[i[0]],'Error') for i in enumerate(has_data_column)] 
            [data_object.is_required(header_name,i[1],"All",missing_data_column.index[i[0]],'Error') for i in enumerate(missing_data_column)]   
        elif (header_name.find('Aliquot_Tube_Type') > -1) or (header_name.find('Aliquot_Initials') > -1):
            error_msg = "Value must be a string and NOT N/A"
            [data_object.is_string(header_name,i[1],False,error_msg,has_data_column.index[i[0]],'Error') for i in enumerate(has_data_column)] 
            [data_object.is_required(header_name,i[1],"All",missing_data_column.index[i[0]],'Error') for i in enumerate(missing_data_column)] 
        elif ((header_name.find('Expiration_Date') > -1) or (header_name.find('Due_Date') > -1)):
            has_data_column = data_object.Data_Table[(data_object.Data_Table['Biospecimen_Type'] == "PBMC") & has_logic][header_name]
            missing_data = data_object.Data_Table[(data_object.Data_Table['Biospecimen_Type'] == "PBMC") & missing_logic][header_name]
            error_msg = "Value must be a valid Date MM/DD/YYYY"
            [data_object.is_date_time(header_name,i[1],False,error_msg,has_data_column.index[i[0]],'Error') for i in enumerate(has_data_column)] 
            [data_object.is_required(header_name,i[1],"All",missing_data_column.index[i[0]],'Error') for i in enumerate(missing_data_column)]   
        elif (header_name in ["Equipment_ID"] or (header_name.find('Catalog_Number') > -1) or (header_name.find('Lot_Number') > -1)):
            has_data_column = data_object.Data_Table[(data_object.Data_Table['Biospecimen_Type'] == "PBMC") & has_logic][header_name]
            missing_data = data_object.Data_Table[(data_object.Data_Table['Biospecimen_Type'] == "PBMC") & missing_logic][header_name]
            error_msg = "Value must be a string and NOT N/A"
            [data_object.is_string(header_name,i[1],False,error_msg,has_data_column.index[i[0]],'Error') for i in enumerate(has_data_column)]
            [data_object.is_required(header_name,i[1],"All",missing_data_column.index[i[0]],'Error') for i in enumerate(missing_data_column)]
        elif (header_name in ["Equipment_Type","Reagent_Name","Consumable_Name"]):
            if (header_name in ["Equipment_Type"]):
                test_string = ['Refrigerator','-80 Refrigerator', 'LN Refrigerator', 'Microsope', 'Pipettor', 'Controlled-Rate Freezer', 'Automated-Cell Counter']
            elif (header_name in ["Reagent_Name"]):
                test_string =  (['DPBS', 'Ficoll-Hypaque','RPMI-1640','no L-Glutamine','Fetal Bovine Serum','200 mM L-Glutamine',
                                 '1M Hepes','Penicillin/Streptomycin','DMSO', 'Cell Culture Grade','Vital Stain Dye'])
            elif (header_name in ["Consumable_Name"]):
                test_string = ["50 mL Polypropylene Tube", "15 mL Conical Tube" ,"Cryovial Label"] 
        
            has_data_column = data_object.Data_Table[(data_object.Data_Table['Biospecimen_Type'] == "PBMC") & has_logic][header_name]
            missing_data = data_object.Data_Table[(data_object.Data_Table['Biospecimen_Type'] == "PBMC") & missing_logic][header_name]
            error_msg = "Value must be one of the following: " + str(test_string)
            [data_object.in_list(header_name,i[1],test_string,error_msg,has_data_column.index[i[0]],'Error') for i in enumerate(has_data_column)]
           
            [data_object.is_required(header_name,i[1],"All",missing_data_column.index[i[0]],'Error') for i in enumerate(missing_data_column)]
      
    data_object.write_error_file(output_file)
    return data_object
