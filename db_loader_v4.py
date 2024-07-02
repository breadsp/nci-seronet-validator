from import_loader_v2 import pd, np, pd_s3, boto3, aws_creds_prod, datetime, os, re
from import_loader_v2 import get_box_data_v2, parse, pathlib
import copy
import itertools
from decimal import Decimal


def Db_loader_main(sub_folder, connection_tuple, validation_date, **kwargs):
    """main function that will import data from s3 bucket into SQL database"""
    pd.options.mode.chained_assignment = None
    s3_client = boto3.client('s3', aws_access_key_id=aws_creds_prod.aws_access_id, aws_secret_access_key=aws_creds_prod.aws_secret_key,
                             region_name='us-east-1')
    bucket_name = "nci-cbiit-seronet-submissions-passed"
    data_release = "2.0.0"

    if sub_folder == "Reference Pannel Submissions":
        sql_table_dict = get_sql_dict_ref()
    elif sub_folder == "Vaccine Response Submissions":
        sql_table_dict = get_sql_dict_vacc_v2()
    else:
        return

    #Update_Assay_Data = get_kwarg_parms("Update_Assay_Data", kwargs)
    #Update_Study_Design = get_kwarg_parms("Update_Study_Design", kwargs)
    Update_BSI_Tables = get_kwarg_parms("Update_BSI_Tables", kwargs)
    
    Update_Assay_Data = False
    Update_Study_Design = False
############################################################################################################################
    try:
        conn = connection_tuple[2]
        engine = connection_tuple[1]
        sql_column_df = connection_tuple[0]

        done_submissions = pd.read_sql(("SELECT * FROM Submission"), conn)  # list of all submissions previously done in db
        done_submissions.drop_duplicates("Submission_S3_Path", inplace=True)

        all_submissions = []  # get list of all submissions by CBC
        cbc_code = []
        all_submissions, cbc_code = get_all_submissions(s3_client, bucket_name, sub_folder, "Feinstein_CBC01", 41, all_submissions, cbc_code)
        all_submissions, cbc_code = get_all_submissions(s3_client, bucket_name, sub_folder, "UMN_CBC02", 27, all_submissions, cbc_code)
        all_submissions, cbc_code = get_all_submissions(s3_client, bucket_name, sub_folder, "ASU_CBC03", 32, all_submissions, cbc_code)
        all_submissions, cbc_code = get_all_submissions(s3_client, bucket_name, sub_folder, "Mt_Sinai_CBC04", 14, all_submissions, cbc_code)

        time_stamp = [i.split("/")[2] for i in all_submissions]
        for i in time_stamp:
            if i[2] == '-':
                time_stamp[time_stamp.index(i)] = datetime.datetime.strptime(i, "%H-%M-%S-%m-%d-%Y")
            else:
                time_stamp[time_stamp.index(i)] = datetime.datetime.strptime(i, "%Y-%m-%d-%H-%M-%S")

        #  sort need to work by date time
        all_submissions = [x for _, x in sorted(zip(time_stamp, all_submissions))]  # sort submission list by time submitted
        cbc_code = [x for _, x in sorted(zip(time_stamp, cbc_code))]  # sort submission list by time submitted

        all_submissions = [i for i in enumerate(zip(all_submissions, cbc_code))]
        # Filter list by submissions already done
        #all_submissions = [i for i in all_submissions if i[1][0] not in done_submissions["Submission_S3_Path"].tolist()]
        #all_submissions = [i for i in all_submissions if "CBC02" in i[1][0]]

    except Exception as e:
        all_submissions = []
        print(e)
############################################################################################################################
    master_dict = {}  # dictionary for all submissions labeled as create
    update_dict = {}  # dictionary for all submissions labeled as update
    # all_submissions = [(99, done_submissions["Submission_S3_Path"].tolist()[99])]
    #all_submissions = all_submissions[-5:]
    #all_submissions = [all_submissions[138]]
    
    #all_submissions = [i for i in all_submissions if "Vaccine Response Submissions/Mt_Sinai_CBC04/2023-06-01-12-11-27/submission_007_Treatment Update Correction 4G.zip/UnZipped_Files" in i[1][0]]

    #all_submissions = []    #use this to stop the db loader from running
    try:
        for curr_sub in all_submissions:
            try:
                index = curr_sub[0]# + 1
            except Exception as e:
                print(e)

            folder_path, folder_tail = os.path.split(curr_sub[1][0])
            file_name = curr_sub[1][0].split("/")
            folders = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=folder_path)["Contents"]
            print(f"\nWoring on Submision #{index}: {file_name[1]}:  {file_name[2]} \n {file_name[3]}")

            upload_date, intent, sub_name = get_upload_info(s3_client, bucket_name, curr_sub, sub_folder)  # get submission info
            if intent == "Create":
                master_dict = get_tables_to_load(s3_client, bucket_name, folders, curr_sub, conn, sub_name, index, upload_date, intent, master_dict, data_release)
            elif intent == "Update":
                update_dict = get_tables_to_load(s3_client, bucket_name, folders, curr_sub, conn, sub_name, index, upload_date, intent, update_dict, data_release)
            else:
                print(f"Submission Intent: {intent} is not valid")

        master_dict = fix_aliquot_ids(master_dict, "last", sql_column_df)
        update_dict = fix_aliquot_ids(update_dict, "last", sql_column_df)
        master_data_dict = get_master_dict(master_dict, update_dict, sql_column_df, sql_table_dict)
        #master_data_dict = update_dict

        #if "baseline_visit_date.csv" in master_data_dict and "baseline.csv" in master_data_dict:
        #    x = master_data_dict["baseline_visit_date.csv"]["Data_Table"]
        #    x.rename(columns={"Sunday_Of_Week": "Sunday_Prior_To_First_Visit"}, inplace=True)
        #    x["Sunday_Prior_To_First_Visit"] = pd.to_datetime(x["Sunday_Prior_To_First_Visit"])#
        #
        #    for curr_part in x.index:
        #        sql_qry = (f"update Participant set Sunday_Prior_To_First_Visit = '{x['Sunday_Prior_To_First_Visit'][curr_part].date()}' " +
        #                   f"where Research_Participant_ID = '{x['Research_Participant_ID'][curr_part]}'")
        #        engine.execute(sql_qry)
        #        conn.connection.commit()
        #
        #    y = master_data_dict["baseline.csv"]
        #    y = master_data_dict["baseline.csv"]["Data_Table"]
        #    master_data_dict["baseline.csv"]["Data_Table"] = y.merge(x[["Research_Participant_ID","Sunday_Prior_To_First_Visit"]])
        #    master_data_dict = {"baseline.csv": master_data_dict["baseline.csv"]}

        if "baseline.csv" in master_data_dict:
            master_data_dict = update_obesity_values(master_data_dict)
            x = master_dict['baseline.csv']["Data_Table"]
            # x = x.query("Age > 0")
            master_dict['baseline.csv']["Data_Table"] = x.drop_duplicates('Research_Participant_ID')
            
        if "covid_vaccination_status.csv" in master_data_dict:
            x = master_dict["covid_vaccination_status.csv"]["Data_Table"]
            x.sort_values(["Research_Participant_ID", "Vaccination_Status", "Submission_Index"], inplace=True)
            # x = x.query("Age > 0")
            master_dict["covid_vaccination_status.csv"]["Data_Table"] = x
            
        if "treatment_history.csv" in master_data_dict and "treatment_history_prov.csv" in master_data_dict:
            tbase = master_dict["treatment_history.csv"]["Data_Table"]
            tprov = master_dict["treatment_history_prov.csv"]["Data_Table"]
            tbase.replace("Baseline(1)", "1", inplace=True)
            tprov.replace("Baseline(1)", "1", inplace=True)
            tprov.drop(["Comments", "Submission_Index", "Submission_CBC"], axis=1, inplace=True)
            tbase = tbase.merge(tprov, how="left")
            tbase.drop_duplicates(inplace=True)
            master_dict["treatment_history.csv"]["Data_Table"] = tbase
        
        if "cancer_cohort.csv" in master_data_dict and "cancer_cohort_prov.csv" in master_data_dict:
            cbase = master_dict["cancer_cohort.csv"]["Data_Table"]
            cprov = master_dict["cancer_cohort_prov.csv"]["Data_Table"]
            cbase.replace("Baseline(1)", "1", inplace=True)
            cprov.replace("Baseline(1)", "1", inplace=True)
            cprov = cprov[['Research_Participant_ID', 'Cohort', 'Visit_Number', 'Cancer_Provenance']]
            cbase = cbase.merge(cprov, how="left")
            cbase.drop_duplicates(inplace=True)
            master_dict["cancer_cohort.csv"]["Data_Table"] = cbase
                        
        #if "covid_vaccination_status.csv" in master_data_dict:
        #    x = master_data_dict["covid_vaccination_status.csv"]["Data_Table"]
        #    x['Visit_Number'] = x["Visit_Number"].replace("Baseline(1)", "1")
        #    x.drop_duplicates(['Research_Participant_ID', 'SARS-CoV-2_Vaccination_Date_Duration_From_Index'], keep="last", inplace=True)
        #    master_data_dict["covid_vaccination_status.csv"]["Data_Table"] = x
        #    master_data_dict = {"covid_vaccination_status.csv": master_data_dict["covid_vaccination_status.csv"]}
    
        #if study_type == "Vaccine_Response":
        #    cohort_file = r"C:\Users\breadsp2\Downloads\Release_1.0.0_by_cohort.xlsx"
        #    x = pd.read_excel(cohort_file)
        #    visit_table = pd.read_sql(("SELECT * FROM `seronetdb-Vaccine_Response`.Participant_Visit_Info;"), sql_tuple[1])
        #    x.drop("CBC", axis=1, inplace=True)
        #    visit_table.rename(columns={"Cohort": "CBC_Grouping"}, inplace=True)
        #    y = visit_table.merge(x)
        #    update_tables(conn, engine, ["Visit_Info_ID"], y, "Participant_Visit_Info")

        if Update_Assay_Data is True:
            master_data_dict = upload_assay_data(master_data_dict)
        if Update_Study_Design is True:
            master_data_dict["study_design.csv"] = {"Data_Table": []}
            master_data_dict["study_design.csv"]["Data_Table"] = get_box_data_v2.get_study_design()
        if Update_BSI_Tables is True:
            master_data_dict = get_bsi_files(s3_client, bucket_name, sub_folder, master_data_dict)
        if "secondary_confirmation_test_result.csv" in master_data_dict:
            master_data_dict = update_secondary_confirm(master_data_dict, sql_column_df)

        if "Add_Blinded_Results" in kwargs:
            eval_data = []
            if kwargs["Add_Blinded_Results"] is True:
                bucket = "nci-cbiit-seronet-submissions-passed"
                key = "Serology_Data_Files/Reference_Panel_Files/Reference_Panel_Submissions/"
                resp = s3_client.list_objects_v2(Bucket=bucket, Prefix=key)
                for curr_file in resp["Contents"]:
                    try:
                        if "blinded_validation_panel_results_example" in curr_file["Key"]:  #testing file, ignore
                            continue
                        elif ".xlsx" in curr_file["Key"]:
                            x = pd_s3.get_df_from_keys(s3_client, bucket, curr_file["Key"], suffix="xlsx",
                                                       format="xlsx", na_filter=False, output_type="pandas")
                        elif ".csv" in curr_file["Key"]:
                            x = pd_s3.get_df_from_keys(s3_client, bucket, curr_file["Key"], suffix="csv",
                                                       format="csv", na_filter=False, output_type="pandas")
                        else:
                            continue
                        if len(eval_data) == 0:
                            eval_data = x
                        else:
                            eval_data = pd.concat([eval_data, x])
                    except Exception as e:
                        print(e)
                eval_data.reset_index(inplace=True, drop=True)
                sub_id = eval_data.loc[eval_data['Subaliquot_ID'].str.contains('FD|FS')]
                bsi_id = eval_data.loc[~eval_data['Subaliquot_ID'].str.contains('FD|FS')]
                sub_id.rename(columns={'Subaliquot_ID': "CGR_Aliquot_ID"}, inplace=True)

                child_data = pd.read_sql(("SELECT Biorepository_ID, Subaliquot_ID, CGR_Aliquot_ID FROM BSI_Child_Aliquots"), conn)
                eval_data = pd.concat([sub_id.merge(child_data), bsi_id.merge(child_data)])
                master_data_dict = add_assay_to_dict(master_data_dict, "Blinded_Evaluation_Panels.csv", eval_data)

        if "update_CDC_tables" in kwargs:
            if kwargs["update_CDC_tables"] is True:
                bucket = "nci-cbiit-seronet-submissions-passed"
                key = "Serology_Data_Files/CDC_Confirmation_Results/"
                resp = s3_client.list_objects_v2(Bucket=bucket, Prefix=key)
                CDC_data_IgG = pd.DataFrame()
                CDC_data_IgM = pd.DataFrame()
                for curr_file in resp["Contents"]:
                    #  curr_file = get_recent_date(resp["Contents"])
                    try:
                        if ".xlsx" in curr_file["Key"]:
                            file_date = curr_file["Key"][-13:-5]
                            CDC_data_IgG = create_CDC_data(s3_client, bucket, curr_file, "IgG", CDC_data_IgG, file_date)
                            CDC_data_IgM = create_CDC_data(s3_client, bucket, curr_file, "IgM", CDC_data_IgM, file_date)
                    except Exception as e:
                        print(e)
                CDC_Data = pd.concat([CDC_data_IgM, CDC_data_IgG])

                CDC_Data["BSI_Parent_ID"] = [i[:7] + " 0001" for i in CDC_Data["Patient ID"].tolist()]
                master_data_dict = add_assay_to_dict(master_data_dict, "CDC_Data.csv", CDC_Data)

        if len(master_data_dict) > 0:
            valid_files = [i for i in sql_table_dict if "_sql.csv" not in i]
            valid_files = [i for i in valid_files if i in master_data_dict]
            filtered_tables = [value for key, value in sql_table_dict.items() if key in valid_files]
            # master_data_dict = {"submission.csv": master_data_dict["submission.csv"]}
            if "covid_history.csv" in master_data_dict:
                master_data_dict = check_decision_tree(master_data_dict)
            tables_to_check = list(itertools.chain(*filtered_tables))
            add_tables_to_database(engine, conn, sql_table_dict, sql_column_df, master_data_dict, tables_to_check, [])

    except Exception as e:
        display_error_line(e)
    
    # update_norm_cancer(conn, engine)

    return sql_table_dict


def create_CDC_data(s3_client, bucket, curr_file, sheet, df, file_date):
    x = pd_s3.get_df_from_keys(s3_client, bucket, curr_file["Key"], suffix="xlsx",
                               format="xlsx", na_filter=False, output_type="pandas", sheet_name=sheet)
    x["file_date"] = file_date
    x["Measurand_Antibody"] = sheet
    x["Patient ID"] = [i.replace(" 9002", "") for i in x["Patient ID"]]
    df = pd.concat([df, x])
    df.drop_duplicates(["Patient ID"], inplace=True, keep='last')
    return df


def display_error_line(ex):
    trace = []
    tb = ex.__traceback__
    while tb is not None:
        trace.append({"filename": tb.tb_frame.f_code.co_filename,
                      "name": tb.tb_frame.f_code.co_name,
                      "lineno": tb.tb_lineno})
        tb = tb.tb_next
    print(str({'type': type(ex).__name__, 'message': str(ex), 'trace': trace}))


def get_recent_date(files):
    curr_date = (files[0]["LastModified"]).replace(tzinfo=None)
    index = -1
    for file in files:
        index = index + 1
        if (file["LastModified"]).replace(tzinfo=None) > curr_date:
            curr_date = (file["LastModified"]).replace(tzinfo=None)
            curr_value = index
    return files[curr_value]["Key"]


def get_kwarg_parms(update_str, kwargs):
    if update_str in kwargs:
        Update_Table = kwargs[update_str]
    else:
        Update_Table = False
    return Update_Table


def check_decision_tree(master_data_dict):
    data_table = master_data_dict["covid_history.csv"]["Data_Table"]
    data_table.replace("No COVID Event Reported", "No COVID event reported", inplace=True)

    no_covid_data = data_table.query("COVID_Status in ['No COVID event reported', 'No COVID data collected']")
    pos_data = data_table[data_table["COVID_Status"].str.contains("Positive")]  # samples that contain at least 1 positive
    neg_data = data_table[data_table["COVID_Status"].str.contains("Negative")]
    neg_data = neg_data[~neg_data["COVID_Status"].str.contains("Positive")]     # samples that are all negative

    no_covid_data = set_col_vals(no_covid_data, "N/A", "N/A", "N/A", 'Not Reported', 'N/A')
    neg_data = set_col_vals(neg_data, "N/A", "N/A", "N/A", 0, 'N/A')

    pos_data["SARS-CoV-2_Variant"] = pos_data["SARS-CoV-2_Variant"].replace("Unavailable", "Unknown")
    pos_data["SARS-CoV-2_Variant"] = pos_data["SARS-CoV-2_Variant"].replace("N/A", "Unknown")

    pos_yes_sys = pos_data.query("Symptomatic_COVID == 'Yes'")
    pos_no_sys = pos_data.query("Symptomatic_COVID == 'No'")
    pos_ukn_sys = pos_data.query("Symptomatic_COVID == 'Unknown'")
    pos_no_sys = set_col_vals(pos_no_sys, True, True, "No", 1, 'N/A')       # value of true means keep same
    pos_ukn_sys = set_col_vals(pos_ukn_sys, True, True, "Unknown", True, 'No symptoms reported')     # value of true means keep same

    data_table_2 = pd.concat([pos_yes_sys, pos_no_sys, pos_ukn_sys, neg_data, no_covid_data])
    master_data_dict["covid_history.csv"]["Data_Table"] = data_table_2
    return master_data_dict


def set_col_vals(df_data, breakthrough, variant, covid, disease, symptoms):
    if breakthrough is not True:
        df_data["Breakthrough_COVID"] = breakthrough
    if variant is not True:
        df_data["SARS-CoV-2_Variant"] = variant
    if covid is not True:
        df_data["Symptomatic_COVID"] = covid
    if disease is not True:
        df_data["Disease_Severity"] = disease
    if symptoms is not True:
        df_data["Symptoms"] = symptoms
    return df_data


def get_all_submissions(s3_client, bucket_name, sub_folder, cbc_name, cbc_id, all_submissions, cbc_code):
    """ scans the buceket name and provides a list of all files paths found """
    uni_submissions = []
    try:
        paginator = s3_client.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=bucket_name, Prefix=sub_folder + "/" + cbc_name)
        for page in pages:
            for key_list in page['Contents']:
                file_parts = os.path.split(key_list["Key"])[0]
                if os.path.split(file_parts)[-1] == "UnZipped_Files":
                    uni_submissions = uni_submissions + [file_parts]
                
        #key_list = s3_client.list_objects(Bucket=bucket_name, Prefix=sub_folder + "/" + cbc_name)
        #if 'Contents' in key_list:
        #    key_list = key_list["Contents"]
            #file_parts = os.path.split(key_list["Key"])[0]
              
                   
            #key_list = [i["Key"] for i in key_list if ("UnZipped_Files" in i["Key"])]
            #file_parts = [os.path.split(i)[0] for i in key_list]
            #file_parts = [i for i in file_parts if "test/" not in i[0:5]]
            #file_parts = [i for i in file_parts if "Submissions_in_Review" not in i]
            #uni_submissions = list(set(file_parts))
        #else:
        #    uni_submissions = []  # no submissions found for given cbc
    except Exception:
        print("Erorr found")
    finally:
        uni_submissions = list(set(uni_submissions))
        uni_submissions.sort()
        cbc_code = cbc_code + [str(cbc_id)]*len(uni_submissions)
        return all_submissions + uni_submissions, cbc_code


def get_cbc_id(conn, cbc_name):
    cbc_table = pd.read_sql("Select * FROM CBC", conn)
    cbc_table = cbc_table.query("CBC_Name == @cbc_name")
    cbc_id = cbc_table["CBC_ID"].tolist()
    return cbc_id[0]


def get_sql_info(conn, sql_table, sql_column_df):
    col_names = sql_column_df.query("Table_Name==@sql_table")
    prim_key = col_names.query("Primary_Key == 'True' and Var_Type not in ['INTEGER', 'FLOAT']")
    sql_df = pd.read_sql(f"Select * FROM {sql_table}", conn)
    sql_df = sql_timestamp(sql_df, sql_column_df)
    primary_keys = prim_key["Column_Name"].tolist()
    return primary_keys, col_names, sql_df


def clean_tube_names(curr_table):
    curr_cols = curr_table.columns.tolist()

    curr_cols = [i.replace("Collection_Tube", "Tube") for i in curr_cols]
    curr_cols = [i.replace("Aliquot_Tube", "Tube") for i in curr_cols]
    # curr_cols = [i.replace("Tube_Type_Expiration_", "Tube_Lot_Expiration_") for i in curr_cols]
    curr_cols = [i.replace("Biospecimen_Company", "Biospecimen_Collection_Company") for i in curr_cols]
    curr_table.columns = curr_cols
    curr_table["Tube_Type_Lot_Number"] = [str(i) for i in curr_table["Tube_Type_Lot_Number"]]
    curr_table["Tube_Type_Catalog_Number"] = [str(i) for i in curr_table["Tube_Type_Catalog_Number"]]
    return curr_table


def get_upload_info(s3_client, bucket_name, curr_sub, sub_folder):
    file_sep = os.path.sep
    if sub_folder in curr_sub[1][0]:
        sub_obj = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=curr_sub[1][0])
    else:
        sub_obj = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=sub_folder + file_sep + curr_sub[1][0])
    try:
        upload_date = sub_obj["Contents"][0]["LastModified"]
        upload_date = upload_date.replace(tzinfo=None)  # removes timezone element from aws
    except Exception:
        upload_date = 0
    submission_file = [i["Key"] for i in sub_obj["Contents"] if "submission.csv" in i["Key"]]
    submission_file = submission_file[0].replace(".csv", "")

    curr_table = pd_s3.get_df_from_keys(s3_client, bucket_name, submission_file, suffix="csv", format="csv", na_filter=False, output_type="pandas")
    intent = curr_table.iloc[3][1]
    sub_name = curr_table.columns[1]
    return upload_date, intent, sub_name


def get_tables_to_load(s3_client, bucket, folders, curr_sub, conn, sub_name, index, upload_date, intent, master_dict, data_release):
    """Takes current submission and gets all csv files into pandas tables """
    files = [i["Key"] for i in folders if curr_sub[1][0] in i["Key"]]
    files = [i for i in files if ".csv" in i]

    #files = [i for i in files if i.split("/")[-1] in ['baseline.csv', 'follow_up.csv', 'submission.csv', 'treatment_history.csv']]
    files = [i for i in files if i.split("/")[-1] in ['aliquot.csv', 'biospecimen.csv', 'shipping_manifest.csv']]
    #files = [i for i in files if i.split("/")[-1] in ['biospecimen_test_result.csv','covid_history.csv', 'covid_vaccination_status.csv']]
    #files = [i for i in files if i.split("/")[-1] in ['equipment.csv', 'consumable.csv', 'reagent.csv']]
    #files = [i for i in files if i.split("/")[-1] in ['autoimmune_cohort.csv', 'cancer_cohort.csv', 'hiv_cohort.csv'," organ_transplant_cohort.csv"]]
    
    if len(files) == 0:
        return master_dict
    data_dict = get_data_dict(s3_client, bucket, files, conn, curr_sub, sub_name, index, upload_date, intent, data_release)

    if len(data_dict) > 0:
        master_dict = combine_dictionaries(master_dict, data_dict)
    return master_dict


def get_data_dict(s3_client, bucket, files, conn, curr_sub, sub_name, index, upload_date, intent, data_release):
    data_dict = {}
    for curr_file in files:
        split_path = os.path.split(curr_file)
        try:
            if "study_design" in split_path[1]:
                continue
            elif "submission" in split_path[1]:
                curr_table = populate_submission(conn, curr_sub, sub_name, index, upload_date, intent, data_dict)
            else:
                curr_table = pd_s3.get_df_from_keys(s3_client, bucket, split_path[0], suffix=split_path[1],
                                                    format="csv", na_filter=False, output_type="pandas")
                if "Age" in curr_table.columns:
                    err_idx = curr_table.query("Research_Participant_ID in ['14_M95508'] and Age in ['93', 93] or " +
                                               "Research_Participant_ID in ['14_M80341'] and Age in ['96', 96]")
                    curr_table["Data_Release_Version"] = data_release
                    if len(err_idx) > 0:
                        curr_table = curr_table.drop(err_idx.index)
                curr_table["Submission_Index"] = str(index)
                #x = [i for i in curr_table.columns if i.find("Provenance") > 0]
                #if len(x) > 0:
                #    curr_table.drop(x, axis=1, inplace=True)
                
            curr_table = curr_table.loc[~(curr_table == '').all(axis=1)]
            curr_table.rename(columns={"Cohort": "CBC_Classification"})
            
            #following partipnts have been removed from study, delete all data
            remove_ids = ['27_300089','27_300106','27_300168','27_300537','27_400094']
            if 'Research_Participant_ID' in curr_table.columns:
                curr_table = curr_table[curr_table["Research_Participant_ID"].str.contains('|'.join(remove_ids)) == False]
                curr_table = curr_table.query("Research_Participant_ID != '' and Research_Participant_ID == Research_Participant_ID")
            if 'Biospecimen_ID' in curr_table.columns:
                curr_table = curr_table[curr_table["Biospecimen_ID"].str.contains('|'.join(remove_ids)) == False]
                curr_table = curr_table.query("Biospecimen_ID != '' and Biospecimen_ID == Biospecimen_ID")
            if 'Aliquot_ID' in curr_table.columns:
                curr_table = curr_table[curr_table["Aliquot_ID"].str.contains('|'.join(remove_ids)) == False]
            if "Cohort" in curr_table.columns:
                curr_table["Cohort"].replace("Organ_Transplant", "Transplant", inplace=True)
                curr_table["Cohort"].replace("Healthy_Control", "Healthy Control", inplace=True)
        except Exception as e:
            print(e)
            
        #if split_path[1] == "northwell_treatment.csv" or split_path[1] == "treatment.csv":
        #    split_path = (split_path[0], "treatment_history.csv")
        #if  split_path[1] == "treatment.csv":
        #    split_path = (split_path[0], "treatment_history.csv")
        if split_path[1] == "followup.csv":
            split_path = (split_path[0], "follow_up.csv")
        elif split_path[1] == "consumables.csv":
            split_path = (split_path[0], "consumable.csv")
        elif split_path[1] == "cancer_cohort.csv":
            #some cancer durations were ogroanly reported in days then later converted to years
            try:
                curr_table["new_index"] = [ 0 if i == "Not Reported" else float(i) for i in curr_table["Year_Of_Diagnosis_Duration_From_Index"]]
            except Exception as e:
                print(e)
#            curr_table = curr_table.query("new_index >= -50")
            
        curr_table = clean_up_tables(curr_table)
        curr_table["Submission_CBC"] = curr_sub[1][1]
        if "secondary_confirmation" in split_path[1]:
            data_dict["secondary_confirmation_test_result.csv"] = {"Data_Table": []}
            data_dict["secondary_confirmation_test_result.csv"]["Data_Table"] = curr_table
        else:
            data_dict[split_path[1]] = {"Data_Table": []}
            #  curr_table.dropna(inplace=True)
            data_dict[split_path[1]]["Data_Table"] = curr_table
    return data_dict


def update_obesity_values(master_data_dict):
    baseline = master_data_dict["baseline.csv"]["Data_Table"]
    try:
        baseline["BMI"] = baseline["BMI"].replace("Not Reported", -1e9)
        baseline["BMI"] = baseline["BMI"].replace("N/A", -1e9)
        baseline["BMI"] = [float(i) for i in baseline["BMI"]]

        baseline.loc[baseline.query("BMI < 0").index, "Obesity"] = "Not Reported"
        baseline.loc[baseline.query("BMI < 18.5 and BMI > 0").index, "Obesity"] = "Underweight"
        baseline.loc[baseline.query("BMI >= 18.5 and BMI <= 24.9").index, "Obesity"] = "Normal Weight"
        baseline.loc[baseline.query("BMI >= 25.0 and BMI <= 29.9").index, "Obesity"] = "Overweight"
        baseline.loc[baseline.query("BMI >= 30.0 and BMI <= 34.9").index, "Obesity"] = "Class 1 Obesity"
        baseline.loc[baseline.query("BMI >= 35.0 and BMI <= 39.9").index, "Obesity"] = "Class 2 Obesity"
        baseline.loc[baseline.query("BMI >= 40").index, "Obesity"] = "Class 3 Obesity"

        baseline["BMI"] = baseline["BMI"].replace(-1e9, np.nan)
    except Exception as e:
        print(e)

    master_data_dict["baseline.csv"]["Data_Table"] = baseline
    return master_data_dict


def add_error_flags(curr_table, err_idx):
    if len(err_idx) > 0:
        curr_table.loc[err_idx.index, "Error_Flag"] = "Yes"
    return curr_table


def get_master_dict(master_data_dict, master_data_update, sql_column_df, sql_table_dict):
    for key in master_data_dict.keys():
        try:
            table = sql_table_dict[key]
            primary_key = sql_column_df.query(f"Table_Name == {table} and Primary_Key == 'True'")["Column_Name"].tolist()
        except Exception:
            primary_key = "Biospecimen_ID"
        if key in ['shipping_manifest.csv']:
            primary_key = 'Current Label'
        if "Test_Result" in primary_key:
            primary_key[primary_key.index("Test_Result")] = 'SARS_CoV_2_PCR_Test_Result'

        if isinstance(primary_key, str):
            primary_key = [primary_key]
            
        if "Biospecimens_Collected" in primary_key: 
            primary_key.remove("Biospecimens_Collected")
        if key in master_data_update.keys() | master_data_dict.keys():
            try:

                if key in master_data_dict and key in master_data_update:
                    x = pd.concat([master_data_dict[key]["Data_Table"], master_data_update[key]["Data_Table"]])
                elif key in master_data_dict:
                    x = master_data_dict[key]["Data_Table"]
                elif key in master_data_update:
                    x = master_data_update[key]["Data_Table"]
                
                x.reset_index(inplace=True, drop=True)
                x = correct_var_types(x, sql_column_df, table)
                if "Cohort" in x.columns:
                    x = x.query("Cohort not in ('nan')")
                    part_list = list(set(x["Research_Participant_ID"]))
                    for curr_part in part_list:
                        if curr_part[:2] in ['41']:
                            pass
                        else:
                            test_id = x.query("Research_Participant_ID == @curr_part")
                            if len(list(set(test_id["Cohort"]))) == 1:
                                pass
                            else:
                                test_id["Submission_Index"] = [int(i) for i in test_id['Submission_Index']]
                                last_index = max([float(i) for i in test_id['Submission_Index']])
                                correct_cohort = list(set(test_id.query("Submission_Index == @last_index")["Cohort"]))
                                x["Cohort"][test_id.index] = correct_cohort[0]


                if "Visit_Info_ID" in primary_key and "Visit_Info_ID" not in x.columns:
                    x, primary_key = add_visit_info(x, key, primary_key)
                if key not in ["submission.csv"]:
                    primary_key = [i for i in primary_key if i in x.columns]
                    primary_key = list(set(primary_key))
                    if len(primary_key) > 0:
                        x["Submission_Index"] = [int(i) for i in x["Submission_Index"]]
                        x = x.sort_values(primary_key + ["Submission_Index"])
                        if key in ['shipping_manifest.csv']:
                            x = x.drop_duplicates(primary_key, keep='last')
                        else:
                            first_data = x.drop_duplicates(primary_key, keep='first')
                            last_data = x.drop_duplicates(primary_key, keep='last')
                            last_data.drop("Submission_Index", axis=1, inplace=True)
                            x = last_data.merge(first_data[primary_key + ["Submission_Index"]])

                        
                master_data_dict[key]["Data_Table"] = x
            except Exception as e:
                print(e)

    for key in master_data_update.keys():  # key only in update
        if key not in master_data_dict.keys():
            master_data_dict[key] = master_data_update[key]

    return master_data_dict


def add_visit_info(df, curr_file, primary_key):
    if curr_file == "baseline.csv":
        df['Type_Of_Visit'] = "Baseline"
        df['Visit_Number'] = "1"
        df['Unscheduled_Visit'] = "No"
        
        #df['Type_Of_Visit'] = "MISPA"
        #df['Visit_Number'] = "-1"
        #df['Unscheduled_Visit'] = "Yes"
    elif curr_file == "follow_up.csv":
        df['Type_Of_Visit'] = "Follow_up"
        base = df.query("Baseline_Visit == 'Yes'")
        if len(base) > 0:
            df.loc[base.index, "Type_Of_Visit"] = "Baseline"
    else:
        primary_key.append("Research_Participant_ID")
        primary_key.append("Cohort")
        primary_key.append("Visit_Number")
        return df, primary_key
    
    list_of_visits = list(range(1,20)) + [str(i) for i in list(range(1,20))] + [-1, '-1']
    df["Visit_Info_ID"] = (df["Research_Participant_ID"] + " : " + [i[0] for i in df["Type_Of_Visit"]] +
                           ["%02d" % (int(i),) if i in list_of_visits else i for i in df['Visit_Number']])
    return df, primary_key


def convert_data_type(v, var_type):
    if isinstance(v, datetime.datetime) and var_type.lower() == "datetime":
        return v
    if isinstance(v, datetime.date) and var_type.lower() == "date":
        return v
    if isinstance(v, datetime.time) and var_type.lower() == "time":
        return v
    if var_type == "VARCHAR(255)":
        return v

    if v == "Baseline(1)":
        v = 1

    if str(v).find('_') > 0:
        return v
    try:
        float(v)        # value is a number
        if (float(v) * 10) % 10 == 0 or (float(v) * 10) % 10 == 0.0:
            return int(float(v))
        else:
            return round(float(v), 5)
    except ValueError:
        try:
            if var_type.lower() == "datetime":
                return parse(v)
            elif var_type.lower() == "date":
                return parse(v).date()
        except ValueError:
            return v


def correct_var_types(data_table, sql_column_df, curr_table):
    col_names = data_table.columns
    for curr_col in col_names:
        if "Derived_Result" in data_table.columns:
            data_table = updated_derived(data_table)
        z = sql_column_df.query("Column_Name == @curr_col and Table_Name == @curr_table").drop_duplicates("Column_Name")
        if len(z) > 0:
            var_type = z.iloc[0]["Var_Type"]
        else:
            var_type = "VARCHAR(255)"
        if var_type in ['INTEGER', 'FLOAT', 'DOUBLE']:
            data_table[curr_col].replace("N/A", np.nan, inplace=True)
        if curr_col in ["Age", "Storage_Time_in_Mr_Frosty", "Biospecimen_Collection_to_Test_Duration", "BMI", "Derived_Result"]:
            data_table = round_data(data_table, curr_col, 1)
            
        if curr_table == 'Submission':
            data_table[curr_col] = [convert_data_type(c, var_type) for c in data_table[curr_col]]
            continue

        if curr_col in ["Derived_Result"]:
            data_table = round_data(data_table, curr_col, 3)
        elif curr_col in ["Sample_Dilution"] and "Subaliquot_ID" in data_table.columns:  # special formating for reference panel testing
            data_table[curr_col].replace("none", "1", inplace=True)   #no dilution is same as 1:1 dilution
            data_table[curr_col] = [str(f"{Decimal(i):.2E}") for i in data_table[curr_col]]
        elif "varchar" in var_type.lower():
            data_table[curr_col] = [str(i) for i in data_table[curr_col]]
        else:
            data_table[curr_col] = [convert_data_type(c, var_type) for c in data_table[curr_col]]
    return data_table


def round_data(data_table, test_col, round_unit):
    for x in data_table.index:
        try:
            if data_table.loc[x, test_col] == "90+":
                data_table.loc[x, test_col] = 90
            curr_data = round(float(data_table.loc[x, test_col]), round_unit)
            if (curr_data * 10) % 10 == 0.0:
                curr_data = int(curr_data)
            data_table.loc[x, test_col] = float(curr_data)
        except Exception:
            data_table.loc[x, test_col] = str(data_table.loc[x, test_col])
    return data_table


def clean_up_tables(curr_table):
    if "Submission_Index" in curr_table.columns:
        x = curr_table.drop("Submission_Index", axis=1)
        x.replace("", float("NaN"), inplace=True)
        #x.dropna(axis=0, how="all", thresh=None, subset=None, inplace=True)
        x.dropna(axis=0, how="all", subset=None, inplace=True)
        z = curr_table["Submission_Index"].to_frame()
        curr_table = x.merge(z, left_index=True, right_index=True)
    else:
        curr_table.dropna(axis=0, how="all", thresh=None, subset=None, inplace=True)
    if len(curr_table) > 0:
        missing_logic = curr_table.eq(curr_table.iloc[:, 0], axis=0).all(axis=1)
        curr_table = curr_table[[i is not True for i in missing_logic]]
        curr_table = curr_table.loc[:, ~curr_table .columns.str.startswith('Unnamed')]
        for iterC in curr_table.columns:
            try:
                curr_table[iterC] = curr_table[iterC].apply(lambda x: x.replace('–', '-'))
            except Exception:
                pass
    if "Comments" in curr_table.columns:
        curr_table = curr_table.query("Comments not in ['Invalid data entry; do not include']")
    return curr_table


def sql_timestamp(sql_df, sql_column_df):
    new_list = sql_column_df.query("Var_Type == 'TIME'")
    for i in new_list["Column_Name"].tolist():
        if i in sql_df.columns.tolist():
            curr_col = sql_df[i]
            for iterC in curr_col.index:
                if (sql_df[i][iterC] == sql_df[i][iterC]) and (isinstance(sql_df[i][iterC], pd.Timedelta)):
                    hours = int((sql_df[i][iterC].seconds/60)/60)
                    minutes = int((sql_df[i][iterC].seconds % 3600)/60)
                    seconds = sql_df[i][iterC].seconds - (hours*3600 + minutes*60)
                    sql_df[i][iterC] = datetime.time(hours, minutes, seconds)
    return sql_df


def populate_submission(conn, curr_sub, sub_name, index, upload_date, intent, data_dict):
    x_name = pathlib.PurePath(curr_sub[1][0])
    part_list = x_name.parts
    try:
        curr_time = datetime.datetime.strptime(part_list[2], "%H-%M-%S-%m-%d-%Y")
    except Exception:  # time stamp was corrected
        curr_time = datetime.datetime.strptime(part_list[2], "%Y-%m-%d-%H-%M-%S")
    cbc_id = get_cbc_id(conn, sub_name)
    file_name = re.sub("submission_[0-9]{3}_", "", part_list[3])
    sql_df = pd.DataFrame([[index, cbc_id, curr_time, sub_name, file_name, curr_sub[1][0], upload_date, intent]],
                          columns=["Submission_Index", "Submission_CBC_ID", "Submission_Time",
                                   "Submission_CBC_Name", "Submission_File_Name", "Submission_S3_Path",
                                   "Date_Submission_Validated", "Submission_Intent"])
    return sql_df


def add_submission_data(data_dict, conn, csv_sheet):
    sub_data = data_dict["submission.csv"]["Data_Table"]
    curr_table = data_dict[csv_sheet]["Data_Table"]
    curr_table["Submission_CBC"] = get_cbc_id(conn, sub_data.columns[1])
    return curr_table


def combine_dictionaries(master_data_dict, data_dict):
    for curr_file in data_dict:
        try:
            if curr_file not in master_data_dict:
                master_data_dict[curr_file] = {"Data_Table": data_dict[curr_file]["Data_Table"]}
            else:
                x = pd.concat([master_data_dict[curr_file]["Data_Table"], data_dict[curr_file]["Data_Table"]],
                              axis=0, ignore_index=True).reset_index(drop=True)
                master_data_dict[curr_file]["Data_Table"] = x
        except Exception as e:
            print(e)
    return master_data_dict


def fix_aliquot_ids(master_data_dict, keep_order, sql_column_df):
    if "aliquot.csv" in master_data_dict:
        z = master_data_dict["aliquot.csv"]["Data_Table"]["Aliquot_ID"].tolist()
        master_data_dict["aliquot.csv"]["Data_Table"]["Aliquot_ID"] = zero_pad_ids(z)
        z = master_data_dict["aliquot.csv"]["Data_Table"]["Aliquot_Volume"].tolist()
        z = [(i.replace("N/A", "0")) if isinstance(i, str) else i for i in z]
        master_data_dict["aliquot.csv"]["Data_Table"]["Aliquot_Volume"] = z
        z = master_data_dict["aliquot.csv"]["Data_Table"]
        z.sort_values("Submission_Index", axis=0, ascending=True, inplace=True)
        #z.drop_duplicates("Aliquot_ID", keep=keep_order, inplace=True)
        master_data_dict["aliquot.csv"]["Data_Table"] = correct_var_types(z, sql_column_df, "Aliquot")
    if "shipping_manifest.csv" in master_data_dict:
        z = master_data_dict["shipping_manifest.csv"]["Data_Table"]
        z['Current Label'] = z['Current Label'].fillna(value="0")
        master_data_dict["shipping_manifest.csv"]["Data_Table"] = z.query("`Current Label` not in ['0']")

        z = master_data_dict["shipping_manifest.csv"]["Data_Table"]["Current Label"].tolist()
        master_data_dict["shipping_manifest.csv"]["Data_Table"]["Current Label"] = zero_pad_ids(z)
        z = master_data_dict["shipping_manifest.csv"]["Data_Table"]["Volume"].tolist()
        z = [(i.replace("N/A", "0")) if isinstance(i, str) else i for i in z]
        master_data_dict["shipping_manifest.csv"]["Data_Table"]["Volume"] = z
        z = master_data_dict["shipping_manifest.csv"]["Data_Table"]
        z.sort_values("Submission_Index", axis=0, ascending=True, inplace=True)
        #z.drop_duplicates("Current Label", keep=keep_order, inplace=True)
        master_data_dict["shipping_manifest.csv"]["Data_Table"] = correct_var_types(z, sql_column_df, "Shipping_Manifest")
    return master_data_dict


def zero_pad_ids(data_list):
    try:
        digits = [len(i[14:16]) for i in data_list]
        for i in enumerate(digits):
            if i[1] == 1:
                if data_list[i[0]][14:15].isdigit():
                    data_list[i[0]] = data_list[i[0]][:15]
            elif i[1] == 2:
                if data_list[i[0]][14:16].isdigit():
                    data_list[i[0]] = data_list[i[0]][:16]
                elif data_list[i[0]][14:15].isdigit():
                    data_list[i[0]] = data_list[i[0]][:15]

        data_list = [i[0:13] + "_0" + i[14] if i[-2] == '_' else i for i in data_list]
    except Exception as e:
        print(e)
    finally:
        return data_list


def upload_assay_data(data_dict):
    #  populate the assay data tables which are independant from submissions

    assay_data, assay_target, all_qc_data, converion_file = get_box_data_v2.get_assay_data("CBC_Data")
    all_qc_data.replace("", "No Data Provided", inplace=True)
    all_qc_data.replace(np.nan, "No Data Provided", inplace=True)
    all_qc_data["Comments"] = all_qc_data["Comments"].replace("No Data Provided", "")

    assay_data["Calibration_Type"] = assay_data["Calibration_Type"].replace("", "No Data Provided")
    assay_data["Calibration_Type"] = assay_data["Calibration_Type"].replace("N/A", "No Data Provided")

    data_dict = add_assay_to_dict(data_dict, "assay_data.csv", assay_data)
    data_dict = add_assay_to_dict(data_dict, "assay_target.csv", assay_target)
    data_dict = add_assay_to_dict(data_dict, "assay_qc.csv", all_qc_data)
    data_dict = add_assay_to_dict(data_dict, "assay_conversion.csv", converion_file)

    validation_panel_assays = get_box_data_v2.get_assay_data("Validation")
    data_dict = add_assay_to_dict(data_dict, "validation_assay_data.csv", validation_panel_assays)
    return data_dict


def add_assay_to_dict(data_dict, csv_file, data_table):
    data_table.rename(columns={"Target_Organism": "Assay_Target_Organism"}, inplace=True)
    data_dict[csv_file] = {"Data_Table": []}
    data_dict[csv_file]["Data_Table"] = data_table
    return data_dict


def add_tables_to_database(engine, conn, sql_table_dict, sql_column_df, master_data_dict, tables_to_check, done_tables):
    not_done = []
    key_count = pd.crosstab(sql_column_df["Table_Name"], sql_column_df["Foreign_Key_Count"])
    key_count.reset_index(inplace=True)
    key_count = key_count.query("Table_Name not in @done_tables")
    key_count = key_count.query("Table_Name not in ['Biospecimen_Consumable','Biospecimen_Reagent','Biospecimen_Equipment']")
    if len(key_count) == 0:
        return

    for curr_table in key_count["Table_Name"].tolist():
        if curr_table in tables_to_check:
            check_foreign = key_count.query("Table_Name == @curr_table and 1.0 > 0")
            if len(check_foreign) > 0:
                foreign_table = sql_column_df.query("Table_Name == @curr_table and Foreign_Key_Table not in ['', 'None']")["Foreign_Key_Table"].tolist()
                check = all(item in done_tables for item in foreign_table)
                if check is False:
                    not_done.append(curr_table)
                    continue    # dependent tables not checked yet

        done_tables.append(curr_table)
        if curr_table not in tables_to_check:
            continue
        
        #if curr_table != 'Participant_Comorbidities':
        #    continue

        y = sql_column_df.query("Table_Name == @curr_table")
        y = y.query("Autoincrement != True")
        y = y.query("Column_Name not in ['Samples_Not_Useable']")
        
        sql_df = pd.read_sql((f"Select * from {curr_table}"), conn)
        sql_df = sql_df[y["Column_Name"].tolist()]
        # sql_df = sql_df.astype(str)
        sql_df.replace("No Data", "NULL", inplace=True)  # if no data in sql this is wrong, used to correct
        sql_df.fillna("N/A", inplace=True)  # replace NULL values from sql with "No Data" for merge purposes

        num_cols = sql_column_df.query(f"Table_Name == '{curr_table}' and Var_Type in ('INTEGER', 'INTERGER', 'FLOAT', 'DOUBLE')")["Column_Name"]
        num_cols = list(set(num_cols))
        char_cols = sql_column_df[sql_column_df['Var_Type'].str.contains("CHAR")]["Column_Name"]
        char_cols = list(set(char_cols))

        csv_file = [key for key, value in sql_table_dict.items() if curr_table in value]
        output_file = pd.DataFrame(columns=y["Column_Name"].tolist())
        if 'Sunday_Prior_To_First_Visit' in output_file.columns:
            output_file.drop("Sunday_Prior_To_First_Visit", inplace=True, axis=1)
        processing_file = []
        processing_data = []
        if len(csv_file) == 0:
            continue
        else:
            for curr_file in csv_file:
                #if curr_table not in ["Participant_Visit_Info", "Visit_One_Offset_Correction"]:
                #    continue
                

                if curr_file in master_data_dict:
                    x = copy.copy(master_data_dict[curr_file]["Data_Table"])
                    if "PCR_Test_Date_Duration_From_Index" in x.columns:
                        z = x[["PCR_Test_Date_Duration_From_Index", "Rapid_Antigen_Test_Date_Duration_From_Index", "Antibody_Test_Date_Duration_From_Index"]]
                        z = correct_var_types(z, sql_column_df, curr_table)
                        z = z.mean(axis=1, numeric_only=True)
                        x['Average_Duration_Of_Test'] = z.fillna("NAN")
                else:
                    continue
                x.rename(columns={"Comments": curr_table + "_Comments"}, inplace=True)
                if curr_table == "Unique_Tube_Info":
                    if curr_file == "aliquot.csv":
                        x["Tube_Used_For"] = "Aliquot"
                    elif curr_file == "biospecimen.csv":
                        x["Tube_Used_For"] = "Biospecimen_Collection"
                if curr_table == "Assay_Bio_Target":
                    x = get_bio_target(x, conn)
                try:
                    x.replace({np.nan: 'N/A', 'nan': "N/A", '': "N/A", True: '1', False: '0'}, inplace=True)
                    x = x.astype(str)
                    if curr_file in ["equipment.csv", "consumable.csv", "reagent.csv"]:
                        processing_data = copy.copy(x)
                    x = get_col_names(x, y, conn, curr_table, curr_file, sql_column_df)
                    if len(x) > 0:
                        x.drop_duplicates(inplace=True)
                except Exception as e:
                    print(e)
                output_file = pd.concat([output_file, x])
                if len(processing_data) > 0 and len(processing_file) > 0:
                    processing_file = pd.concat([processing_file, processing_data])
                elif len(processing_data) > 0:
                     processing_file = processing_data

            if len(output_file) > 0:
                if curr_table == 'Non_SARS_Covid_2_Vaccination_Status':
                    output_file = output_file.query("Vaccination_Record not in ['N/A', 'Not Reported']") #N/A means not asked, do not include
                if curr_table + "_Comments" in output_file.columns:
                    output_file[curr_table + "_Comments"].replace('nan', 'No Data', inplace=True)
                if "Visit_Info_Comments" in output_file.columns:
                    output_file["Visit_Info_Comments"].replace(np.nan, 'No Data', inplace=True)
                
            if len(output_file) > 0:
                output_file = correct_var_types(output_file, sql_column_df, curr_table)
                sql_df = fix_num_cols(sql_df, num_cols, "sql")
                output_file = fix_num_cols(output_file, num_cols, "file")
                output_file = fix_char_cols(output_file, char_cols)
                output_file.drop_duplicates(inplace=True)

                output_file.reset_index(inplace=True, drop=True)
                output_file = fix_aliquot_ids(output_file, "first", sql_column_df)
                #output_file.replace("N/A", "No Data", inplace=True)
                #output_file.replace("nan", "No Data", inplace=True)
                
                if "Biospecimens_Collected" in output_file.columns:
                    output_file.replace('nan', 'No Specimens Collected', inplace=True)

                comment_col = [i for i in output_file.columns if "Comments" in i]
                if len(comment_col) > 0:
                    
                    output_file[comment_col[0]] = output_file[comment_col[0]].replace("No Data", "")
                    output_file[comment_col[0]] = output_file[comment_col[0]].replace("N/A", "")
                    try:
                        sql_df[comment_col[0]] = sql_df[comment_col[0]].replace("No Data", "")
                        sql_df[comment_col[0]] = sql_df[comment_col[0]].replace("N/A", "")
                    except Exception:
                        print(f"{comment_col} not in database")

                if curr_table not in ["Unique_Tube_Info", "Aliquot", "Unique_Consumable", "Unique_Reagent", "Unique_Equipment"]:
                    col_list = [i for i in sql_df.columns if i not in ["Derived_Result", "Raw_Result", "BMI"]]
                    col_list = [i for i in col_list if i.find("ICD10") == -1 and i.find("ICD_10") == -1]
                    
                    sql_df[col_list] = sql_df[col_list].replace("\.0", "", regex=True)

                    col_list = [i for i in output_file.columns if i not in ["Derived_Result", "Raw_Result", "BMI", 'Sample_Dilution']]
                    output_file[col_list] = output_file[col_list].replace("\.0", "", regex=True)

                number_col = sql_column_df.query("Table_Name == @curr_table and Var_Type in ['FLOAT', 'INTEGER', 'DOUBLE']")["Column_Name"].tolist()
                for n_col in number_col:
                    if n_col in output_file.columns:
                        output_file[n_col].replace("", -1e9, inplace=True)
                        output_file[n_col].replace("N/A", -1e9, inplace=True)
                for cell_col in sql_df:
                    if "Hemocytometer_Count" in cell_col or "Automated_Count" in cell_col:
                        sql_df[cell_col].replace("No Data", -1e9, inplace=True)

                try:
                    primary_keys = sql_column_df.query("Table_Name == @curr_table and Primary_Key == 'True' and Autoincrement != True")
                    if len(primary_keys) > 0:
                        primary_keys = primary_keys["Column_Name"].tolist()
                        for i in primary_keys:
                            output_file[i] = output_file[i].replace("No Data", "")   # primary keys cant be null


                    if curr_table + "_Comments" in primary_keys:
                        primary_keys.remove(curr_table + "_Comments")
                    #sql_df.replace("", "No Data", inplace=True)
                    #output_file.replace({"": "No Data"}, inplace=True)
                    sql_df = sql_df.replace(np.nan, -1e9)
                    output_file = output_file.replace(np.nan, -1e9)
                    
                    if "Primary_Study_Cohort" in output_file.columns:
                        output_file.drop("Primary_Study_Cohort", axis=1, inplace=True)
                    if 'Data_Release_Version' in output_file.columns:
                        output_file.drop('Data_Release_Version', axis=1, inplace=True)

                    output_file.replace('Lymphocytes', 'PBMC', inplace=True)
                    output_file.replace('Gist', 'GIST', inplace=True)
                    
                    if curr_table == "Biospecimen":
                        output_file = output_file.sort_values(["Biospecimen_ID", "Biospecimen_Collection_Date_Duration_From_Index"])
                        output_file.drop_duplicates(["Visit_Info_ID", "Biospecimen_ID"], keep="last", inplace=True)
                    
                    if curr_table == "Covid_Vaccination_Status":
                        output_file = cleanup_Covid_hist(output_file, conn)
                    #if curr_table == "Comorbidities_Names":
                    #    output_file = cleanup_Comorbidities(output_file, conn)

                    #if curr_table == "Drugs_And_Alcohol_Use":
                    #    output_file = cleanup_Drugs(output_file)


                    #if "Sunday_Prior_To_First_Visit" in output_file.columns:
                    #    output_file["Sunday_Prior_To_First_Visit"] = output_file["Sunday_Prior_To_First_Visit"].replace(-1e9, datetime.date(2000,1,1))
                    if "Sunday_Prior_To_First_Visit" in sql_df.columns:
                        sql_df.drop("Sunday_Prior_To_First_Visit", axis=1, inplace=True)
                        # sql_df["Sunday_Prior_To_First_Visit"] = sql_df["Sunday_Prior_To_First_Visit"].replace("No Data", datetime.date(2000,1,1))
                    
                    #if 'Submission_Index' not in primary_keys and 'Submission_Index' in sql_df.columns:
                    #    sql_df.drop('Submission_Index', axis=1, inplace=True)
                    
                    #if curr_table + "_Expiration_Date" in sql_df.columns:
                    #    sql_df[curr_table + "_Expiration_Date"].replace("No Data", "N/A", inplace=True)
                    if len([i for i in output_file.columns if "Expiration_Date" in i]) > 0:
                        output_file[[i for i in output_file.columns if "Expiration_Date" in i][0]].replace(-1e9, "0000-00-00", inplace=True)
                        output_file[[i for i in output_file.columns if "Expiration_Date" in i][0]] = output_file[[i for i in output_file.columns if "Expiration_Date" in i][0]].astype(str)
                    if len([i for i in sql_df.columns if "Expiration_Date" in i]) > 0:
                        sql_df[[i for i in sql_df.columns if "Expiration_Date" in i][0]] = sql_df[[i for i in sql_df.columns if "Expiration_Date" in i][0]].astype(str) 


                    
                    if curr_table == "Treatment_History":
                        output_file["Treat_lower"] = output_file["Treatment"].str.lower()
                        output_file.drop_duplicates(['Visit_Info_ID', 'Health_Condition_Or_Disease', 'Treat_lower'], inplace=True, keep="first")
                        output_file.drop("Treat_lower", axis=1, inplace=True)
                    try:
                        if "Derived_Result" in output_file.columns:
                            output_file["Derived_Result"].replace("No Data", -1e+09, inplace=True)
                            output_file["Derived_Result"] = output_file["Derived_Result"].astype(str)
                        if "Sample_Dilution" in output_file.columns:
                            output_file["Sample_Dilution"] = output_file["Sample_Dilution"].astype(str)
                        if "Biospecimens_Collected" in output_file.columns:
                            output_file["Biospecimens_Collected"].replace("PBMC|serum", "PBMC|Serum", inplace=True)
                            output_file["Biospecimens_Collected"].replace("serum", "Serum", inplace=True)
                        if "Biospecimens_Collected" in sql_df.columns:
                            sql_df["Biospecimens_Collected"].replace("PBMC|serum", "PBMC|Serum", inplace=True)
                            sql_df["Biospecimens_Collected"].replace("serum", "Serum", inplace=True)
                        #if "Equipment_Calibration_Due_Date" in output_file.columns:
                        #    output_file["Equipment_Calibration_Due_Date"].replace("N/A", "No Data", inplace=True)
                        
                        if "Samples_Not_Useable" in sql_df.columns:
                            sql_df.drop("Samples_Not_Useable", axis=1, inplace=True)
                        
                        output_file = output_file.replace('27_403194-400_02', '27_403194_400_02')
                        #if "Biospecimen_Tube_ID" in sql_df.columns:
                        #    output_file = output_file.merge(sql_df[["Biospecimen_ID","Biospecimen_Tube_ID"]])
                        #if "Aliquot_Tube_ID" in sql_df.columns:
                        #    output_file = output_file.merge(sql_df[["Aliquot_ID","Aliquot_Tube_ID"]])


                        #z = output_file[output_file.columns].applymap(lambda x: str(x).lower()).merge(sql_df[sql_df.columns].applymap(lambda x: str(x).lower()), how="outer", indicator=True)
                        table_ids = sql_column_df.query("Table_Name == @curr_table")["Column_Name"].tolist()
                        output_file =  output_file[[i for i in output_file.columns if i in table_ids]]
                        z = output_file.merge(sql_df, how="outer", indicator=True)
                        z.drop_duplicates(inplace=True)
                        
                    except Exception:
                        sql_df["Sample_Dilution"] = sql_df["Sample_Dilution"].astype(str)
                        #z = output_file[output_file.columns].applymap(lambda x: str(x).lower()).merge(sql_df[sql_df.columns].applymap(lambda x: str(x).lower()), how="outer", indicator=True)
                        z = output_file.merge(sql_df, how="outer", indicator=True)
                    finally:
                        if curr_file not in ["equipment.csv", "consumable.csv", "reagent.csv"]:
                            z = z[[not i for i in z[[i for i in output_file.columns if (i in char_cols and i not in ['Samples_Not_Useable'])]].applymap(lambda x: x.lower()).duplicated(keep=False)]]
                        new_data = z.query("_merge == 'left_only'")       # new or update data
                except Exception as e:
                    print(e)
                    new_data = []
                    
                    
                if "Primary_Study_Cohort" in new_data:
                    new_data.drop("Primary_Study_Cohort", axis=1, inplace=True)
                    new_data = new_data.merge(sql_df[["Visit_Info_ID","Primary_Study_Cohort"]], how="left")

                if len(new_data) > 0:
                    new_data.drop("_merge", inplace=True, axis=1)
                    x = sql_column_df.query("Var_Type in ['INTEGER', 'FLOAT']")
                    x = x.query("Column_Name in @new_data.columns")
                    update_data = []

                    try:
                        merge_data = new_data.merge(sql_df[primary_keys], how="left", indicator=True)
                        merge_data.replace({-1e9: np.nan}, inplace=True)
                        new_data = merge_data.query("_merge == 'left_only'")
                        update_data = merge_data.query("_merge == 'both'")
                    except Exception as e:
                        print(e)

                    if len(new_data) > 0:
                        if "_merge" in new_data.columns:
                            new_data.drop("_merge", inplace=True, axis=1)
                        try:
                            if len(new_data) > 0:
                                new_data.replace("No Data", np.nan, inplace=True)
                                new_data.replace(datetime.date(2000,1,1), np.nan, inplace=True)
                                
                                if len(primary_keys) > 0:
                                    new_data = new_data[[not i for i in pd.concat([new_data[[i for i in primary_keys if i not in char_cols]],
                                                                                   new_data[[i for i in primary_keys if i in char_cols]].applymap(lambda x: x.lower())], axis=1).duplicated(keep="last")]]

                                #if "Current Label" in new_data.columns:
                                    #new_data = new_data.query("`Current Label` not in ['32_441013_102_02', '32_441006_311_01', '32_441131_101_02', " +
                                    #                          "'32_441040_102_02', '32_441040_102_01', '32_441057_102_02', '32_441057_102_01', '32_441047_102_02', " +
                                    #                          "'32_441047_102_01', '32_441040_101_02', '32_441040_101_01', '32_441041_101_01', '32_441041_101_02', " +
                                    #                          "'32_441047_101_01', '32_441047_101_02', '32_441057_101_01','32_441057_101_02']")
                                    #
                                    #new_data = new_data.query("`Current Label` not in ['32_441083_311_01','32_441153_311_01','32_441112_304_01','32_441095_305_01','32_441108_301_01', " +
                                    #                          "'32_441139_305_01','32_441013_311_01','32_441146_304_01','32_441165_304_01','32_441131_101_01']")


                                #new_data = new_data[sql_column_df.query("Table_Name == @curr_table")["Column_Name"].tolist()]
                                if "Treatment_History" in curr_table:
                                    new_data = new_data.drop_duplicates(['Visit_Info_ID', 'Health_Condition_Or_Disease', 'Treatment', 'Dosage', 'Dosage_Units'])
                                if "Assay_Target_Organism" in new_data.columns:
                                    new_data = new_data.query("Assay_Target_Organism == Assay_Target_Organism")
                                #new_data = new_data.query("Assay_ID in ['32_888','32_999']")
                                
                                
                                if "Biospecimen_ID" in new_data.columns and "Visit_Info_ID" in new_data.columns:
                                    new_data.sort_values(["Biospecimen_ID", "Visit_Info_ID"], inplace=True)
                                    new_data.drop_duplicates("Biospecimen_ID", keep="last", inplace=True)
                                
                                if "Aliquot_ID" in new_data.columns:
                                    new_data = new_data.query("Aliquot_ID not in ['14_M27007_101_01','27_200089_300_01','27_200089_300_02','27_200089_300_03', " +
                                                              "'27_200089_306_01','27_200089_306_02','27_200089_306_03','27_200089_400_01','27_200089_400_02','27_200089_406_01','27_200089_406_02', " +
                                                              "'27_200106_301_01','27_200106_301_02','27_200106_301_03','27_200106_401_01','27_200106_401_02','27_200168_301_01','27_200168_301_02', " +
                                                              "'27_200168_301_03','27_200168_401_01','27_200168_401_02','27_200537_301_01','27_200537_301_02','27_200537_301_03','27_200537_401_01','27_200537_401_02']")

                                if curr_table == "Cancer_Cohort":
                                    new_data = new_data.query("Visit_Info_ID != '27_200318 : B01'")
                                    
                                if len(new_data) > 0:
                                    print(f"\n## Adding New Rows {len(new_data)} to table: {curr_table} ##\n")
                                    new_data.to_sql(name=curr_table, con=engine, if_exists="append", index=False)
                                    conn.connection.commit()
                        except Exception as e:
                            display_error_line(e)
                            print(f"error loading table {curr_table}")
                    if len(update_data) > 0:
                        row_count = len(update_data)
                        #update_data = update_data[sql_column_df.query("Table_Name == @curr_table")["Column_Name"].tolist()]
                        print(f"\n## Updating {row_count} Rows in table: {curr_table} ##\n")
                        if "Assay_Target_Organism" in update_data.columns:
                            update_data = update_data.query("Assay_Target_Organism == Assay_Target_Organism")
                        update_tables(conn, engine, primary_keys, update_data, curr_table)
                else:
                    print(f" \n## {curr_table} has been checked, no data to add")
            else:
                print(f" \n## {curr_table} was not found in submission, nothing to add")
            if len(processing_file) > 0 and "Biospecimen" in done_tables:
                prim_table = pd.read_sql((f"Select * from {curr_table}"), conn)
                prim_table.fillna("No Data", inplace=True)  # replace NULL values from sql with "No Data" for merge purposes
                prim_table.replace("N/A", "No Data", inplace=True)
                processing_file.replace("N/A", "No Data", inplace=True)
                curr_table = curr_table.replace("Unique_","")
                
                prim_table = prim_table[[i for i in prim_table.columns if "Comments" not in i]]
                date_col = [i for i in prim_table.columns if "Date" in i]
                try:
                    prim_table[date_col[0]] = prim_table[date_col[0]].apply(lambda x: str(x))
                except Exception as e:
                    print(e)
                
                df_obj = processing_file.select_dtypes(['object'])
                processing_file[df_obj.columns] = df_obj.apply(lambda x: x.str.strip())
                processing_file = correct_var_types(processing_file, sql_column_df, curr_table)
                processing_data = processing_data.merge(prim_table, indicator=True, how="left")
                #processing_data = processing_data.query("_merge not in ['both']")
                test_table = processing_data[["Biospecimen_ID", curr_table + "_Index"]]
                print(f"working on Biospecimen_{curr_table}")
                
                sql_table = pd.read_sql((f"SELECT Biospecimen_ID, {curr_table}_Index FROM Biospecimen_{curr_table}"), conn)
                test_table = test_table.merge(sql_table, how="left", indicator=True)
                test_table = test_table.query("_merge == 'left_only'").drop("_merge", axis=1)
                test_table.drop_duplicates(inplace=True)
                
                bio_table = pd.read_sql(("SELECT Biospecimen_ID, Biospecimen_Type FROM Biospecimen"), conn)
                test_table = test_table.merge(bio_table["Biospecimen_ID"])
                
                print(f"\n##Adding {len(test_table)} New Rows to table: Biospecimen_{curr_table} ##\n")
                test_table.to_sql(name="Biospecimen_" + curr_table, con=engine, if_exists="append", index=False)
                
                #for curr_row in test_table.index:
                #    bio_id= test_table.loc[curr_row]["Biospecimen_ID"]
                #    index_id= test_table.loc[curr_row]["Consumable_Index"]
                #    sql_str = (f"INSERT INTO `Biospecimen_{curr_table}` (Biospecimen_ID, {curr_table}_Index) VALUES ('{bio_id}', '{index_id}')")
                #    try:
                #        engine.execute(sql_str)
                #            #curr_row.to_sql(name="Biospecimen_" + curr_table, con=engine, if_exists="append", index=False)
                #        conn.connection.commit()
                #    except Exception:
                #        print(f"{bio_id} has an issue that needs to be addressed")

    if len(not_done) > 0:
        add_tables_to_database(engine, conn, sql_table_dict, sql_column_df, master_data_dict, tables_to_check, done_tables)

def fix_num_cols(df, num_cols, data_type):
    for col_name in df.columns:
        if col_name in num_cols:
            if data_type == "file":
                # df[col_name] = [str(i) for i in df[col_name]]
                df[col_name] = df[col_name].replace("N/A", np.nan)
                df[col_name] = df[col_name].replace("Not Reported", np.nan)
                df[col_name] = df[col_name].replace(-1000000000, np.nan)
                df[col_name] = df[col_name].replace(-1e+09, np.nan)

            df[col_name] = df[col_name].replace("No Data", np.nan)
            df[col_name] = df[col_name].replace("nan", np.nan)
            try:
                df[col_name].replace("N/A", np.nan, inplace=True)
                df[col_name].fillna(np.nan, inplace=True)
                if 'INTEGER' in num_cols or 'INTERGER' in num_cols:
                    df[col_name] = [int(i) for i in df[col_name]]
                else:
                    df[col_name] = [round(float(i), 4)  for i in df[col_name]]
            except Exception as e:
                print(e)
    return df


def fix_char_cols(df, char_cols):
    for col_name in df.columns:
        if col_name in char_cols:
            df[col_name] = [str(i).strip() for i in df[col_name]]  # remove trail white space
            df[col_name] = [str(i).replace("'", "") for i in df[col_name]]  # remove trail white space
            df[col_name] = [str(i).replace("", "No Data") if len(i) == 0 else i for i in df[col_name]]  # fill blank cells with no data
    return df


def cleanup_Covid_hist(output_file, conn):
    part_list = list(set(output_file["Research_Participant_ID"]))
    for curr_part in part_list:

        x = output_file.query("Research_Participant_ID == @curr_part and Vaccination_Status not in ['Unvaccinated','No vaccination event reported']")
        x["Vaccination_Status_New"] = [i.replace(":Bivalent","") for i in x["Vaccination_Status"]]
        x["Vaccination_Status_New"] = [i.replace(":Monovalent XBB.1.5","") for i in x["Vaccination_Status_New"]]
        x = x.query("Covid_Vaccination_Status_Comments not in ['Record previously submitted in error.', 'Vaccine Status does not exist, input error']")

        dups =x[x.duplicated(["Vaccination_Status_New"], keep='last')]
        if len(dups) > 0:
            #dups['Covid_Vaccination_Status_Comments'] = 'Record previously submitted in error.'
            output_file = output_file.merge(dups, how="left", indicator = True)
            z = output_file.query("_merge in ['both']")
            output_file.drop(["_merge", "Vaccination_Status_New"], axis=1, inplace=True)
            output_file['Covid_Vaccination_Status_Comments'][z.index] = 'Record previously submitted: Duplicated.'

    return output_file

def cleanup_Comorbidities(output_file, conn):
    status_file = pd.read_sql(("SELECT * FROM `seronetdb-Vaccine_Response`.Participant_Comorbidities;"), conn)

    output_file= output_file.merge(status_file)
    for curr_col in status_file.columns:
        if curr_col == 'Visit_Info_ID':
            pass
        else:
            x = output_file.query(f"{curr_col} in ['Not Reported', 'Unknown']")
            try:
                output_file[f'{curr_col}_Description_Or_ICD10_codes'][x.index] = "No Reported Status"
            except:
                output_file[f'{curr_col}_ICD10_codes_Or_Agents'][x.index] = "No Reported Status"
                
            if f'{curr_col}_Description_Or_ICD10_codes' in output_file.columns:
                x = output_file.query(f"{curr_col}_Description_Or_ICD10_codes in ['Not Reported', 'Unknown']")
                output_file[f'{curr_col}'][x.index] = "Condition not described"

            output_file.drop(curr_col, axis=1, inplace=True)
    
    return output_file

def cleanup_Drugs(output_file):
    x = output_file.query("Drug_Use in ['Not Reported']")
    output_file["Drug_Type"][x.index] = "Drug Type Not Reported"
        
    x = output_file.query("Drug_Use not in ['Never user','Current non-user'] and Drug_Type in ['Unknown', 'N/A','Drug Not Disclosed']")
    output_file["Drug_Type"][x.index] = "Drug Type Not Reported"
    return output_file


def get_col_names(x, y, conn, curr_table, curr_file, sql_column_df):
    col_list = y["Column_Name"].tolist()
    if "Normalized_Cohort" in col_list:   # this will be removed once tempaltes updated
        col_list.remove("Normalized_Cohort")
    if "Visit_Info_Comments" in col_list:   # this will be removed once tempaltes updated
        col_list.remove("Visit_Info_Comments")
    if "Data_Release_Version" in col_list and curr_file not in ["baseline.csv"]:
        col_list.remove("Data_Release_Version")

    #if curr_table == "Participant":
    #    if "Sunday_Prior_To_First_Visit" not in x.columns:
    #        x["Sunday_Prior_To_First_Visit"] =  datetime.date(2000,1,1)

    if curr_table == "Participant_Visit_Info":
        if "Primary_Study_Cohort" not in x.columns:
            x["Primary_Study_Cohort"] = "None"
        if "CBC_Classification" not in x.columns:
            x.rename(columns={"Cohort": "CBC_Classification"}, inplace=True)
        x, primary_key = add_visit_info(x, curr_file, [])
    else:
        if "Visit_Info_ID" in col_list:
            visit_data = pd.read_sql(("SELECT Visit_Info_ID, Research_Participant_ID, Visit_Number FROM Participant_Visit_Info;"), conn)
            list_of_visits = list(range(1,20)) + [str(i) for i in list(range(1,20))]
            list_of_visits = list_of_visits + [-1, '-1']
            visit_data["Visit_Number"] = [int(i) if i in list_of_visits else i for i in visit_data["Visit_Number"]]
            
            if curr_file == "baseline.csv":
                #x["Visit_Number"] = -1
                x["Visit_Number"] = 1
            x.replace("Baseline(1)", 1, inplace=True)
            try:
                x["Visit_Number"] = [int(i) if i in list_of_visits else i for i in x["Visit_Number"]]
                x = x.merge(visit_data)
            except Exception as e:
                print(e)
    if "Biospecimen_" in curr_table and "Test_Results" not in curr_table:
        table_name = curr_table.replace("Biospecimen_", "Unique_")
        table_data = pd.read_sql((f"SELECT * FROM {table_name}"), conn)
        x = x.merge(table_data)

    if curr_table in ["Biospecimen", "Aliquot"]:
        tube_data = pd.read_sql(("SELECT * FROM Unique_Tube_Info;"), conn)
        tube_data.fillna("N/A", inplace=True)
        col_list = tube_data.columns.tolist()
        col_list.remove("Tube_ID")
        tube_data = tube_data.drop_duplicates(col_list, keep="first")
        
        tube_data["Tube_Type_Expiration_Date"] = [str(i) for i in tube_data["Tube_Type_Expiration_Date"]]
        if curr_table == "Biospecimen":
            tube_data.columns = [i.replace("Tube", "Collection_Tube") for i in tube_data.columns]
            x["Collection_Tube_Type_Expiration_Date"] = [i if i == "N/A" else parse(i).date()
                                                         for i in x["Collection_Tube_Type_Expiration_Date"].tolist()]
        elif curr_table == "Aliquot":
            tube_data.columns = [i.replace("Tube", "Aliquot_Tube") for i in tube_data.columns]
            x["Aliquot_Tube_Type_Expiration_Date"] = [i if i == "N/A" else str(parse(i).date()) for i in x["Aliquot_Tube_Type_Expiration_Date"].tolist()]
        try:
            #tube_data.replace("N/A", "No Data", inplace=True)
            #x.replace("N/A", "No Data", inplace=True)
            #x.fillna("No Data", inplace=True)
            
            if 'Visit_Number' in x.columns:
                x['Visit_Number'] = [str(i) for i in x['Visit_Number']]
            for curr_col in x.columns:
                if curr_col in ['Submission_Index', "Collection_Tube_Type_Expiration_Date", "Aliquot_Tube_Type_Expiration_Date"]:
                    continue
                try:
                    x[curr_col] = x[curr_col].str.strip()
                except AttributeError:
                    pass  # not a character col
            x = x.merge(tube_data, how="left", indicator=True)
        except Exception as e:
            print(e)

        if curr_table == "Biospecimen":
            x.rename(columns={"Collection_Tube_ID": "Biospecimen_Tube_ID"}, inplace=True)
        else:
            x.rename(columns={"Collection_Tube_ID": "Aliquot_Tube_ID"}, inplace=True)
    try:
        if "Submission_CBC" in col_list and "Submission_CBC" not in x.columns:
            if "Research_Participant_ID" in x.columns:
                x['Submission_CBC'] = [str(i[:2]) for i in x["Research_Participant_ID"]]
            else:
                x['Submission_CBC'] = [str(i[:2]) for i in x["Biospecimen_ID"]]
        if curr_table == "Unique_Tube_Info":
            x.columns = [i.replace("Biospecimen_Tube", "Tube") for i in x.columns]
            x.columns = [i.replace("Aliquot_Tube", "Tube") for i in x.columns]

            x = x[[i for i in col_list if i in x.columns]]
        if "_merge" in x.columns:
            x.drop("_merge", axis=1, inplace=True)
        #x = x[col_list]
    except Exception as e:
        display_error_line(e)
        return []
    return x


def updated_derived(x):
    x["Derived_Result"] = [str(i).replace("<", "") for i in x["Derived_Result"]]
    x["Derived_Result"] = [str(i).replace(">", "") for i in x["Derived_Result"]]
    x["Derived_Result"] = [str(i).replace("Nonreactive", "-1e9") for i in x["Derived_Result"]]
    x["Derived_Result"] = [str(i).replace("Reactive", "-1e9") for i in x["Derived_Result"]]
    return x

def update_secondary_confirm(master_data_dict, sql_column_df):
    x = master_data_dict["secondary_confirmation_test_result.csv"]["Data_Table"]
    x = correct_var_types(x, sql_column_df, "Secondary_Confirmatory_Test")

    if "Subaliquot_ID" in x.columns:
        x["BSI_Parent_ID"] = [i[:7] + " 0001" for i in x["Subaliquot_ID"].tolist()]
    x.rename(columns={"Comments": "Confirmatory_Clinical_Test_Comments"}, inplace=True)
    master_data_dict["secondary_confirmation_test_result.csv"]["Data_Table"] = x
    return master_data_dict


def get_bio_target(curr_table, conn):
    curr_table.drop_duplicates(inplace=True)
    curr_cols = curr_table.columns.tolist()
    curr_cols = [i.replace("Target_biospecimen_is_", "") for i in curr_cols]
    curr_cols = [i.replace("/", "_") for i in curr_cols]
    curr_table.columns = curr_cols

    bio_type = pd.read_sql("Select * FROM Biospecimen_Type", conn)
    new_col_names = [i for i in bio_type["Biospecimen_Type"].tolist() if i in curr_table.columns]
    bio_table = curr_table[new_col_names].stack().to_frame()
    bio_table.reset_index(inplace=True)
    bio_table.columns = ["Assay_ID", "Target_Biospecimen_Type", "Is_Present"]
    try:
        bio_table["Assay_ID"] = curr_table["Assay_ID"].repeat(len(new_col_names)).tolist()
        bio_table = bio_table.query("Is_Present == 'Yes'")
        curr_table = curr_table.merge(bio_table)
    except Exception as e:
        print(e)
    return curr_table


def get_bsi_files(s3_client, bucket, sub_folder, master_data_dict):
    if sub_folder == "Reference Pannel Submissions":
        curr_file = "Serology_Data_Files/biorepository_id_map/Biorepository_ID_Reference_Panel_map.xlsx"
    elif sub_folder == "Vaccine Response Submissions":
        curr_file = "Serology_Data_Files/biorepository_id_map/Biorepository_ID_Vaccine_Response_map.xlsx"
    else:
        return master_data_dict

    print(" getting bsi parent data ")
    parent_data = pd_s3.get_df_from_keys(s3_client, bucket, curr_file, suffix="xlsx", format="xlsx", na_filter=False,
                                         output_type="pandas", sheet_name="BSI_Parent_Aliquots")
    print(" gettting bsi child data ")
    child_data = pd_s3.get_df_from_keys(s3_client, bucket, curr_file, suffix="xlsx", format="xlsx", na_filter=False,
                                        output_type="pandas", sheet_name="BSI_Child_Aliquots")

    master_data_dict["bsi_parent.csv"] = {"Data_Table": parent_data}
    master_data_dict["bsi_child.csv"] = {"Data_Table": child_data}
    return master_data_dict


def update_tables(conn, engine, primary_keys, update_table, sql_table):
    key_str = ['`' + str(s) + '`' + " like '%s'" for s in primary_keys]
    key_str = " and ".join(key_str)
    if "Sunday_Prior_To_First_Visit" in update_table:
        update_table = update_table[["Research_Participant_ID", "Age", "Sunday_Prior_To_First_Visit"]]
    else:
        update_table.drop("_merge", inplace=True, axis=1)
    
    #if 'Cancer_Description_Or_ICD10_codes' in update_table.columns:
    #    update_table = update_table[['Visit_Info_ID','Cancer_Description_Or_ICD10_codes']]

    if "Primary_Study_Cohort" in update_table.columns:
        update_table = update_table.drop("Primary_Study_Cohort", axis=1)

    col_list = update_table.columns.tolist()
    col_list = [i for i in col_list if i not in primary_keys]

    if "Vaccination_Record" in update_table.columns:
        col_list.append("Vaccination_Record")

    for index in update_table.index:
        try:
            curr_data = update_table.loc[index, col_list].values.tolist()
            curr_data = [str(i).replace("'", "") for i in curr_data]
            curr_data = [i.replace('', "NULL") if len(i) == 0 else i for i in curr_data]

            primary_value = update_table.loc[index, primary_keys].values.tolist()
            primary_value = [str(i).replace(".0", "") for i in primary_value]
            update_str = ["`" + i + "` = '" + str(j) + "'" for i, j in zip(col_list, curr_data)]
            update_str = ', '.join(update_str)

            update_str = update_str.replace("'nan'", "NULL")
            update_str = update_str.replace("'NULL'", "NULL")
            update_str = update_str.replace("'No Data'", "NULL")
            update_str = update_str.replace("'2000-01-01'", "NULL")
            update_str = update_str.replace("`Data_Release_Version` = NULL", "`Data_Release_Version` = '3.0.0'") # outdated, replace later
            update_str = update_str.replace("`Data_Release_Version` = '2'", "`Data_Release_Version` = '3.0.0'")

            sql_query = (f"UPDATE {sql_table} set {update_str} where {key_str %tuple(primary_value)}")
            engine.execute(sql_query)
        except Exception as e:
            print(e)
        finally:
            conn.connection.commit()


def get_sql_dict_ref():
    sql_table_dict = {}
    sql_table_dict["assay_data.csv"] = ["Assay_Metadata", "Assay_Calibration", "Assay_Bio_Target"]
    sql_table_dict["assay_target.csv"] = ["Assay_Target"]
    sql_table_dict["assay_qc.csv"] = ["Assay_Quality_Controls"]
    sql_table_dict["assay_conversion.csv"] = ["Assay_Organism_Conversion"]
    sql_table_dict["validation_assay_data.csv"] = ["Validation_Panel_Assays"]

    sql_table_dict["aliquot.csv"] = ["Unique_Tube_Info", "Aliquot"]
    sql_table_dict["biospecimen.csv"] = ["Unique_Tube_Info", "Biospecimen"]

    sql_table_dict["confirmatory_clinical_test.csv"] = ["Confirmatory_Clinical_Test"]
    sql_table_dict["demographic.csv"] = ["Participant"]  # , "Prior_Covid_Outcome", "Participant_Comorbidity_Reported", "Comorbidity"]
    sql_table_dict["prior_clinical_test.csv"] = ["Participant_Prior_SARS_CoV2_PCR"]  # , "Participant_Prior_Infection_Reported"]

    sql_table_dict["consumable.csv"] = ["Consumable", "Biospecimen_Consumable"]
    sql_table_dict["reagent.csv"] = ["Reagent", "Biospecimen_Reagent"]
    sql_table_dict["equipment.csv"] = ["Equipment", "Biospecimen_Equipment"]

    sql_table_dict["secondary_confirmation_test_result.csv"] = ["Secondary_Confirmatory_Test"]
    sql_table_dict["bsi_child.csv"] = ["BSI_Child_Aliquots"]
    sql_table_dict["bsi_parent.csv"] = ["BSI_Parent_Aliquots"]
    sql_table_dict["submission.csv"] = ["Submission"]
    sql_table_dict["shipping_manifest.csv"] = ["Shipping_Manifest"]
    sql_table_dict["CDC_Data.csv"] = ["CDC_Confrimation_Results"]
    
    sql_table_dict["Blinded_Evaluation_Panels.csv"] = ["Blinded_Validation_Test_Results"]
    return sql_table_dict


def get_sql_dict_vacc():
    sql_table_dict = {}
    visit_list = ["Participant_Visit_Info", "Participant",  "Participant_Comorbidities", "Comorbidities_Names",
                  "Participant_Other_Conditions", "Participant_Other_Condition_Names", "Drugs_And_Alcohol_Use", "Non_SARS_Covid_2_Vaccination_Status"]

    #visit_list = ["Participant_Visit_Info", "Participant", "Specimens_Collected", "Participant_Comorbidities", "Comorbidities_Names",
    #              "Participant_Other_Conditions", "Participant_Other_Condition_Names", "Drugs_And_Alcohol_Use", "Non_SARS_Covid_2_Vaccination_Status"]

    sql_table_dict["baseline.csv"] = visit_list
    sql_table_dict["follow_up.csv"] = [i for i in visit_list if i != "Participant"]  # Participant table does not exist in follow up

    sql_table_dict["aliquot.csv"] = ["Tube", "Aliquot"]
    sql_table_dict["biospecimen.csv"] = ["Tube", "Biospecimen"]

    sql_table_dict["assay_data.csv"] = ["Assay_Metadata", "Assay_Calibration", "Assay_Bio_Target"]
    sql_table_dict["assay_target.csv"] = ["Assay_Target"]
    sql_table_dict["assay_qc.csv"] = ["Assay_Quality_Controls"]
    sql_table_dict["assay_conversion.csv"] = ["Assay_Organism_Conversion"]

    sql_table_dict["study_design.csv"] = ["Study_Design"]

    sql_table_dict["consumable.csv"] = ["Consumable", "Biospecimen_Consumable"]
    sql_table_dict["reagent.csv"] = ["Reagent", "Biospecimen_Reagent"]
    sql_table_dict["equipment.csv"] = ["Equipment", "Biospecimen_Equipment"]

    sql_table_dict["shipping_manifest.csv"] = ["Shipping_Manifest"]

    sql_table_dict["covid_history.csv"] = ["Covid_History"]
    sql_table_dict["covid_hist_sql.csv"] = ["Covid_History"]

    sql_table_dict["covid_vaccination_status.csv"] = ["Covid_Vaccination_Status"]
    sql_table_dict["vac_status_sql.csv"] = ["Covid_Vaccination_Status"]

    sql_table_dict["biospecimen_test_result.csv"] = ["Biospecimen_Test_Results"]
    sql_table_dict["test_results_sql.csv"] = ["Biospecimen_Test_Results"]

    sql_table_dict["treatment_history.csv"] = ["Treatment_History"]

    sql_table_dict["autoimmune_cohort.csv"] = ["AutoImmune_Cohort"]
    sql_table_dict["cancer_cohort.csv"] = ["Cancer_Cohort"]
    sql_table_dict["cancer_cohort_prov.csv"] = ["Cancer_Cohort"]
    sql_table_dict["hiv_cohort.csv"] = ["HIV_Cohort"]
    sql_table_dict["organ_transplant_cohort.csv"] = ["Organ_Transplant_Cohort"]

    sql_table_dict["visit_info_sql.csv"] = ["Participant_Visit_Info"]
    sql_table_dict["submission.csv"] = ["Submission"]

    sql_table_dict["bsi_child.csv"] = ["BSI_Child_Aliquots"]
    sql_table_dict["bsi_parent.csv"] = ["BSI_Parent_Aliquots"]

    return sql_table_dict


def get_sql_dict_vacc_v2():
    sql_table_dict = {}
    visit_list = ["Participant_Visit_Info", "Participant",  "Participant_Comorbidities_Status", "Participant_Comorbidities_Names",
                  "Participant_Other_Condition_Status", "Participant_Other_Condition_Names", "Drugs_And_Alcohol_Use", "Non_SARS_Covid_2_Vaccination_Status"]

    #visit_list = ["Participant_Visit_Info", "Participant", "Specimens_Collected", "Participant_Comorbidities", "Comorbidities_Names",
    #              "Participant_Other_Conditions", "Participant_Other_Condition_Names", "Drugs_And_Alcohol_Use", "Non_SARS_Covid_2_Vaccination_Status"]

    sql_table_dict["baseline.csv"] = visit_list
    sql_table_dict["follow_up.csv"] = [i for i in visit_list if i != "Participant"]  # Participant table does not exist in follow up

    sql_table_dict["aliquot.csv"] = ["Unique_Tube_Info", "Aliquot"]
    sql_table_dict["biospecimen.csv"] = ["Unique_Tube_Info", "Biospecimen"]

    sql_table_dict["assay_data.csv"] = ["Assay_Metadata", "Assay_Calibration", "Assay_Bio_Target"]
    sql_table_dict["assay_target.csv"] = ["Assay_Target"]
    sql_table_dict["assay_qc.csv"] = ["Assay_Quality_Controls"]
    sql_table_dict["assay_conversion.csv"] = ["Assay_Organism_Conversion"]

    sql_table_dict["study_design.csv"] = ["Study_Design"]

    sql_table_dict["consumable.csv"] = ["Unique_Consumable", "Biospecimen_Consumable"]
    sql_table_dict["reagent.csv"] =    ["Unique_Reagent",    "Biospecimen_Reagent"]
    sql_table_dict["equipment.csv"] =  ["Unique_Equipment",  "Biospecimen_Equipment"]

    sql_table_dict["shipping_manifest.csv"] = ["Shipping_Manifest"]

    sql_table_dict["covid_history.csv"] = ["Covid_History"]
    sql_table_dict["covid_hist_sql.csv"] = ["Covid_History"]

    sql_table_dict["covid_vaccination_status.csv"] = ["Covid_Vaccination_Status"]
    sql_table_dict["vac_status_sql.csv"] = ["Covid_Vaccination_Status"]

    sql_table_dict["biospecimen_test_result.csv"] = ["Biospecimen_Test_Results"]
    sql_table_dict["test_results_sql.csv"] = ["Biospecimen_Test_Results"]

    sql_table_dict["treatment_history.csv"] = ["Treatment_History"]

    sql_table_dict["autoimmune_cohort.csv"] = ["AutoImmune_Cohort"]
    sql_table_dict["cancer_cohort.csv"] = ["Cancer_Cohort"]
    sql_table_dict["hiv_cohort.csv"] = ["HIV_Cohort"]
    sql_table_dict["organ_transplant_cohort.csv"] = ["Organ_Transplant_Cohort"]

    sql_table_dict["visit_info_sql.csv"] = ["Participant_Visit_Info"]
    sql_table_dict["submission.csv"] = ["Submission"]

    sql_table_dict["bsi_child.csv"] = ["BSI_Child_Aliquots"]
    sql_table_dict["bsi_parent.csv"] = ["BSI_Parent_Aliquots"]

    return sql_table_dict


def update_norm_cancer(conn, engine):
    harm_table = pd.read_sql(("SELECT * FROM Normalized_Cancer_Names_v2;"), conn)
    cancer_dict = pd.read_sql(("SELECT * FROM Normalized_Cancer_Dictionary;"), conn)
    cancer_names = pd.read_sql(("SELECT Visit_Info_ID, Cancer_Description_Or_ICD10_codes FROM Comorbidities_Names;"), conn)
    full_table = pd.DataFrame(columns=['Visit_Info_ID', 'Original Cancer Name', 'Harmonized Cancer Name', 'SEER Category', "Found_In_Dict"])

    cancer_names.fillna('Not Reported', inplace=True)
    for curr_row in cancer_names.index:
        
        z = cancer_names["Cancer_Description_Or_ICD10_codes"][curr_row].split("|")
        z = [i.strip() for i in z]
        z = pd.DataFrame(z, columns= ["Cancer"])
        z["Visit_Info_ID"] = cancer_names["Visit_Info_ID"][curr_row]
        merge_1 = z.merge(cancer_dict, how="left", indicator="Found_In_Dict")
        merge_1 = merge_1.rename(columns={'Cancer': 'Original Cancer Name'})
        full_table = pd.concat([full_table, merge_1])


    full_table.drop_duplicates(inplace=True)
    full_table.to_sql("Normalized_Cancer_Names_v2",con=engine, if_exists="append", index=False)
    
    #full_table = full_table.query("`Original Cancer Name` != ''")         #remove records that are not found
    z = full_table.merge(harm_table, on=["Visit_Info_ID", 'Original Cancer Name'], how="left")
    merge_data = z.query("`Harmonized Cancer Name_x` != `Harmonized Cancer Name_y`")
    new_data = merge_data.query("`Harmonized Cancer Name_y` != `Harmonized Cancer Name_y`")

    new_data = new_data[['Visit_Info_ID', 'Original Cancer Name', 'Harmonized Cancer Name_x', 'SEER Category_x']]
    new_data.columns = [i.replace("_x","") for i in new_data.columns]
    new_data.to_sql("Normalized_Cancer_Names_v2",con=engine, if_exists="append", index=False)
    return full_table

