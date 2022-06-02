from import_loader_v2 import *
import copy


def Db_loader_main(sub_folder, connection_tuple, validation_date, **kwargs):
    """main function that will import data from s3 bucket into SQL database"""
    pd.options.mode.chained_assignment = None
    s3_client = boto3.client('s3', aws_access_key_id=aws_creds_prod.aws_access_id, aws_secret_access_key=aws_creds_prod.aws_secret_key,
                             region_name='us-east-1')
    bucket_name = "nci-cbiit-seronet-submissions-passed"

    if sub_folder == "Reference Pannel Submissions":
        sql_table_dict = get_sql_dict_ref()
    elif sub_folder == "Vaccine Response Submissions":
        sql_table_dict = get_sql_dict_vac()
    else:
        return

    Update_Assay_Data = get_kwarg_parms("Update_Assay_Data", kwargs)
    Update_Study_Design = get_kwarg_parms("Update_Study_Design", kwargs)
    Update_BSI_Tables = get_kwarg_parms("Update_BSI_Tables", kwargs)
############################################################################################################################
    try:
        conn = connection_tuple[2]
        engine = connection_tuple[1]
        sql_column_df = connection_tuple[0]

        done_submissions = pd.read_sql(("SELECT * FROM Submission"), conn)  # list of all submissions previously done in db
        done_submissions.drop_duplicates("Submission_S3_Path", inplace=True)

        all_submissions = []  # get list of all submissions by CBC
        all_submissions = get_all_submissions(s3_client, bucket_name, sub_folder, "Feinstein_CBC01", all_submissions)
        all_submissions = get_all_submissions(s3_client, bucket_name, sub_folder, "UMN_CBC02", all_submissions)
        all_submissions = get_all_submissions(s3_client, bucket_name, sub_folder, "ASU_CBC03", all_submissions)
        all_submissions = get_all_submissions(s3_client, bucket_name, sub_folder, "Mt_Sinai_CBC04", all_submissions)

        time_stamp = [i.split("/")[2] for i in all_submissions]
        for i in time_stamp:
            if i[2] == '-':
                time_stamp[time_stamp.index(i)] = datetime.datetime.strptime(i, "%H-%M-%S-%m-%d-%Y")
            else:
                time_stamp[time_stamp.index(i)] = datetime.datetime.strptime(i, "%Y-%m-%d-%H-%M-%S")

        #  sort need to work by date time
        all_submissions = [x for _, x in sorted(zip(time_stamp, all_submissions))]  # sort submission list by time submitted

        all_submissions = [i for i in enumerate(all_submissions)]
        # Filter list by submissions already done
        all_submissions = [i for i in all_submissions if i[1] not in done_submissions["Submission_S3_Path"].tolist()]

    except Exception as e:
        all_submissions = []
        print(e)
############################################################################################################################
    folders = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=sub_folder)["Contents"]
    master_dict = {}  # dictionary for all submissions labeled as create
    update_dict = {}  # dictionary for all submissions labeled as update
    try:
        for curr_sub in all_submissions:
            index = curr_sub[0] + 1
            file_name = curr_sub[1].split("/")
            print(f"\nWoring on Submision #{index}: {file_name[1]}:  {file_name[2]} \n {file_name[3]}")

            upload_date, intent, sub_name = get_upload_info(s3_client, bucket_name, curr_sub, sub_folder)  # get submission info
            if intent == "Create":
                master_dict = get_tables_to_load(s3_client, bucket_name, folders, curr_sub, conn, sub_name, index, upload_date, intent, master_dict)
            elif intent == "Update":
                update_dict = get_tables_to_load(s3_client, bucket_name, folders, curr_sub, conn, sub_name, index, upload_date, intent, update_dict)
            else:
                print(f"Submission Intent: {intent} is not valid")

        master_dict = fix_aliquot_ids(master_dict, "first")
        update_dict = fix_aliquot_ids(update_dict, "last")
        master_data_dict = get_master_dict(master_dict, update_dict, sql_column_df, sql_table_dict)

        if Update_Assay_Data is True:
            master_data_dict = upload_assay_data(master_data_dict)
        if Update_Study_Design is True:
            master_data_dict["study_design.csv"] = {"Data_Table": []}
            master_data_dict["study_design.csv"]["Data_Table"] = get_box_data.get_study_design()
        if Update_BSI_Tables is True:
            master_data_dict = get_bsi_files(s3_client, bucket_name, sub_folder, master_data_dict)

        done_sql_tables = add_tables_to_database(engine, conn, sql_table_dict, sql_column_df, master_data_dict, [])

    except Exception as e:
        display_error_line(e)


def display_error_line(ex):
    trace = []
    tb = ex.__traceback__
    while tb is not None:
        trace.append({"filename": tb.tb_frame.f_code.co_filename,
                      "name": tb.tb_frame.f_code.co_name,
                      "lineno": tb.tb_lineno})
        tb = tb.tb_next
    print(str({'type': type(ex).__name__, 'message': str(ex), 'trace': trace}))


def get_kwarg_parms(update_str, kwargs):
    if update_str in kwargs:
        Update_Table = kwargs[update_str]
    else:
        Update_Table = False
    return Update_Table


def get_all_submissions(s3_client, bucket_name, sub_folder, cbc_name, all_submissions):
    """ scans the buceket name and provides a list of all files paths found """
    try:
        key_list = s3_client.list_objects(Bucket=bucket_name, Prefix=sub_folder + "/" + cbc_name)
        if 'Contents' in key_list:
            key_list = key_list["Contents"]
            key_list = [i["Key"] for i in key_list if ("UnZipped_Files" in i["Key"])]
            file_parts = [os.path.split(i)[0] for i in key_list]
            file_parts = [i for i in file_parts if "test/" not in i[0:5]]
            file_parts = [i for i in file_parts if "Submissions_in_Review" not in i]
            uni_submissions = list(set(file_parts))
        else:
            uni_submissions = []  # no submissions found for given cbc
    except Exception as e:
        print("Erorr found")
        uni_submissions = []
    finally:
        return all_submissions + uni_submissions


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
    return curr_table


def get_upload_info(s3_client, bucket_name, curr_sub, sub_folder):
    if sub_folder in curr_sub[1]:
        sub_obj = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=curr_sub[1])
    else:
        sub_obj = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=sub_folder + file_sep + curr_sub[1])
    try:
        upload_date = sub_obj["Contents"][0]["LastModified"]
        upload_date = upload_date.replace(tzinfo=None)  # removes timezone element from aws
    except Exception as e:
        upload_date = 0
    submission_file = [i["Key"] for i in sub_obj["Contents"] if "submission.csv" in i["Key"]]
    submission_file = submission_file[0].replace(".csv", "")

    curr_table = pd_s3.get_df_from_keys(s3_client, bucket_name, submission_file, suffix="csv", format="csv", na_filter=False, output_type="pandas")
    intent = curr_table.iloc[3][1]
    sub_name = curr_table.columns[1]
    return upload_date, intent, sub_name


def get_tables_to_load(s3_client, bucket, folders, curr_sub, conn, sub_name, index, upload_date, intent, master_dict):
    """Takes current submission and gets all csv files into pandas tables """
    files = [i["Key"] for i in folders if curr_sub[1] in i["Key"]]
    files = [i for i in files if ".csv" in i]
    data_dict = get_data_dict(s3_client, bucket, files, conn, curr_sub, sub_name, index, upload_date, intent)

    if len(data_dict) > 0:
        master_dict = combine_dictionaries(master_dict, data_dict)
    return master_dict


def get_data_dict(s3_client, bucket, files, conn, curr_sub, sub_name, index, upload_date, intent):
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
                curr_table["Submission_Index"] = index
            curr_table = curr_table.loc[~(curr_table == '').all(axis=1)]
        except Exception as e:
            print(e)
        curr_table = clean_up_tables(curr_table)
        if "secondary_confirmation" in split_path[1]:
            data_dict["secondary_confirmation_test_result.csv"] = {"Data_Table": []}
            data_dict["secondary_confirmation_test_result.csv"]["Data_Table"] = curr_table
        else:
            data_dict[split_path[1]] = {"Data_Table": []}
            #  curr_table.dropna(inplace=True)
            data_dict[split_path[1]]["Data_Table"] = curr_table
    return data_dict


def get_master_dict(master_data_dict, master_data_update, sql_column_df, sql_table_dict):
    for key in master_data_dict.keys():
        try:
            table = sql_table_dict[key]
            primary_key = sql_column_df.query("Table_Name == @table and Primary_Key == 'True'")["Column_Name"].tolist()
        except Exception as e:
            primary_key = "Biospecimen_ID"
        if key in ['shipping_manifest.csv']:
            primary_key = 'Current Label'
        if "Test_Result" in primary_key:
            primary_key[primary_key.index("Test_Result")] = 'SARS_CoV_2_PCR_Test_Result'

        if isinstance(primary_key, str):
            primary_key = [primary_key]
        if key in master_data_update.keys():
            try:
                x = pd.concat([master_data_dict[key]["Data_Table"], master_data_update[key]["Data_Table"]])
                if key not in ["submission.csv"]:
                    primary_key = [i for i in primary_key if i in x.columns]
                    if len(primary_key) > 0:
                        x = x.drop_duplicates(primary_key, keep='last')
                    else:
                        print("no key")
                master_data_dict[key]["Data_Table"] = x
            except Exception as e:
                print(e)

    for key in master_data_update.keys():  # key only in update
        if key not in master_data_dict.keys():
            master_data_dict[key] = master_data_update[key]

    return master_data_dict


def convert_data_type(v):
    if isinstance(v, (datetime.datetime, datetime.time, datetime.date)):
        return v
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
            return parse(v).date()
        except ValueError:
            return v


def correct_var_types(data_table, sql_column_df):
    col_names = data_table.columns
    for curr_col in col_names:
        z = sql_column_df.query("Column_Name == @curr_col").drop_duplicates("Column_Name")
        if len(z) > 0:
            var_type = z.iloc[0]["Var_Type"]
        else:
            var_type = "VARCHAR(255)"
        if curr_col in ["Age", "Storage_Time_in_Mr_Frosty", "Biospecimen_Collection_to_Test_Duration"]:
            data_table = round_data(data_table, curr_col)
        elif "varchar" in var_type.lower():
            data_table[curr_col] = [str(i) for i in data_table[curr_col]]
        else:
            data_table[curr_col] = [convert_data_type(c) for c in data_table[curr_col]]
    return data_table


def round_data(data_table, test_col):
    for x in data_table.index:
        try:
            data_table.loc[x, test_col] = round(float(data_table.loc[x, test_col]), 1)
        except Exception as e:
            data_table.loc[x, test_col] = str(data_table.loc[x, test_col])
    return data_table


def clean_up_tables(curr_table):
    if "Submission_Index" in curr_table.columns:
        x = curr_table.drop("Submission_Index", axis=1)
        x.replace("", float("NaN"), inplace=True)
        x.dropna(axis=0, how="all", thresh=None, subset=None, inplace=True)
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
    return curr_table


def fix_date_cols(filt_table, sql_column_df):
    filt_table.reset_index(inplace=True, drop=True)
    for i in filt_table.columns:
        test_df = sql_column_df.query("Column_Name == @i and Var_Type == 'DATE'")
        if len(test_df) > 0:
            filt_table = fix_cols(filt_table, i, "Date")
        test_df = sql_column_df.query("Column_Name == @i and Var_Type == 'TIME'")
        if len(test_df) > 0:
            filt_table = fix_cols(filt_table, i, "Time")
        test_df = sql_column_df.query("Column_Name == @i and Var_Type == 'TINYINT'")
        if len(test_df) > 0:
            filt_table[i].replace("Yes", True, inplace=True)
            filt_table[i].replace("No", False, inplace=True)
    return filt_table


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
    x_name = pathlib.PurePath(curr_sub[1])
    part_list = x_name.parts
    try:
        curr_time = datetime.datetime.strptime(part_list[2], "%H-%M-%S-%m-%d-%Y")
    except Exception as e:  # time stamp was corrected
        curr_time = datetime.datetime.strptime(part_list[2], "%Y-%m-%d-%H-%M-%S")
    cbc_id = get_cbc_id(conn, sub_name)
    file_name = re.sub("submission_[0-9]{3}_", "", part_list[3])
    sql_df = pd.DataFrame([[index, cbc_id, curr_time, sub_name, file_name, curr_sub[1], upload_date, intent]],
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


def fix_aliquot_ids(master_data_dict, keep_order):
    if "aliquot.csv" in master_data_dict:
        z = master_data_dict["aliquot.csv"]["Data_Table"]["Aliquot_ID"].tolist()
        master_data_dict["aliquot.csv"]["Data_Table"]["Aliquot_ID"] = zero_pad_ids(z)
        z = master_data_dict["aliquot.csv"]["Data_Table"]["Aliquot_Volume"].tolist()
        z = [int(i.replace("N/A", "0")) if isinstance(i, str) else i for i in z]
        master_data_dict["aliquot.csv"]["Data_Table"]["Aliquot_Volume"] = z
        z = master_data_dict["aliquot.csv"]["Data_Table"]
        z.sort_values("Submission_Index", axis=0, ascending=True, inplace=True)
        z.drop_duplicates("Aliquot_ID", keep=keep_order, inplace=True)
        master_data_dict["aliquot.csv"]["Data_Table"] = z
    if "shipping_manifest.csv" in master_data_dict:
        z = master_data_dict["shipping_manifest.csv"]["Data_Table"]["Current Label"].tolist()
        master_data_dict["shipping_manifest.csv"]["Data_Table"]["Current Label"] = zero_pad_ids(z)
        z = master_data_dict["shipping_manifest.csv"]["Data_Table"]["Volume"].tolist()
        z = [int(i.replace("N/A", "0")) if isinstance(i, str) else i for i in z]
        master_data_dict["shipping_manifest.csv"]["Data_Table"]["Volume"] = z
        z = master_data_dict["shipping_manifest.csv"]["Data_Table"]
        z.sort_values("Submission_Index", axis=0, ascending=True, inplace=True)
        z.drop_duplicates("Current Label", keep=keep_order, inplace=True)
        master_data_dict["shipping_manifest.csv"]["Data_Table"] = z
    return master_data_dict


def zero_pad_ids(data_list):
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
    return data_list


def upload_assay_data(data_dict):
    #  populate the assay data tables which are independant from submissions
    assay_data, assay_target, all_qc_data, converion_file = get_box_data.get_assay_data()
    data_dict = add_assay_to_dict(data_dict, "assay_data.csv", assay_data)
    data_dict = add_assay_to_dict(data_dict, "assay_target.csv", assay_target)
    data_dict = add_assay_to_dict(data_dict, "assay_qc.csv", all_qc_data)
    data_dict = add_assay_to_dict(data_dict, "assay_conversion.csv", converion_file)
    return data_dict


def add_assay_to_dict(data_dict, csv_file, data_table):
    data_table.rename(columns={"Target_Organism": "Assay_Target_Organism"}, inplace=True)
    data_dict[csv_file] = {"Data_Table": []}
    data_dict[csv_file]["Data_Table"] = data_table
    return data_dict


def add_tables_to_database(engine, conn, sql_table_dict, sql_column_df, master_data_dict, done_tables):
    not_done = []
    valid_files = [i for i in sql_table_dict if "_sql.csv" not in i]
    filtered_dictionary = {key: value for key, value in sql_table_dict.items() if key in valid_files}

    key_count = pd.crosstab(sql_column_df["Table_Name"], sql_column_df["Foreign_Key_Count"])
    key_count.reset_index(inplace=True)
    key_count = key_count.query("Table_Name not in @done_tables")
    if len(key_count) == 0:
        return

    for curr_table in key_count["Table_Name"].tolist():
        check_foreign = key_count.query("Table_Name == @curr_table and 1.0 > 0")
        if len(check_foreign) > 0:
            foreign_table = sql_column_df.query("Table_Name == @curr_table and Foreign_Key_Table != ''")["Foreign_Key_Table"].tolist()
            check = all(item in done_tables for item in foreign_table)
            if check is False:
                not_done.append(curr_table)
                continue    # dependent tables not checked yet

        done_tables.append(curr_table)
        y = sql_column_df.query("Table_Name == @curr_table")
        y = y.query("Autoincrement != True")
        sql_df = pd.read_sql((f"Select * from {curr_table}"), conn)
        sql_df = sql_df[y["Column_Name"].tolist()]
        sql_df.fillna("N/A", inplace=True)
        sql_df.replace({np.nan: -1e9, 'nan': -1e9, '': -1e9}, inplace=True)
        sql_df = correct_var_types(sql_df, sql_column_df)

        csv_file = [key for key, value in sql_table_dict.items() if curr_table in value]
        output_file = pd.DataFrame(columns=y["Column_Name"].tolist())
        if len(csv_file) == 0:
            continue
        else:
            for curr_file in csv_file:
                if curr_file in master_data_dict:
                    x = copy.copy(master_data_dict[curr_file]["Data_Table"])
                else:
                    continue
                x.rename(columns={"Comments": curr_table + "_Comments"}, inplace=True)
                if curr_table == "Tube":
                    x = clean_tube_names(x)
                    if curr_file == "aliquot.csv":
                        x["Tube_Used_For"] = "Aliquot"
                    elif curr_file == "biospecimen.csv":
                        x["Tube_Used_For"] = "Biospecimen_Collection"
                if curr_table == "Assay_Bio_Target":
                    x = get_bio_target(x, conn)
                try:
                    x.replace({np.nan: -1e9, 'nan': -1e9, '': -1e9, True: '1', False: '0'}, inplace=True)
                    x = correct_var_types(x, sql_column_df)
                    x = get_col_names(x, y, conn, curr_table, curr_file, sql_column_df)
                    x.drop_duplicates(inplace=True)
                except Exception as e:
                    print(e)
                output_file = pd.concat([output_file, x])

            if len(output_file) > 0:
                output_file.replace("nan", "", inplace=True)
                sql_df.replace({-1e9: "N/A", "-1000000000.0": "N/A"}, inplace=True)
                output_file.replace({-1e9: "N/A", "-1000000000.0": "N/A"}, inplace=True)

                if curr_table not in ["Tube", "Aliquot"]:
                    sql_df.replace("\.0", "", regex=True, inplace=True)
                    output_file.replace("\.0", "", regex=True, inplace=True)
                number_col = sql_column_df.query("Table_Name == @curr_table and Var_Type in ['FLOAT', 'INTEGER', 'DOUBLE']")["Column_Name"].tolist()
                for n_col in number_col:
                    if n_col in output_file.columns:
                        output_file[n_col].replace("", 0, inplace=True)
                        output_file[n_col].replace("N/A", 0, inplace=True)

                try:
                    z = output_file.merge(sql_df, how="outer", indicator=True)
                    new_data = z.query("_merge == 'left_only'")       # new or update data
                except Exception as e:
                    print(e)
                    new_data = []
                if len(new_data) > 0:
                    new_data.drop("_merge", inplace=True, axis=1)
                    primary_keys = sql_column_df.query("Table_Name == @curr_table and Primary_Key == 'True'")["Column_Name"].tolist()
                    merge_data = new_data.merge(sql_df[primary_keys], how="left", indicator=True)
                    new_data = merge_data.query("_merge == 'left_only'")
                    update_data = merge_data.query("_merge == 'both'")
                    if len(new_data) > 0:
                        new_data.drop("_merge", inplace=True, axis=1)
                        print(f"\n## Adding New Rows to table: {curr_table} ##\n")
                        try:
                            new_data.to_sql(name=curr_table, con=engine, if_exists="append", index=False)
                            conn.connection.commit()
                        except Exception as e:
                            print("error loading table")
                    if len(update_data) > 0:
                        row_count = len(update_data)
                        print(f"\n## Updating {row_count} Rows in table: {curr_table} ##\n")
                        update_tables(conn, engine, primary_keys, update_data, curr_table)
                else:
                    print(f" \n## {curr_table} has been checked, no data to add")
            else:
                print(f" \n## {curr_table} was not found in submission, nothing to add")
    if len(not_done) > 0:
        add_tables_to_database(engine, conn, sql_table_dict, sql_column_df, master_data_dict, done_tables)


def get_col_names(x, y, conn, curr_table, curr_file, sql_column_df):
    col_list = y["Column_Name"].tolist()

    if curr_table == "Participant_Visit_Info":
        if curr_file == "baseline.csv":
            x['Type_Of_Visit'] = "Baseline"
            x['Visit_Number'] = "1"
            x['Unscheduled_Visit'] = "No"
        else:
            x['Type_Of_Visit'] = "Follow_up"
            base = x.query("Baseline_Visit == 'Yes'")
            if len(base) > 0:
                x.loc[base.index, "Type_Of_Visit"] = "Baseline"
        x["Visit_Info_ID"] = (x["Research_Participant_ID"] + " : " + [i[0] for i in x["Type_Of_Visit"]] +
                              ["%02d" % (int(i),) for i in x['Visit_Number']])
    else:
        if "Visit_Info_ID" in col_list:
            visit_data = pd.read_sql(("SELECT Visit_Info_ID, Research_Participant_ID, Visit_Number FROM Participant_Visit_Info;"), conn)
            if curr_file == "baseline.csv":
                x["Visit_Number"] = "1"
            x.replace("Baseline(1)", "1", inplace=True)
            x = x.merge(visit_data)
    if "Biospecimen_" in curr_table and "Test_Results" not in curr_table:
        table_name = curr_table.replace("Biospecimen_", "")
        table_data = pd.read_sql((f"SELECT * FROM {table_name}"), conn)
        x = x.merge(table_data)

    if curr_table in ["Biospecimen", "Aliquot"]:
        tube_data = pd.read_sql(("SELECT * FROM Tube;"), conn)
        tube_data.fillna("N/A", inplace=True)
        if curr_table == "Biospecimen":
            tube_data.columns = [i.replace("Tube", "Collection_Tube") for i in tube_data.columns]
            x["Collection_Tube_Type_Expiration_Date"] = [i if i == "N/A" else parse(i).date()
                                                         for i in x["Collection_Tube_Type_Expiration_Date"].tolist()]
        elif curr_table == "Aliquot":
            tube_data.columns = [i.replace("Tube", "Aliquot_Tube") for i in tube_data.columns]
            x["Aliquot_Tube_Type_Expiration_Date"] = [i if i == "N/A" else parse(i).date() for i in x["Aliquot_Tube_Type_Expiration_Date"].tolist()]
        try:
            x = x.merge(tube_data)
        except Exception as e:
            print(e)

        if curr_table == "Biospecimen":
            x.rename(columns={"Collection_Tube_ID": "Biospecimen_Tube_ID"}, inplace=True)
        else:
            x.rename(columns={"Collection_Tube_ID": "Aliquot_Tube_ID"}, inplace=True)
    try:
        x = x[col_list]
    except Exception as e:
        print(e)
    return x


def get_bio_target(curr_table, conn):
    curr_cols = curr_table.columns.tolist()
    curr_cols = [i.replace("Target_biospecimen_is_", "") for i in curr_cols]
    curr_cols = [i.replace("/", "_") for i in curr_cols]
    curr_table.columns = curr_cols

    bio_type = pd.read_sql("Select * FROM Biospecimen_Type", conn)
    new_col_names = [i for i in bio_type["Biospecimen_Type"].tolist() if i in curr_table.columns]
    bio_table = curr_table[new_col_names].stack().to_frame()
    bio_table.reset_index(inplace=True)
    bio_table.columns = ["Assay_ID", "Target_Biospecimen_Type", "Is_Present"]
    bio_table["Assay_ID"] = curr_table["Assay_ID"].repeat(len(new_col_names)).tolist()
    bio_table = bio_table.query("Is_Present == 'Yes'")
    curr_table = curr_table.merge(bio_table)
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
    update_table.drop("_merge", inplace=True, axis=1)
    col_list = update_table.columns.tolist()
    col_list = [i for i in col_list if i not in primary_keys]

    for index in update_table.index:
        try:
            curr_data = update_table.loc[index, col_list].values.tolist()
            primary_value = update_table.loc[index, primary_keys].values.tolist()
            update_str = ["`" + i + "` = '" + str(j) + "'" for i, j in zip(col_list, curr_data)]
            update_str = ', '.join(update_str)

            update_str = update_str.replace("N/A", str(np.nan))
            update_str = update_str.replace("'nan'", "NULL")

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

    sql_table_dict["aliquot.csv"] = ["Tube", "Aliquot"]
    sql_table_dict["biospecimen.csv"] = ["Tube", "Biospecimen"]

    sql_table_dict["confirmatory_clinical_test.csv"] = ["Confirmatory_Clinical_Test"]
    sql_table_dict["demographic.csv"] = ["Participant", "Prior_Covid_Outcome", "Participant_Comorbidity_Reported", "Comorbidity"]
    sql_table_dict["prior_clinical_test.csv"] = ["Participant_Prior_SARS_CoV2_PCR", "Participant_Prior_Infection_Reported"]

    sql_table_dict["consumable.csv"] = ["Consumable", "Biospecimen_Consumable"]
    sql_table_dict["reagent.csv"] = ["Reagent", "Biospecimen_Reagent"]
    sql_table_dict["equipment.csv"] = ["Equipment", "Biospecimen_Equipment"]

    sql_table_dict["secondary_confirmation_test_result.csv"] = ["Secondary_Confirmatory_Test"]
    sql_table_dict["bsi_child.csv"] = ["BSI_Child_Aliquots"]
    sql_table_dict["bsi_parent.csv"] = ["BSI_Parent_Aliquots"]
    sql_table_dict["submission.csv"] = ["Submission"]
    sql_table_dict["shipping_manifest.csv"] = ["Shipping_Manifest"]
    return sql_table_dict


def get_sql_dict_vacc():
    sql_table_dict = {}
    visit_list = ["Participant_Visit_Info", "Participant", "Specimens_Collected", "Participant_Comorbidities", "Comorbidities_Names",
                  "Participant_Other_Conditions", "Participant_Other_Condition_Names", "Drugs_And_Alcohol_Use", "Non_SARS_Covid_2_Vaccination_Status"]

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
    sql_table_dict["hiv_cohort.csv"] = ["HIV_Cohort"]
    sql_table_dict["organ_transplant_cohort.csv"] = ["Organ_Transplant_Cohort"]

    sql_table_dict["visit_info_sql.csv"] = ["Participant_Visit_Info"]
    sql_table_dict["submission.csv"] = ["Submission"]

    sql_table_dict["bsi_child.csv"] = ["BSI_Child_Aliquots"]
    sql_table_dict["bsi_parent.csv"] = ["BSI_Parent_Aliquots"]

    return sql_table_dict
