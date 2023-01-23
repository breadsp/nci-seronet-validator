# -*- coding: utf-8 -*-
"""
Created on Fri Nov  4 09:21:43 2022

@author: breadsp2
"""

from import_loader_v2 import pd, sd, np
from connect_to_sql_db import connect_to_sql_db
import os

def Dictionary_Updater():
    pd.options.mode.chained_assignment = None  # default='warn'
    file_sep = os.path.sep
     
    sql_column_df, engine, conn = connect_to_sql_db(pd, sd, "seronetdb-Vaccine_Response")
    #box_dir = "C:" + file_sep + "Users" + file_sep + os.getlogin() + file_sep + "Box"
    #normal_dir = box_dir + file_sep + "CBC Data Submission Documents" + file_sep + "Dictionary_Files_for_Normalized_Terms"
    normal_dir = r"C:\Dictionary_Files"
    file_path = normal_dir + file_sep + "Comorbidity_Names_harmonized.csv"
    try:
        norm_table = pd.read_csv(file_path)

        if "Comorbidity" in file_path:   # Ex: Reported_Comorbidity_Names_Release_2.0_harmonized 17Aug2022
            Normalized_Comorbidities(norm_table, pd, conn, engine)
        if "Cancer" in file_path: # Ex:  Cancer_Cohort_harmonized 11Aug2021
            Normalized_Cancer(norm_table, pd, conn, engine)
        if "Treatment" in file_path:  # Ex:  Reported_Condition_Treatment_Release_1.0_harmonized_16Aug2022
            Normalized_Treatment(norm_table, pd, conn, engine)

    except Exception as e:
        display_error_line(e)
    finally:
        print('## Database has been checked.  Closing the connections ##')
        if conn:
            conn.close()

def display_error_line(ex):
    trace = []
    tb = ex.__traceback__
    while tb is not None:
        trace.append({"filename": tb.tb_frame.f_code.co_filename, "name": tb.tb_frame.f_code.co_name, "lineno": tb.tb_lineno})
        tb = tb.tb_next
    print(str({'type': type(ex).__name__, 'message': str(ex), 'trace': trace}))


def clean_table(norm_table):
    norm_table.fillna("No Data", inplace=True)
    for col_name in norm_table:
        if "Index" not in col_name:
            norm_table[col_name] = norm_table[col_name].str.strip()
    norm_table = norm_table.drop_duplicates()
    return norm_table

def get_to_add(input_data, sql_df, prim_key):
    merge_data = input_data.merge(sql_df, how="left", indicator=True)
    merge_data = merge_data.query("_merge == 'left_only'")  # if both then already done
    merge_data = merge_data[input_data.columns]
    merge_data.reset_index(inplace=True, drop=True)
    z = merge_data[prim_key].merge(sql_df, how="left", on=prim_key, indicator=True)

    new_data = z.query("_merge not in ['both']")
    if len(new_data) > 0:
        new_data = merge_data.iloc[new_data.index]        # primary key not in db (new record)
        new_data.drop_duplicates(inplace=True)
        
    update_data = z.query("_merge in ['both']")
    if len(update_data) > 0:

        update_data = merge_data.iloc[update_data.index]  # primary key found but record changed (update)
        update_data.drop_duplicates(inplace=True)

    return new_data, update_data


def Normalized_Comorbidities(norm_table, pd, conn, engine):
    table_name = "Normalized_Comorbidity_Dictionary"
    norm_table = norm_table[['Comorbid_Name', 'Orgional_Description_Or_ICD10_codes', 'Normalized_Description']]
    norm_dict = pd.read_sql(("SELECT * FROM Normalized_Comorbidity_Dictionary;"), conn)
    norm_table = clean_table(norm_table)
    norm_dict = clean_table(norm_dict)
    
    new_data, update_data = get_to_add(norm_table, norm_dict, ['Comorbid_Name', 'Orgional_Description_Or_ICD10_codes'])
    col_names = ", ".join(["`" + i + "`" for i in norm_dict.columns])
    add_count = add_new_rows(conn, engine, new_data, table_name, col_names)
    
    update_count = update_tables(conn, engine, ['Comorbid_Name', 'Orgional_Description_Or_ICD10_codes'], update_data, "Normalized_Comorbidity_Dictionary")
 
    if add_count == -1 or update_count == -1:   #error updating dictionary, do not make tables
        return add_count, update_count, table_name
 
    norm_dict = pd.read_sql(("SELECT * FROM Normalized_Comorbidity_Dictionary;"), conn)                          #normalized dictoinary
    norm_db = pd.read_sql(("SELECT * FROM `seronetdb-Vaccine_Response`.Normalized_Comorbidity_Names;"), conn)    #harmonized output table

    org_names = pd.read_sql(("SELECT * FROM Comorbidities_Names;"), conn)           #orional Names of the Comorbidities
    org_cond = pd.read_sql(("SELECT * FROM Participant_Comorbidities;"), conn)      #yes/no/ condition status flag

    normalized_df = pd.DataFrame()
    normalized_df["Visit_Info_ID"] = org_names["Visit_Info_ID"]
    norm_dict["Normalized_Description"] = norm_dict["Normalized_Description"].str.strip()
    norm_dict.fillna("No Data", inplace=True)
    org_names.fillna("No Data", inplace=True)
    comorbid_cols = org_cond.columns.tolist()
    for curr_cond in comorbid_cols:
        col_name = [i for i in org_names.columns if curr_cond in i]
        x = norm_dict.query("Comorbid_Name in @curr_cond")

        if len(x) > 0:
            try:
                x["Orgional_Description_Or_ICD10_codes"] = x["Orgional_Description_Or_ICD10_codes"].str.lower()
                normalized_df[curr_cond + "_Description_Normalized"] = ""
                org_names[col_name[0]] = org_names[col_name[0]].str.lower()
                org_names[col_name[0]] = org_names[col_name[0]].str.replace("crohns", "crohn's")
                org_names[col_name[0]] = org_names[col_name[0]].str.replace("hashimotos", "hashimoto's")

                for index in org_names.index:
                    split_names = org_names[col_name[0]][index].split("|")
                    split_names = [i.strip() for i in split_names]
                    norm_term = x.query("Orgional_Description_Or_ICD10_codes in @split_names")["Normalized_Description"]
                    norm_term = list(set(norm_term))
                    norm_term.sort()
                    if len(norm_term) == 0:
                        print(f"{split_names} was not found in {curr_cond}")
                    normalized_df[curr_cond + "_Description_Normalized"][index] = " | ".join(norm_term)
            except Exception as e:
                print(e)

    normalized_df.replace("No Data", np.nan, inplace=True)
    normalized_df.rename(columns={"Viral_Infection_Description_Normalized": "Viral_Infection_Normalized"}, inplace=True)
    normalized_df.rename(columns={"Bacterial_Infection_Description_Normalized": "Bacterial_Infection_Normalized"}, inplace=True)
    normalized_df.fillna('Participant Does Not Have', inplace=True)

    new_data, update_data = get_to_add(normalized_df, norm_db, ['Visit_Info_ID'])
    col_names = ", ".join(["`" + i + "`" for i in new_data.columns])
    add_new_rows(conn, engine, new_data, "Normalized_Comorbidity_Names", col_names)
    update_tables(conn, engine,  ['Visit_Info_ID'], update_data, "Normalized_Comorbidity_Names")


def Normalized_Treatment(norm_table, pd, conn, sql_connect):
    table_name = "Normalized_Treatment_Dict"
    norm_table = norm_table[['Treatment', 'Harmonized Treatment']]
    norm_dict = pd.read_sql(("SELECT * FROM Normalized_Treatment_Dict;"), conn)
    norm_table = clean_table(norm_table)
    norm_dict = clean_table(norm_dict)

    new_data, update_data = get_to_add(norm_table, norm_dict, ["Treatment"])

    add_count = add_new_rows(conn, sql_connect, new_data, "Normalized_Treatment_Dict", "`Treatment`, `Harmonized Treatment`")
    update_count = update_tables(conn, sql_connect, ["Treatment"], update_data, "Normalized_Treatment_Dict")
    
    if add_count == -1 or update_count == -1:   #error updating dictionary, do not make tables
        return add_count, update_count, table_name
    
    org_names = pd.read_sql(("SELECT Visit_Info_ID, Health_Condition_Or_Disease, Treatment FROM Treatment_History;"), conn)
    norm_dict = pd.read_sql(("SELECT * FROM Normalized_Treatment_Dict;"), conn)
    curr_norm = pd.read_sql(("SELECT * FROM Normalized_Treatment_Names"), conn)

    org_names.rename(columns={"Treatment": "Original Treatment Name"}, inplace=True)
    norm_dict.rename(columns={"Treatment": "Original Treatment Name"}, inplace=True)

    merge_data = org_names.merge(norm_dict, how="left")
    merge_data.drop("Normalized_Index", axis=1, inplace=True)
    new_data, update_data = get_to_add(merge_data, curr_norm, ["Visit_Info_ID", "Original Treatment Name"])
    
    col_names = ", ".join(["`" + i + "`" for i in new_data.columns])
    add_new_rows(conn, sql_connect, new_data, "Normalized_Treatment_Names", col_names)
    update_tables(conn, sql_connect, ["Visit_Info_ID", "Original Treatment Name"], update_data, "Normalized_Treatment_Names")


def Normalized_Cancer(norm_table, pd, conn, sql_connect):
    table_name = "Normalized_Cancer_Dictionary"
    norm_table = norm_table[['Cancer', 'Harmonized Cancer Name', 'SEER Category']]
    norm_dict = pd.read_sql(("SELECT * FROM Normalized_Cancer_Dictionary;"),conn)
    norm_table = clean_table(norm_table)
    norm_dict = clean_table(norm_dict)

    new_data, update_data = get_to_add(norm_table, norm_dict, ["Cancer"])

    add_count = add_new_rows(conn, sql_connect, new_data, "Normalized_Cancer_Dictionary", "`Cancer`, `Harmonized Cancer Name`, `SEER Category`")
    update_count = update_tables(conn, sql_connect, ["Cancer"], update_data, "Normalized_Cancer_Dictionary")
 
    if add_count == -1 or update_count == -1:   #error updating dictionary, do not make tables
      return add_count, update_count, table_name

    org_names = pd.read_sql(("SELECT Visit_Info_ID, Cancer FROM Cancer_Cohort;"), conn)
    norm_dict = pd.read_sql(("SELECT * FROM Normalized_Cancer_Dictionary;"), conn)
    curr_norm = pd.read_sql(("SELECT * FROM Normalized_Cancer_Names_v2"), conn)

    org_names.rename(columns={"Cancer": "Original Cancer Name"}, inplace=True)
    norm_dict.rename(columns={"Cancer": "Original Cancer Name"}, inplace=True)
    
    merge_data = org_names.merge(norm_dict, how="left")
    new_data, update_data = get_to_add(merge_data, curr_norm, ["Visit_Info_ID", "Original Cancer Name"])
    
    x = new_data.query("`{0}` != `{0}`".format("Harmonized Cancer Name"))
    if len(x) > 0:
        print(f"There are {len(x)} new terms missing")

    col_names = ", ".join(["`" + i + "`" for i in new_data.columns])
    add_new_rows(conn, sql_connect, new_data, "Normalized_Cancer_Names_v2", col_names)
    update_tables(conn, sql_connect, ["Visit_Info_ID", "Original Cancer Name"], update_data, "Normalized_Cancer_Names_v2")



def add_new_rows(conn, sql_connect, new_data, table_name, col_names):
    add_count = len(new_data)
    print("There are " + str(add_count) + " new records to add to the database")
    if len(new_data) == 0:
        return
    for index in new_data.index:
        try:
            curr_data = new_data.loc[index].values.tolist()
            curr_data = ["\"" + str(s) + "\"" for s in curr_data]
            curr_data = ', '.join(curr_data)

            sql_query = (f"INSERT INTO {table_name} ({col_names}) VALUES ({curr_data})")
            sql_connect.execute(sql_query)
        except Exception as e:
            print(col_names)
            print(curr_data)
            display_error_line(e)
            print(e)
            add_count = -1
            break
        finally:
            conn.connection.commit()
    return  add_count


def update_tables(conn, sql_connect, primary_keys, update_table, sql_table):
    update_count = len(update_table)
    print("There are " + str(update_count) + " records that need to be updated")
    if len(update_table) == 0:
        return
    
    key_str = ['`' + str(s) + '`' + " like '%s'" for s in primary_keys]
    key_str = " and ".join(key_str)
    col_list = update_table.columns.tolist()
    col_list = [i for i in col_list if i not in primary_keys]

    for index in update_table.index:
        try:
            curr_data = update_table.loc[index, col_list].values.tolist()
            primary_value = update_table.loc[index, primary_keys].values.tolist()
            update_str = ["`" + i + '` = "' + str(j) + '"' for i, j in zip(col_list, curr_data)]
            update_str = ', '.join(update_str)

            update_str = update_str.replace('-1000000000', "N/A")
            update_str = update_str.replace("N/A", str(np.nan))
            update_str = update_str.replace("nan", "NULL")
            update_str = update_str.replace('"nan"', "NULL")

            update_str = update_str.replace('"NULL"',"NULL")

            sql_query = (f"UPDATE {sql_table} set {update_str} where {key_str %tuple(primary_value)}")
            sql_connect.execute(sql_query)
        except Exception as e:
            print(e)
            update_count = -1
            print(update_table)
            display_error_line(e)
        finally:
            conn.connection.commit()
    return update_count

Dictionary_Updater()