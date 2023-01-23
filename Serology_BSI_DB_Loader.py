# -*- coding: utf-8 -*-
"""
Created on Mon Nov  7 10:05:45 2022

@author: breadsp2
"""

import os
import boto3
# import urllib3
import pandas as pd
import numpy as np
from io import BytesIO
import mysql.connector

def main_func():
    ##user defined variables
    pd.options.mode.chained_assignment = None  # default='warn'
    func_name = "Seronet_BSI_to_SQL_DB"
    
    file_path = r"C:\Users\breadsp2\Documents\BSI_Reports\BSI_Report_LP003.csv"
    file_dir, file_name = os.path.split(file_path)
    study_type = "Vaccine_Response"

    try:
        print(f"## The BSI Report Uploaded is: {file_path}")
        bsi_data = pd.read_csv(file_path)

        bsi_data = bsi_data.query("`Original ID` != 'NO LABEL'")
        bsi_data = bsi_data.query("`Vial Warnings` != 'Arrived Empty; Sample vial without lid'")
        
        missing_data = bsi_data.query("`Original ID` == ''")
        missing_data["Original ID"] = missing_data["Current Label"]
        bsi_data = bsi_data.query("`Original ID` != ''")
        bsi_data = pd.concat([bsi_data, missing_data])
        
        bad_label = bsi_data.query("`Vial Warnings` == 'Barcode does not match Original ID' or " +
                                   "`Vial Warnings` == 'Do Not Distribute; Barcode does not match Original ID'")
        for index in bad_label.index:
            if bad_label.loc[index]["Original ID"] in bad_label.loc[index]["Current Label"]:
                bsi_data.loc[index]["Current Label"] = bsi_data.loc[index]["Original ID"]
    
        bsi_data.fillna("no data", inplace=True)
        parent_data = bsi_data[bsi_data["Parent ID"].apply(lambda x: str(x) in ["no data"])]
        child_data = bsi_data[bsi_data["Parent ID"].apply(lambda x: str(x) not in ["no data"])]
        bsi_parent = bsi_data[bsi_data["BSI ID"].apply(lambda x: str(x[-4:]) in ["0500"])]
        
        merge_data = parent_data.merge(bsi_parent["Original ID"], how='left', indicator=True)
        parent_data = merge_data.query("_merge not in ['both']")[parent_data.columns]
        
        merge_data = child_data.merge(bsi_parent["Original ID"], how='left', indicator=True)
        child_data = merge_data.query("_merge not in ['both']")[parent_data.columns]

        parent_data = fix_df(parent_data, 'parent')
        bsi_parent = fix_df(bsi_parent, 'parent')
        child_data = fix_df(child_data, 'child')
        
        parent_data = zero_pad_ids(parent_data, "Current Label")
        parent_data = zero_pad_ids(parent_data, "CBC_Biospecimen_Aliquot_ID")
        
        bsi_parent = zero_pad_ids(bsi_parent, "Current Label")
        bsi_parent = zero_pad_ids(bsi_parent, "CBC_Biospecimen_Aliquot_ID")

        try:
            file_dbname, conn, sql_connect = connect_to_db(study_type)
            if len(file_dbname) == 0:
                print(f"From {func_name}. File was not procesed.  Not able to connect to {study_type} DB")
                return{}
            else:
                print(f"## Sucessfully connected to {file_dbname} database ##")

            new_parent, update_parent, add_err, update_err = load_data_into_sql_DB(conn, sql_connect, parent_data, "BSI_Parent_Aliquots", ["Biorepository_ID"])
            print(f"new_parent: {new_parent}.  Update Parent: {update_parent}")
            
            new_bsi_parent, update_bsi_parent, add_err, update_err = load_data_into_sql_DB(conn, sql_connect, bsi_parent, "BSI_Parent_Aliquots", ["Biorepository_ID"])
            print(f"new_bsi_parent: {new_bsi_parent}.  Update BSI Parent: {update_bsi_parent}")
               
            new_child, update_child, add_err, update_err = load_data_into_sql_DB(conn, sql_connect, child_data, "BSI_Child_Aliquots", ["Subaliquot_ID"])
            print(f"new_child: {new_child}.  Update Child: {update_child}")

        except Exception as e:
            display_error_line(e)
        finally:
            print('## Database has been checked.  Closing the connections ##')
            if sql_connect:
                sql_connect.close()
            if conn:
                conn.close()

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


def fix_df(df, sheet_type):
    df = df.rename(columns={"Shipping ID": "Shipment_ID"})
    df = df.rename(columns={"Original ID": "CBC_Biospecimen_Aliquot_ID"})

    df["Consented_For_Research_Use"] = "Yes"
    df["Comments"] = ""
    if "Repository" in df.columns:
        df = df.drop("Repository", axis=1)

    if sheet_type == "parent":
        df = df.rename(columns={"BSI ID": "Biorepository_ID"})
        df = df.drop("Parent ID", axis=1)
    elif sheet_type == "child":
        df = df.rename(columns={"BSI ID": "Subaliquot_ID"})
        df = df.rename(columns={"Parent ID": "Biorepository_ID"})
        df = df.rename(columns={"Current Label": "CGR_Aliquot_ID"})
    return df


def zero_pad_ids(parent_data, col_str):
    z = parent_data[col_str].tolist()
    all_values = []
    for cur_val in z:
        if len(cur_val) > 15:
            if cur_val[15] in ["P", "S"]:
                cur_val = cur_val[:15]
        if len(cur_val) > 16:
            if cur_val[16] in ["P", "S"]:
                cur_val = cur_val[:16]
                
        if cur_val[-2] == '_':
            cur_val = cur_val[0:13] + "_0" + cur_val[14]
        all_values.append(cur_val)
    parent_data[col_str] = all_values
    return parent_data
    
        
def write_excel_file(parent_data, child_data, study_type):
    output = BytesIO()
    writer = pd.ExcelWriter(output, engine='xlsxwriter')
    parent_data.to_excel(writer, sheet_name="BSI_Parent_Aliquots", index=False)
    child_data.to_excel(writer, sheet_name="BSI_Child_Aliquots", index=False)
    writer.save()

    xlsx_data = output.getvalue()
    
    if study_type == "Reference_Panel":
        Output_Dest = "Biorepository_ID_Reference_Panel_map.xlsx"
    elif study_type == "Vaccine_Response":
        Output_Dest =  "Biorepository_ID_Vaccine_Response_map.xlsx"

    s3 = boto3.resource('s3')
    s3.Bucket('seronet-demo-submissions-passed').put_object(Key='Serology_Data_Files/biorepository_id_map/' + Output_Dest, Body=xlsx_data)


def connect_to_db(study_type):
    if study_type == "Reference_Panel":
        dbname = "seronetdb-Validated"
    elif study_type == "Vaccine_Response":
        dbname = "seronetdb-Vaccine_Response" 
    else:
       return []

    host_client = "seronet-dev-instance.cwe7vdnqsvxr.us-east-1.rds.amazonaws.com"
    user_name = "seronet-datauser4"
    user_password = "1ebe65925b6bc578f93a43ccdb2ff972"  # non-prod
    try:
        conn = mysql.connector.connect(user=user_name, host=host_client, password=user_password, database=dbname)
    except mysql.connector.Error as err:
        print(err)
        return []
    sql_connect = conn.cursor(prepared=True)
    return dbname, conn, sql_connect


def load_data_into_sql_DB(conn,  sql_connect, test_data, table_name, primary_keys):
    #if "CBC_Biospecimen_Aliquot_ID" in test_data.columns and "Current Label" in test_data.columns:
    #    aliquot_data = pd.read_sql(("Select Aliquot_ID FROM Aliquot"), conn)
    #    x = test_data.merge(aliquot_data, how="outer", left_on="CBC_Biospecimen_Aliquot_ID", right_on="Aliquot_ID", indicator=True)
    #    test_data = x.query("_merge in ['both']")
    
    test_data, sql_table = fix_col_names(conn, table_name, test_data)
    error_add_msg = ''
    error_update_msg = ''
    
    print(f"Ids are ready to merge into SQL DB: {table_name}")
    
    print("data to load has " + str(len(test_data)) + " rows")
    print("sql database has " + str(len(sql_table)) + " rows")

    #add_new_data_to_table(conn, sql_table, test_data)
    
    z = test_data.merge(sql_table, how="left", indicator=True)  # merge tables on all rows to elimate already exists
    new_data = z.query("_merge == 'left_only'")                 # left_only = new rows or rows needing updates
    print("Total recods to check: " + str(len(new_data)))
    update_data = []
    if len(new_data) > 0:
        new_data.drop("_merge", inplace=True, axis=1)
        merge_data = new_data.merge(sql_table[primary_keys], how="left", indicator=True)
        new_data = merge_data.query("_merge == 'left_only'")  # records that exist in new file but not in sql database
        update_data = merge_data.query("_merge == 'both'")    # records exist by primary key but other columns need updating
        
        if len(new_data) > 0:
            if "_merge" in new_data.columns:
                new_data.drop("_merge", inplace=True, axis=1)
            row_count = len(new_data)
            print(f"\n## Adding {row_count} New Rows to table: {table_name} ##\n")
            status, error_add_msg = add_new_rows(conn, sql_connect, new_data, sql_table, table_name)
            if status == "Failed":
                print("error adding new rows")
                return{}
        if len(update_data) > 0:
            row_count = len(update_data)
            print(f"\n## Updating {row_count} Rows in table: {table_name} ##\n")
            error_update_msg = update_tables(conn, sql_connect, primary_keys, update_data, table_name)
    else:
        print(f" \n## {table_name} has been checked, no data to add")
    return len(new_data), len(update_data), error_add_msg, error_update_msg


def fix_col_names(conn, sql_table, filt_table):
    sql_data = pd.read_sql((f"Select * FROM {sql_table}"), conn)
    sql_data.fillna("no data", inplace=True)
    sql_data.fillna("no data", inplace=True)

    new_cols = [i for i in filt_table.columns.tolist() if i in sql_data.columns.tolist()]
    filt_table = filt_table[new_cols]
    filt_table.drop_duplicates(inplace=True)
    filt_table = correct_data(filt_table)
    sql_data = correct_data(sql_data)
    return filt_table, sql_data


def correct_data(df):
    df = df.astype(str)
    df.replace({"None": "N/A", np.nan: "N/A", 'nan': "N/A", '': "N/A"}, inplace=True)
    for curr_col in df.columns:
        if curr_col == "Volume":
            pass
        else:
            df[curr_col] = df[curr_col].replace("\.0", "", regex=True)  # only works on strings
    return df
    

def add_new_rows(conn, sql_connect, new_data, sql_data, table_name):
    new_data = new_data[sql_data.columns]
    error_msg = ''

    for index in new_data.index:
        status = "Passed"
        try:
            curr_data = new_data.loc[index].values.tolist()
            curr_data = ["'" + str(s) + "'" for s in curr_data]
            curr_data = ', '.join(curr_data)

            sql_query = (f"INSERT INTO {table_name} VALUES ({curr_data})")
            sql_connect.execute(sql_query)
        except Exception as e:
            error_msg = e
            print(curr_data)
            status = "Failed"
            break
        finally:
            conn.commit()
    return status, error_msg

    
def update_tables(conn, sql_connect, primary_keys, update_table, sql_table):
    key_str = ['`' + str(s) + '`' + " like '%s'" for s in primary_keys]
    key_str = " and ".join(key_str)
    update_table.drop("_merge", inplace=True, axis=1)
    col_list = update_table.columns.tolist()
    col_list = [i for i in col_list if i not in primary_keys]

    error_msg = ''
    for index in update_table.index:
        try:
            curr_data = update_table.loc[index, col_list].values.tolist()
            primary_value = update_table.loc[index, primary_keys].values.tolist()
            update_str = ["`" + i + "` = '" + str(j) + "'" for i, j in zip(col_list, curr_data)]
            update_str = ', '.join(update_str)

            update_str = update_str.replace("'no data'", "NULL")

            sql_query = (f"UPDATE {sql_table} set {update_str} where {key_str %tuple(primary_value)}")
            sql_connect.execute(sql_query)
        except Exception as e:
            print(e)
            error_msg = e
            break
        finally:
            conn.commit()
    return error_msg


main_func()