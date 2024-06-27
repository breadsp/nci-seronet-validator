# -*- coding: utf-8 -*-
"""
Created on Mon Nov  7 10:05:45 2022

@author: breadsp2
"""

from import_loader_v2 import pd, sd, np, os
from connect_to_sql_db import connect_to_sql_db
from dateutil.parser import parse

def main_func():
    ##user defined variables
    pd.options.mode.chained_assignment = None  # default='warn'
    func_name = "Seronet_BSI_to_SQL_DB"
    
    file_path = r"C:\Users\breadsp2\Documents\BSI_Reports\BSI_Report_LP003.csv"
    #file_path = r"C:\Users\breadsp2\Documents\BSI_Reports\BSI_Report_LP003_child.csv"
    #file_path = r"C:\Users\breadsp2\Documents\BSI_Reports\BSI_Report_LP002.csv"
    file_dir, file_name = os.path.split(file_path)

    if "LP003" in file_path:
        study_type = "Vaccine_Response"
    elif "LP002" in file_path:
        study_type = "Reference_Panel"
    else:
        return
    
    if study_type == "Reference_Panel":
        file_dbname = "seronetdb-Validated"
    elif study_type == "Vaccine_Response":
        file_dbname = "seronetdb-Vaccine_Response"

    try:
        print(f"## The BSI Report Uploaded is: {file_path}")
        bsi_data = pd.read_csv(file_path)

        bsi_data = bsi_data.query("`Original ID` != 'NO LABEL'")
        bsi_data = bsi_data.query("`Vial Warnings` != 'Arrived Empty; Sample vial without lid'")
        
        missing_data = bsi_data.query("`Original ID` == '' or `Original ID` != `Original ID`")
        missing_data["Original ID"] = missing_data["Current Label"]
        bsi_data = bsi_data.query("`Original ID` != ''")
        bsi_data = pd.concat([bsi_data, missing_data])

        bsi_data = bsi_data.sort_values(["Current Label", "Date Entered"])
        bsi_data = bsi_data.drop_duplicates(["Current Label", "BSI ID"], keep="last")
        
        bsi_data = bsi_data.drop_duplicates(["Current Label"], keep="last")

        x = bsi_data.query("`Vial Status` == 'Empty'")
        bsi_data.loc[x.index, "Volume"] = 0

        #remove samples that appear in bsi but were returned to cbcs
        bsi_data = bsi_data.query("`Last Requisition ID` not in ('R2024:000098', 'R2023:000021', 'R2022:001109')")
        
        bad_label = bsi_data.query("`Vial Warnings` == 'Barcode does not match Original ID' or " +
                                   "`Vial Warnings` == 'Do Not Distribute; Barcode does not match Original ID'")
    
        for index in bad_label.index:
            if bad_label.loc[index]["Original ID"] in bad_label.loc[index]["Current Label"]:
                bsi_data.loc[index]["Current Label"] = bsi_data.loc[index]["Original ID"]
    
        bsi_data.fillna("no data", inplace=True)
        bsi_data["vial_code"] = [i[:2] for i in bsi_data["Current Label"]]

        bsi_data.rename(columns={"Date Vial Modified": "Date Last Modified"}, inplace=True)

        cgr_child_data = bsi_data[bsi_data["Parent ID"].apply(lambda x: str(x) not in ["no data"])]
        fnl_child_data = bsi_data.query("`Parent ID` in ['no data'] and vial_code in ['FR','FS', 'FD']")
        child_data = pd.concat([cgr_child_data, fnl_child_data])
        
        parent_data = bsi_data.query("`Parent ID` in ['no data'] and vial_code in ['14', '27', '32', '41']")
        bsi_parent = bsi_data[bsi_data["BSI ID"].apply(lambda x: str(x[-4:]) in ["0500"])]
        
        merge_data = parent_data.merge(bsi_parent["Original ID"], how='left', indicator=True)
        parent_data = merge_data.query("_merge not in ['both']")[parent_data.columns]
        
        merge_data = child_data.merge(bsi_parent["Original ID"], how='left', indicator=True)
        child_data = merge_data.query("_merge not in ['both']")[parent_data.columns]

        bsi_parent["Current Label"] = bsi_parent["Original ID"]
        parent_data = pd.concat([parent_data, bsi_parent])
        parent_data.reset_index(drop=True, inplace=True)


        parent_data = fix_df(parent_data, 'parent')
        #bsi_parent = fix_df(bsi_parent, 'parent')
        child_data = fix_df(child_data, 'child')
        
        parent_data = zero_pad_ids(parent_data, "Current Label")
        #parent_data = zero_pad_ids(parent_data, "Original ID")
        parent_data = zero_pad_ids(parent_data, "CBC_Biospecimen_Aliquot_ID")
        
        #bsi_parent = zero_pad_ids(bsi_parent, "Current Label")
        #bsi_parent = zero_pad_ids(bsi_parent, "CBC_Biospecimen_Aliquot_ID")

        child_data = child_data.query("`Vial Status` not in ['Empty']")
        
        x = child_data.query("Biorepository_ID == 'no data'")
        child_data["Biorepository_ID"][x.index] = [i[:7] + " 0001" for i in x["Subaliquot_ID"]]
        
        duplicate_ids = ['14_M54994_201_01','14_M56639_101_01','14_M62513_102_01','14_M80127_102_01','27_200011_301_03','27_200011_401_02','27_200318_301_02',
                            '27_200318_301_03','27_201239_301_02','27_201239_301_03','27_201314_306_02','27_201314_306_03','27_202030_301_02','27_202030_301_03','27_300018_401_01',
                            '27_300018_401_02','27_300040_301_02','27_300040_301_03','27_300042_300_02','27_300042_300_03','27_300042_312_02','27_300082_400_02','27_300082_400_03',
                            '27_300098_301_02','27_300098_301_03','27_300589_301_03','27_300661_301_03','27_300742_301_03','27_300927_301_02','27_300927_301_03','27_301380_312_02',
                            '27_301380_312_03','27_400047_301_02','27_400047_301_03','27_400226_301_02','27_400226_301_03','27_402245_301_02','27_402245_301_03','27_410035_306_03',
                            '27_414619_306_02','27_414619_306_03','27_414961_306_02','27_414961_306_03','27_414992_306_03','27_490019_306_03','27_500024_301_02','27_500024_301_03',
                            '27_500323_306_02','27_500323_306_03','27_500754_306_02','27_600112_301_03','27_600251_301_03','27_600391_306_02','27_600391_306_03','27_601493_301_03',
                            '27_610010_306_02','27_610010_306_03','27_700926_401_02','27_801209_301_03','32_221075_104_01','32_221075_104_02','32_441079_303_01']

        try:
            sql_column_df, engine, conn = connect_to_sql_db(pd, sd, file_dbname)
            if len(file_dbname) == 0:
                print(f"From {func_name}. File was not procesed.  Not able to connect to {study_type} DB")
                return{}
            else:
                print(f"## Sucessfully connected to {file_dbname} database ##")

            new_parent, update_parent, add_err, update_err = load_data_into_sql_DB(conn, engine, parent_data, "BSI_Parent_Aliquots", ["Biorepository_ID"], dup_list = duplicate_ids, relabel=True)
            print(f"new_parent: {new_parent}.  Update Parent: {update_parent}")
            
            #new_bsi_parent, update_bsi_parent, add_err, update_err = load_data_into_sql_DB(conn, engine, bsi_parent, "BSI_Parent_Aliquots", ["Biorepository_ID"])
            #print(f"new_bsi_parent: {new_bsi_parent}.  Update BSI Parent: {update_bsi_parent}")
               
            new_child, update_child, add_err, update_err = load_data_into_sql_DB(conn, engine, child_data, "BSI_Child_Aliquots", ["Subaliquot_ID"])
            print(f"new_child: {new_child}.  Update Child: {update_child}")
            
            #these ids exist multiple times in BSI. not able to use these samples

            
        

        except Exception as e:
            display_error_line(e)
        finally:
            print('## Database has been checked.  Closing the connections ##')
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

    df["Consented_For_Research_Use"] = "Yes"
    df["Comments"] = ""
    if "Repository" in df.columns:
        df = df.drop("Repository", axis=1)

    if sheet_type == "parent":
        try:
            df = df.rename(columns={"Original ID": "CBC_Biospecimen_Aliquot_ID"})
            df = df.rename(columns={"BSI ID": "Biorepository_ID"})
            df = df.drop("Parent ID", axis=1)
        except Exception as e:
            print(e)
    elif sheet_type == "child":
        df = df.rename(columns={"BSI ID": "Subaliquot_ID"})
        df = df.rename(columns={"Parent ID": "Biorepository_ID"})
        df = df.rename(columns={"Current Label": "CGR_Aliquot_ID"})
    return df


def zero_pad_ids(parent_data, col_str):
    try:
        z = parent_data[col_str].tolist()
    except Exception as e:
        print(e)
    all_values = []
    for cur_val in z:
        try:
            if len(cur_val) > 15:
                if cur_val[15] in ["P", "S"]:
                    cur_val = cur_val[:15]
            if len(cur_val) > 16:
                if cur_val[16] in ["P", "S"]:
                    cur_val = cur_val[:16]
            if cur_val[-2] == '_':
                cur_val = cur_val[0:13] + "_0" + cur_val[14]
            all_values.append(cur_val)
        except Exception:
            all_values.append(cur_val)
            print(f"{cur_val} failed zero pad")
        finally:
            continue
    parent_data[col_str] = all_values
    return parent_data


def load_data_into_sql_DB(conn,  engine, test_data, table_name, primary_keys, **kwargs):
    test_data, sql_table = fix_col_names(conn, table_name, test_data, engine)
    sql_table.fillna("no data", inplace=True)

    test_data.fillna("no data", inplace=True)
    error_add_msg = ''
    error_update_msg = ''

    x = sql_table.query("`Vial Status` == 'Empty' and Volume == 'no data'")
    sql_table["Volume"][x.index] = 0
    try:
        sql_table["Volume"] = [float(i) for i in sql_table["Volume"]]
        x = test_data.query("`Vial Status` == 'Empty' and Volume in ['no data', 0, '0.0']")
        test_data["Volume"][x.index] = 0
        test_data["Volume"].replace('no data', 0, inplace=True)
        test_data["Volume"] = [float(i) for i in test_data["Volume"]]
    except Exception as e:
        print(e)

    print(f"Ids are ready to merge into SQL DB: {table_name}")
    
    print("data to load has " + str(len(test_data)) + " rows")
    print("sql database has " + str(len(sql_table)) + " rows")

    #add_new_data_to_table(conn, sql_table, test_data)
    if table_name == "BSI_Parent_Aliquots" and "Date Entered" in test_data.columns:
        test_data["Date Entered"] = [parse(i).date().strftime("%Y-%m-%d") for i in test_data["Date Entered"]]
    
    if table_name == "BSI_Child_Aliquots" and 'Date Received' in test_data.columns:
        test_data['Date Received'] = [parse(i).date().strftime("%Y-%m-%d") for i in test_data["Date Received"]]
        
    if 'Date Last Modified' in test_data.columns:
        test_data['Date Last Modified'] = [parse(i).date().strftime("%Y-%m-%d") for i in test_data["Date Last Modified"]]
    
    if table_name == "BSI_Child_Aliquots":
        try:
            y = test_data.query("`Volume Unit` == 'ml (cc)'")
            test_data["Volume"][y.index] = y["Volume"]*1000  #convert ml to ul
            test_data["Volume Unit"][y.index] = 'ul'
            
            test_data["Volume"] = [float(i) for i in test_data["Volume"]]
        except Exception:
            pass
    
    sql_table = sql_table.query("Volume not in ['no data']")
    sql_table["Volume"] = [float(i) for i in sql_table["Volume"]]
    
    sql_table["Vial Warnings"] = [i.replace("n't", "nt") for i in sql_table["Vial Warnings"]]
    
    test_data["Volume"] = [0 if i == 'no data' else float(i) for i in test_data["Volume"]]
    test_data["Vial Warnings"] = [i.replace("n't", "nt") for i in test_data["Vial Warnings"]]
    
    try:
        if primary_keys == ["Biorepository_ID"]:
            ali_data = pd.read_sql(("SELECT Aliquot_ID FROM Aliquot"), conn)
            try:
                test_data = test_data.merge(ali_data, how="left", indicator=True, left_on="Current Label", right_on="Aliquot_ID")
            except:
                test_data = test_data.merge(ali_data, how="left", indicator=True, left_on='CBC_Biospecimen_Aliquot_ID', right_on="Aliquot_ID")
            x = test_data.query("_merge == 'left_only'")
            x["Vial Warnings"].replace("no data", "", inplace=True)
            test_data["Vial Warnings"][x.index] = [i + " Discrepency: No Aliquot Data" for i in x["Vial Warnings"]]
            test_data["Vial Status"][x.index] = 'Discrepant'
            test_data.drop(["Aliquot_ID", "_merge"], axis=1, inplace=True)
        
        z = test_data.merge(sql_table, how="left", indicator=True)  # merge tables on all rows to elimate already exists
    except Exception as e:
        print(e)
    new_data = z.query("_merge == 'left_only'")                 # left_only = new rows or rows needing updates
    print("Total records to check: " + str(len(new_data)))
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
            status, error_add_msg = add_new_rows(conn, engine, new_data, sql_table, table_name)
            if status == "Failed":
                print("error adding new rows")
                return{}
        if len(update_data) > 0:
            update_data.drop("_merge", inplace=True, axis=1)
            merge_data = update_data.merge(sql_table, how="left", indicator=True)
            update_data = merge_data.query("_merge == 'left_only'") 
            
            row_count = len(update_data)
            print(f"\n## Updating {row_count} Rows in table: {table_name} ##\n")
            error_update_msg = update_tables(conn, engine, primary_keys, update_data, table_name)
    else:
        print(f" \n## {table_name} has been checked, no data to add")
        
    if len(kwargs) == 1: #update duplicate IDs in database
        dup_ids = kwargs["dup_list"]
        dup_ids =  "'" + "','".join(dup_ids) + "'"
        
        sql_qry = f"update {table_name} set `Vial Status` = 'Duplicate' where `Current Label` in ({dup_ids})"
        engine.execute(sql_qry)
        conn.connection.commit()
        
    return len(new_data), len(update_data), error_add_msg, error_update_msg


def fix_col_names(conn, sql_table, filt_table, sql_connect):
    sql_data = pd.read_sql((f"Select * FROM {sql_table}"), conn)
    sql_data.fillna("no data", inplace=True)
    sql_data.fillna("no data", inplace=True)

    #if 'Current Label' in sql_data.columns:
    #    x = sql_data[sql_data.duplicated('Current Label', keep=False)]
    #    x.sort_values(['Current Label', 'Date Last Modified'], inplace=True)
    #    x.drop_duplicates('Current Label', keep = 'first', inplace=True)
    #    x2 = x[x['Biorepository_ID'].str.contains('LP60122', regex=False)]

    #for idx in x2.index:
    #    repo_id = x2["Biorepository_ID"][idx]
    #    try:
    #        sql_connect.execute(f"DELETE FROM `seronetdb-Vaccine_Response`.`BSI_Parent_Aliquots` WHERE (`Biorepository_ID` = '{repo_id}')")
    #        conn.connection.commit()
    #    except Exception as e:
    #        print(e)

    if "Date Received" in sql_data.columns:
        filt_table.rename(columns = {"Date Entered": "Date Received"}, inplace=True)

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
            sql_query = sql_query.replace("n't", "nt")
            sql_query = sql_query.replace("'no data'", "NULL")
            sql_connect.execute(sql_query)
        except Exception as e:
            error_msg = e
            #print(curr_data)
            #status = "Failed"
            continue
        finally:
            conn.connection.commit()
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

            update_str = update_str.replace("n't", "nt")
            update_str = update_str.replace("'no data'", "NULL")

            sql_query = (f"UPDATE {sql_table} set {update_str} where {key_str %tuple(primary_value)}")
            sql_connect.execute(sql_query)
        except Exception as e:
            print(e)
            error_msg = e
            break
        finally:
            conn.connection.commit()
    return error_msg


main_func()