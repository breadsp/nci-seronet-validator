from import_loader import *


def bio_repo_map(s3_client, s3_resource, study_type):
    bucket = "nci-cbiit-seronet-submissions-passed"
    curr_file = "Serology_Data_Files/biorepository_id_map/BSI output/"

    s3_bucket = s3_resource.Bucket(bucket)
    files = list(s3_bucket.objects.filter(Prefix=curr_file))
    curr_file = get_recent_date(files, study_type)

    print(f"## Latest BSI Report is {curr_file} ##")
    bsi_data = pd_s3.get_df_from_keys(s3_client, bucket, curr_file, suffix="csv", format="csv",
                                      na_filter=True, output_type="pandas")

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

    bsi_data.fillna('', inplace=True)
    parent_data = bsi_data.query("`Parent ID` == ''")
    child_data = bsi_data.query("`Parent ID` != ''")

    parent_data = fix_df(parent_data, 'parent')
    child_data = fix_df(child_data, 'child')

    Output_Dest = "Serology_Data_Files/biorepository_id_map/"

    dup_df = pd.DataFrame(columns=["Column_Name", "Column_Value", "Frequency"])
    dup_df = check_for_dups(pd, dup_df, bsi_data, "Original ID")
    dup_df = check_for_dups(pd, dup_df, parent_data, "CBC_Biospecimen_Aliquot_ID")
    dup_df = check_for_dups(pd, dup_df, parent_data, "Biorepository_ID")
    dup_df = check_for_dups(pd, dup_df, child_data, "CGR_Aliquot_ID")
    dup_df = check_for_dups(pd, dup_df, child_data, "Subaliquot_ID")

    parent_data = parent_data.query("CBC_Biospecimen_Aliquot_ID != ''")

    z = parent_data["Current Label"].tolist()
    z = [i[0:13] + "_0" + i[14] if i[-2] == '_' else i for i in z]
    parent_data["Current Label"] = z

    z = parent_data["CBC_Biospecimen_Aliquot_ID"].tolist()
    z = [i[0:13] + "_0" + i[14] if i[-2] == '_' else i for i in z]
    parent_data["CBC_Biospecimen_Aliquot_ID"] = z

    if len(dup_df) > 0:
        print("dup issues found?")
        parent_data.drop_duplicates("CBC_Biospecimen_Aliquot_ID", inplace=True)
        parent_data.drop_duplicates("Biorepository_ID", inplace=True)
        child_data.drop_duplicates("CGR_Aliquot_ID", inplace=True)
        child_data.drop_duplicates("Subaliquot_ID", inplace=True)

    temp_location = "C:\\Python_Code\\Test_File.xlsx"

    writer = pd.ExcelWriter(temp_location, engine='xlsxwriter')
    parent_data.to_excel(writer, sheet_name="BSI_Parent_Aliquots", index=False)
    child_data.to_excel(writer, sheet_name="BSI_Child_Aliquots", index=False)

    writer.save()
    writer.close()
    writer.handles = None

    if study_type == "Refrence_Pannel":
        Output_Dest = Output_Dest + "Biorepository_ID_Reference_Panel_map.xlsx"
    elif study_type == "Vaccine_Response":
        Output_Dest = Output_Dest + "Biorepository_ID_Vaccine_Response_map.xlsx"

    s3_resource.meta.client.upload_file(temp_location, bucket, Output_Dest)
    return dup_df


def get_recent_date(files, study_type):
    if study_type == "Refrence_Pannel":
        key = "BSI_Report_LP002"
    elif study_type == "Vaccine_Response":
        key = "BSI_Report_LP003"

    curr_date = (files[0].last_modified).replace(tzinfo=None)
    index = -1
    for file in files:
        index = index + 1
        if key in file.key:
            if (file.last_modified).replace(tzinfo=None) > curr_date:
                curr_date = (file.last_modified).replace(tzinfo=None)
                curr_value = index
    return files[curr_value].key


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


def check_for_dups(pd, dup_df, data_table, field_name):
    if field_name == "Original ID":
        data_table = data_table.query("`Parent ID` == ''")

    table_counts = data_table[field_name].value_counts(dropna=False).to_frame()
    curr_dups = table_counts[table_counts[field_name] > 1]

    if len(curr_dups) > 0:
        curr_dups = curr_dups.reset_index()
        curr_dups["Column_Name"] = field_name
        curr_dups.columns = ["Column_Value", "Frequency", "Column_Name"]
        curr_dups = curr_dups[["Column_Name", "Column_Value", "Frequency"]]
        dup_df = pd.concat([dup_df, curr_dups])
    return dup_df

