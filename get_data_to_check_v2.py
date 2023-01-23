# -*- coding: utf-8 -*-
import shutil


def get_summary_file(os, pd, root_dir, file_sep, s3_client, s3_resource, summary_path, file_path):
    if os.path.exists(summary_path):
        summary_file = pd.read_excel(summary_path)
    else:
        summary_file = pd.DataFrame(columns=["Submission_Status", "Date_Of_Last_Status", "Folder_Location",
                                             "CBC_Num", "Date_Timestamp", "Submission_Name", "Validation_Status"])

    get_submissions_to_check(os, s3_client, s3_resource, pd, root_dir, "cbc01", "Feinstein_CBC01", file_path)
    get_submissions_to_check(os, s3_client, s3_resource, pd, root_dir, "cbc02", "UMN_CBC02", file_path)
    get_submissions_to_check(os, s3_client, s3_resource, pd, root_dir, "cbc03", "ASU_CBC03", file_path)
    get_submissions_to_check(os, s3_client, s3_resource, pd, root_dir, "cbc04", "Mt_Sinai_CBC04", file_path)
    return summary_file


def get_submissions_to_check(os, s3_client, s3_resource, pd, root_dir, cbc_num, full_cbc, file_path):
    file_sep = os.path.sep
    bucket_name = "nci-seronet-cbc-destination"
    test_list = ["09-13-36-06-22-2021", "14-58-48-03-26-2021", "16-13-43-04-01-2021", "10-03-08-06-11-2021",
                 "15-43-32-06-10-2021", "12-37-48-04-12-2021", "15-54-47-05-06-2021", "12-11-00-06-11-2021",
                 "15-45-38-05-06-2021", "15-48-31-05-06-2021", "15-51-30-05-06-2021", "14-22-18-10-22-2021",
                 "13-09-54-09-09-2021", "10-10-11-07-27-2021", "10-42-55-02-18-2022", "09-53-37-02-18-2022"]

    folders_to_process = get_s3_folders(s3_client, pd, bucket_name, prefix=cbc_num, suffix='.zip')
    folder_list = folders_to_process.query("S3_Date not in @test_list")
    folder_list = folder_list.query("CBC_Name not in ['cbc-test-prod']")
    folder_list.rename(columns={"CBC_Name": "Org_CBC_Name"}, inplace=True)

    pass_list = get_s3_folders(s3_client, pd, "nci-cbiit-seronet-submissions-passed", prefix=full_cbc, suffix='.zip')
    fail_list = get_s3_folders(s3_client, pd, "nci-cbiit-seronet-submissions-failed", prefix=full_cbc, suffix='.zip')

    downloaded_folders = get_curr_subfolder(os, root_dir, file_path, full_cbc, file_sep)
    if len(downloaded_folders) > 0:
        subs = [i for i in downloaded_folders if (i in fail_list["S3_Date"].tolist()) or
                i in pass_list["S3_Date"].tolist()]
        [shutil.rmtree(root_dir + file_sep + "Files_To_Validate" + file_sep + full_cbc + file_sep + i) for i in subs]

    if len(pass_list) > 0:
        folder_list = folder_list.merge(pass_list, on=["S3_Date", "File_Name"], how="left", indicator="Sub_To_Pass")
    if len(fail_list) > 0:
        folder_list = folder_list.merge(fail_list, on=["S3_Date", "File_Name"], how="left", indicator="Sub_To_Fail")

    folder_list = folder_list.query("Sub_To_Pass not in ['both'] and Sub_To_Fail not in ['both']")
    folder_list = folder_list[["Org_CBC_Name", "S3_Date", "File_Name"]]

    curr_dir = os.getcwd()
    for index in folder_list.values:
        resp = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=(index[0] + "/" + index[1]))
        list_of_files = [i["Key"] for i in resp["Contents"]]
        if True in ["Accrual_Participant_Info.csv" in i for i in list_of_files]: #accrual submission
            for curr_file in list_of_files:
                new_file_name = rename_list(curr_file)
                s3_resource.Object("nci-cbiit-seronet-submissions-passed", new_file_name).copy_from(CopySource={'Bucket': bucket_name,'Key' : curr_file})
        else:
            for curr_file in resp["Contents"]:
                try:
                    os.chdir(root_dir + os.path.sep + "Files_To_Validate")
                    path, name = os.path.split(curr_file["Key"])
                    create_sub_folders(os, path)
                    os.chdir(root_dir + os.path.sep + "Files_To_Validate" + os.path.sep + path)
                    if len(name) > 0:
                        s3_client.download_file(bucket_name, curr_file["Key"], name)
                except Exception as e:
                    print("no file found in this path")
        os.chdir(curr_dir)

def rename_list(curr_file):
    curr_file = curr_file.replace("cbc01", "Feinstein_CBC01")
    curr_file = curr_file.replace("cbc02", "UMN_CBC02")
    curr_file = curr_file.replace("cbc03", "ASU_CBC03")
    curr_file = curr_file.replace("cbc04", "Mt_Sinai_CBC04")
    curr_file = "Monthly_Accrual_Reports/" + curr_file
    return curr_file

def get_curr_subfolder(os, root_dir, folder_name, cbc_name, file_sep):
    file_path = root_dir + file_sep + folder_name + file_sep + cbc_name
    if os.path.exists(file_path):
        sub_folders = os.listdir(file_path)
    else:
        sub_folders = []
    return sub_folders


def get_s3_folders(s3, pd, bucket, prefix, suffix):
    sub_folders = ["Submissions_in_Review/", "Reference Pannel Submissions/", "Vaccine Response Submissions/", "Monthly_Accrual_Reports/"]
    key_list = []
    if "nci-seronet-cbc-destination" in bucket:
        sub_folders = " "
    for curr_folder in sub_folders:
        if sub_folders == " ":
            new_prefix = prefix
        else:
            new_prefix = curr_folder + prefix
        resp = s3.list_objects_v2(Bucket=bucket, Prefix=new_prefix)
        if 'Contents' in resp:
            for obj in resp['Contents']:
                key = obj['Key']
                if key.endswith(suffix):
                    key_list.append(key)
    new_list = [i.split("/") for i in key_list]
    if "nci-seronet-cbc-destination" in bucket:
        z = [[i[0], i[1], i[-1]] for i in new_list]
    else:
        z = [[i[1], i[2], i[-1]] for i in new_list]
    z = pd.DataFrame(z)
    z.columns = ["CBC_Name", "S3_Date", "File_Name"]
    z = z.query("'Data_Validation_Results.zip' not in File_Name")
    z.drop_duplicates(inplace=True)
    return z


def create_sub_folders(os, folder_name):
    if not os.path.exists(folder_name):
        os.makedirs(folder_name, mode=0o666)
