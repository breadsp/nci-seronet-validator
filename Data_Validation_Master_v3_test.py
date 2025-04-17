from import_loader_v2 import time, pd, sd, os, re, colored, pd_s3, pathlib, cprint, shutil, datetime, parse
from import_loader_v2 import set_up_function, get_template_data, get_box_data_v2
import hashlib

from get_data_to_check_v2 import get_summary_file
import db_loader_ref_pannels
# import db_loader_vac_resp
import db_loader_v4
#from bio_repo_map import bio_repo_map

from connect_to_sql_db import connect_to_sql_db
from File_Submission_Object_v2 import Submission_Object
import Validation_Rules_v2 as vald_rules

import Update_Participant_Info
import rename_cohorts
import vaccine_resp_time_line_v2
#########################################################################################hhh####
#  import templates abd CBC codes directly from box
#  connect to S3 client and return handles for future use
start_time = time.time()
print("## Running Set Up Functions")
file_sep, s3_client, s3_resource, Support_Files, validation_date, box_dir = set_up_function()
#study_type = "Reference_Pannel"
study_type = "Vaccine_Response"

template_df, dbname = get_template_data(pd, box_dir, file_sep, study_type)
print("Initialization took %.2f seconds" % (time.time() - start_time))


def Data_Validation_Main(study_type):
    if len(template_df) == 0:
        print("Study Name was not found, please correct")
        return

    root_dir = "C:\\Seronet_Data_Validation"  # Directory where Files will be downloaded
    ignore_validation_list = ["submission.csv", "assay.csv", "assay_target.csv", "baseline_visit_date.csv"]
    check_BSI_tables = False
    make_rec_report = False
    upload_ref_data = False
    bucket = "nci-cbiit-seronet-submissions-passed"
##############################################################################################
    start_time = time.time()
    print("Connection to SQL database took %.2f seconds" % (time.time() - start_time))
    if study_type == "Vaccine_Response":
        sql_tuple = connect_to_sql_db(pd, sd, "seronetdb-Vaccine_Response_v2")
    elif study_type == "Reference_Pannel":
        sql_tuple = connect_to_sql_db(pd, sd, "seronetdb-Validated")
        
    #new_cohort = pd.read_excel(r"C:\Users\breadsp2\Documents\update_cohort_table.xlsx")
    #new_cohort["Primary_Cohort"] = new_cohort["Primary_Cohort"].apply(str)
    #sql_data = pd.read_sql(("SELECT * FROM `seronetdb-Vaccine_Response_v2`.Participant_Cohort"),sql_tuple[2])
    #new_cohort = new_cohort.merge(sql_data, how="left", indicator = True)
    #new_cohort = new_cohort.query("_merge not in ['both']")
    #new_cohort.drop("_merge", axis=1, inplace=True)

    if upload_ref_data is True and study_type == "Reference_Panel":
        #db_loader_ref_pannels.write_panel_to_db(sql_tuple, s3_client, bucket)
        db_loader_ref_pannels.write_requests_to_db(sql_tuple, s3_client, bucket)
        db_loader_ref_pannels.make_manifests(sql_tuple, s3_client, s3_resource, bucket)
        return

    if make_rec_report is True:
        generate_rec_report(sql_tuple, s3_client, bucket)
###############################################################################################
#    if check_BSI_tables is True:
#        check_bio_repo_tables(s3_client, s3_resource, study_type)  # Create BSI report using file in S3 bucket
###############################################################################################
    if study_type == "Reference_Pannel":
        # sql_table_dict = db_loader_ref_pannels.Db_loader_main(sql_tuple, validation_date)
                                                     
        
        sql_table_dict = db_loader_v4.Db_loader_main("Reference Pannel Submissions", sql_tuple, validation_date,
                                                     Update_Assay_Data=False, Update_BSI_Tables=check_BSI_tables,
                                                     add_serology_data=False, Add_Blinded_Results=True,
                                                     update_CDC_tables=False)
    elif study_type == "Vaccine_Response":
        sql_table_dict = db_loader_v4.Db_loader_main("Vaccine Response Submissions", sql_tuple, validation_date,
                                                     Update_Assay_Data=False, Update_Study_Design=False, Update_BSI_Tables=False)
#############################################################################################
# compares S3 destination to S3-Passed and S3-Failed to get list of submissions to work
    try:
        summary_path = root_dir + file_sep + "Downloaded_Submissions.xlsx"
        summary_file = get_summary_file(os, pd, root_dir, file_sep, s3_client, s3_resource, summary_path, "Files_To_Validate")
#############################################################################################
# pulls the all assay data directly from box
        start_time = time.time()
        assay_data = pd.read_sql(("Select * from Assay_Metadata"), sql_tuple[1])
        assay_target = pd.read_sql(("Select * from Assay_Target"), sql_tuple[1])
        #assay_data, assay_target, all_qc_data, converion_file = get_box_data_v2.get_assay_data("CBC_Data")
   #     study_design = pd.read_sql(("Select * from Study_Design"), sql_tuple[1])
   #     study_design.drop("Cohort_Index", axis=1, inplace=True)
        #get_box_data_v2.get_study_design()

        print("\nLoading Assay Data took %.2f seconds" % (time.time()-start_time))
############################################################################################
        if study_type == "Refrence_Pannel":
            #check_serology_submissions(s3_client, colored, bucket, assay_data, assay_target)
            vald_rules.check_serology_shipping(pd_s3, pd, colored, s3_client, bucket, sql_tuple)
############################################################################################
# Creates Sub folders to place submissions in based on validation results
        start_time = time.time()
        create_sub_folders(root_dir, ["01_Failed_File_Validation", "02_Passed_Data_Validation",
                                      "03_Column_Errors", "04_Failed_Data_Validation"])

        summary_file = check_for_typo(summary_file)          # checks for typoes in submission status and corrects
        summary_file = move_updated(summary_file, root_dir)  # moves submissions to approtiate folder
        CBC_Folders = get_subfolder(root_dir, "Files_To_Validate")  # List of all CBC names to check
        print("\nGetting Files to Work took %.2f seconds" % (time.time()-start_time))
#############################################################################################
        if len(CBC_Folders) == 0:
            print("\nThe Files_To_Validate Folder is empty, no Submissions Downloaded to Process\n")
        else:
            rename_CBC_folders(root_dir, CBC_Folders)   # flip CBC numbers into CBC names
            CBC_Folders = get_subfolder(root_dir, "Files_To_Validate")
            CBC_Folders = sort_CBC_list(CBC_Folders)
#############################################################################################
        #bio_data = pd.read_sql(("select Biospecimen_ID, Biospecimen_Type from Biospecimen"), sql_tuple[2])

        for iterT in CBC_Folders:
            file_list = listdirs(iterT, [])
            cbc_name = pathlib.PurePath(iterT).name
            cprint("\n##    Starting Data Validation for " + cbc_name + "    ##", color='yellow', attrs=['bold'])
            if len(file_list) == 0:
                print("There are no submitted files for " + cbc_name)
                clear_dir(iterT)
                continue
            file_list = check_if_done(file_list)
            for curr_file in file_list:
                if "intake.csv" == curr_file:  #this is an accural file, do not check
                    continue
                file_path, file_name = os.path.split(curr_file)         # gets file path and file name
                curr_date = os.path.split(file_path)                    # trims date from file path
                list_of_folders = os.listdir(curr_file)
                result_message = check_passed_file_validation(os, curr_file, root_dir)
                if result_message == "File Failed File-Validation":
                    move_target_folder(curr_date, file_sep, file_path, "01_Failed_File_Validation")
                    #  update excel file
                    continue
                if "UnZipped_Files" in list_of_folders:
                    list_of_files = os.listdir(curr_file + file_sep + "Unzipped_Files")
                else:
                    print("There are no files found within this submission to process")
                    move_target_folder(curr_date, file_sep, file_path, "01_Failed_File_Validation")
                    #  update excel file
                    continue

                if "submission" in file_name:
                    current_sub_object = Submission_Object(file_name[15:])  # creates the Object
                else:
                    current_sub_object = Submission_Object(file_name)  # creates the Object
                try:
                    current_sub_object.initalize_parms(os, shutil, curr_file, template_df,
                                                       sql_tuple[0], sql_table_dict)
                except PermissionError:
                    print("One or more files needed is open, not able to proceed")
                    continue

                print("\n## Starting the Data Validation Proccess for " + current_sub_object.File_Name + " ##")

                try:
                    current_sub_object, study_name = populate_object(current_sub_object, curr_file, list_of_files,
                                                                     Support_Files, study_type)
                    if study_name == "Accrual_Reports":
                        print("##  Submission is an Accrual Report##")
                        move_accrual_data(s3_client, bucket, curr_file)
                    elif study_name != study_type:
                        print(f"##  Submission not in {study_type}, correct and rerun ##")
                        #continue
                    col_err_count = current_sub_object.check_col_errors(file_sep, curr_file)
                    if col_err_count > 0:
                        print(colored("Submission has Column Errors, Data Validation NOT Preformed", "red"))
                        continue
                    current_sub_object = zero_pad_ids(current_sub_object)
                    current_sub_object.get_all_unique_ids(re)
                    current_sub_object.rec_file_names = list(current_sub_object.Data_Object_Table.keys())
                    current_sub_object.populate_missing_keys(sql_tuple)
                    data_table = current_sub_object.Data_Object_Table
                    empty_list = []

                    for file_name in data_table:
                        current_sub_object.set_key_cols(file_name, study_type)
                        if len(data_table[file_name]["Data_Table"]) == 0 and "visit_info_sql.csv" not in file_name:
                            empty_list.append(file_name)
                    for index in empty_list:
                        del current_sub_object.Data_Object_Table[index]
                except Exception as e:
                    display_error_line(e)
                    continue
                try:
                    current_sub_object.create_visit_table_v2(sql_tuple)
                    current_sub_object.create_visit_table("baseline.csv", study_type)
                    current_sub_object.create_visit_table("follow_up.csv", study_type)
                except Exception as e:
                    display_error_line(e)
                    print("Submission does not match study type, visit info is missing")

                current_sub_object.update_object(assay_data, "assay.csv")
                current_sub_object.update_object(assay_target, "assay_target.csv")
                #current_sub_object.update_object(study_design, "study_design.csv")

                data_table = current_sub_object.Data_Object_Table
                if "baseline_visit_date.csv" in data_table:
                    current_sub_object.Data_Object_Table = {"baseline_visit_date.csv": data_table["baseline_visit_date.csv"]}

                valid_cbc_ids = str(current_sub_object.CBC_ID)
                for file_name in current_sub_object.Data_Object_Table:
                    try:
                        if file_name in ignore_validation_list or "_sql.csv" in file_name:
                            continue
                        if "Data_Table" in current_sub_object.Data_Object_Table[file_name]:
                            data_table = current_sub_object.Data_Object_Table[file_name]['Data_Table']
                            data_table.fillna("N/A", inplace=True)
                            data_table = current_sub_object.correct_var_types(file_name, study_type)
                            print(f"{file_name} has been completed: corrected var types")
                    except Exception as e:
                        display_error_line(e)
                for file_name in current_sub_object.Data_Object_Table:
                    try:
                        if file_name in ignore_validation_list or "_sql.csv" in file_name:
                            continue
                        if "Data_Table" in current_sub_object.Data_Object_Table[file_name]:
                            tic = time.time()
                            data_table, drop_list = current_sub_object.merge_tables(file_name)
                            data_table.fillna("N/A", inplace=True)
                            data_table.replace("","N/A", inplace=True)
                            current_sub_object.Data_Object_Table[file_name]['Data_Table'] = data_table.drop(drop_list, axis=1)
                            current_sub_object.Data_Object_Table[file_name]['Data_Table'].drop_duplicates(inplace=True)
                            current_sub_object = vald_rules.Validation_Rules(re, datetime, current_sub_object, data_table,
                                                                             file_name, valid_cbc_ids, drop_list, study_type)
                            #if file_name in current_sub_object.rec_file_names:
                            #    current_sub_object.check_dup_visit(pd, data_table, drop_list, file_name)
                            toc = time.time()
                            print(f"{file_name} took %.2f seconds" % (toc-tic))
                        else:
                            print(file_name + " was not included in the submission")
                    except Exception as e:
                        display_error_line(e)
                vald_rules.check_ID_Cross_Sheet(current_sub_object, os, re, file_sep, study_name)
                if file_name in ["baseline_visit_date.csv"]:
                    vald_rules.check_baseline_date(current_sub_object, pd, sql_tuple, parse)
                elif study_type == "Vaccine_Response":
                    current_sub_object.compare_visits("baseline")
                    current_sub_object.compare_visits("followup")
                    vald_rules.check_comorbid_hist(pd, sql_tuple, current_sub_object)
                    vald_rules.check_vacc_hist(pd, sql_tuple, current_sub_object)
                    #current_sub_object.check_comorbid_dict(pd, sql_tuple[2])
                elif study_type == "Refrence_Pannel":
                    vald_rules.compare_SARS_tests(current_sub_object, pd, sql_tuple[2])
                vald_rules.check_shipping(current_sub_object, pd, sql_tuple[2])
                try:
                    dup_visits = current_sub_object.dup_visits
                    if len(dup_visits) > 0:
                        dup_visits = dup_visits.query("File_Name in ['baseline.csv', 'follow_up.csv']")
                        dup_count = len(dup_visits)
                    else:
                        dup_count = 0
                    err_count = len(current_sub_object.Error_list)
                    current_sub_object.write_error_file(os, file_sep)

                    if dup_count > 0:
                        print(colored("Duplicate Visit Information was found", 'red'))
                        dup_visits.to_csv(current_sub_object.Data_Validation_Path +
                                          file_sep + "Duplicate_Visit_ID_Errors.csv", index=False)
                    if err_count == 0 and dup_count == 0:
                        error_str = "No Errors were Found during Data Validation"
#                        move_target_folder(curr_date, file_sep, file_path, "02_Passed_Data_Validation")
                        print(colored(error_str, "green"))
                    else:
                        error_str = ("Data Validation found " + str(len(current_sub_object.Error_list)) +
                                     " errors in the submitted files")
                        # current_sub_object.split_into_error_files(os, file_sep)
#                        move_target_folder(curr_date, file_sep, file_path, "04_Failed_Data_Validation")
                        print(colored(error_str, "red"))
                except Exception as err:
                    print("An Error Occured when trying to write output file")
                    display_error_line(err)
            print(colored("\nEnd of Current CBC Folder (" + cbc_name + "), moving to next CBC Folder", 'green'))
            clear_dir(iterT)
    except Exception as e:
        print(e)
        display_error_line(e)
    finally:
        if study_type == "Refrence_Pannel":
            populate_md5_table(pd, sql_tuple, study_type)
        close_connections(dbname, sql_tuple)
    print("\nALl folders have been checked")


    print("Preforming update data tables")
    print("Updating Participant offset table")
    #Update_Participant_Info.update_participant_info()
    print("updating cohort definations for new data")
    #rename_cohorts.main_func()
    #print("updating normalized visit info")
    #vaccine_resp_time_line_v2.make_time_line()



    print("Closing Validation Program")





def close_connections(file_name, conn_tuple):
    print("\n## Connection to " + file_name + " has been closed ##\n")
    conn_tuple[2].close()    # conn
    conn_tuple[1].dispose()  # engine


def move_accrual_data(s3, bucketname, path):
    for root,dirs,files in os.walk(path):
        for file in files:
            new_key = root.replace("C:\Seronet_Data_Validation\Files_To_Validate", "Monthly_Accrual_Reports")
            new_key = new_key.replace( os.path.sep, "/")
            s3.upload_file(os.path.join(root, file), bucketname, new_key + "/" + file)
    

#def check_bio_repo_tables(s3_client, s3_resource, study_type):
#    print("\n## Checking Latest BSI report that was uploaded to S3 ##")
#    start_time = time.time()
#    dup_df = bio_repo_map(s3_client, s3_resource, study_type)
#    if len(dup_df) == 0:
#        print("## Biorepository_ID_map.xlsx file has been updated ## \n")
#    else:
##        print("## Duplicate IDs were found in the Biorepository.  Please Fix## \n")
#    print("Biorepository Report took %.2f seconds" % (time.time() - start_time))


def sort_CBC_list(CBC_Folders):
    sort_order = [int(i[-2:]) for i in CBC_Folders]
    sort_list = sorted(range(len(sort_order)), key=lambda k: sort_order[k])
    CBC_Folders = [CBC_Folders[i] for i in sort_list]
    return CBC_Folders


def clear_dir(file_path):
    if not os.path.exists(file_path):  # if folder no longer exists nothing to process
        return
    if len(os.listdir(file_path)) == 0:  # if CBC folder is empty after all files procesed
        shutil.rmtree(file_path)         # remove empty folder


def check_if_done(file_list):
    for iterD in file_list:
        if os.path.isfile(iterD):
            file_path, file_name = os.path.split(iterD)
            os.remove(file_path)
            print(colored("\n##    File Validation has NOT been run for " + file_name + "    ##", 'yellow'))
            file_list.remove(iterD)
    return file_list


def display_error_line(ex):
    trace = []
    tb = ex.__traceback__
    while tb is not None:
        trace.append({"filename": tb.tb_frame.f_code.co_filename,
                      "name": tb.tb_frame.f_code.co_name,
                      "lineno": tb.tb_lineno})
        tb = tb.tb_next
    print(str({'type': type(ex).__name__, 'message': str(ex), 'trace': trace}))


def rename_CBC_folders(root_dir, CBC_Folders):
    os.chdir(root_dir + file_sep + "Files_To_Validate" + file_sep)
    curr_folders = os.listdir(root_dir + file_sep + "Files_To_Validate" + file_sep)
    dir_check("cbc01", "Feinstein_CBC01", curr_folders)
    dir_check("cbc02", "UMN_CBC02", curr_folders)
    dir_check("cbc03", "ASU_CBC03", curr_folders)
    dir_check("cbc04", "Mt_Sinai_CBC04", curr_folders)


def listdirs(rootdir, file_list):
    for file in os.listdir(rootdir):
        d = os.path.join(rootdir, file)
        if os.path.isdir(d):
            if(('Data_Validation_Results' in d) or ('File_Validation_Results' in d) or
               ('UnZipped_Files' in d)):
                pass
            elif '.zip' in d:
                file_list.append(d)
            listdirs(d, file_list)
    return file_list


def dir_check(old_cbc, new_cbc, curr_folder):
    if old_cbc in curr_folder:
        if new_cbc not in curr_folder:
            try:
                os.rename(old_cbc, new_cbc)
            except Exception:
                print("unable to rename folder")
        else:
            shutil.rmtree(os.getcwd() + file_sep + old_cbc)


def get_subfolder(root_dir, folder_name):
    file_path = root_dir + file_sep + folder_name
    file_dir = os.listdir(file_path)
    file_dir = [file_path + file_sep + i for i in file_dir]
    return file_dir


def create_sub_folders(root_dir, folder_name, data_folder=False):
    if isinstance(folder_name, list):
        for curr_folder in folder_name:
            folder_path = root_dir + file_sep + "Files_Processed" + file_sep + curr_folder
            make_folder(root_dir, data_folder, folder_path, folder_name)
    else:
        folder_path = root_dir + file_sep + "Files_Processed" + file_sep + folder_name
        make_folder(root_dir, data_folder, folder_path, folder_name)


def make_folder(root_dir, data_folder, folder_path, folder_name):
    if data_folder is True:
        folder_path = root_dir + file_sep + folder_name
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)


def move_target_folder(root_dir, file_sep, orgional_path, error_path):
    error_dir = root_dir[0].replace("Files_To_Validate", "Files_Processed" + file_sep + error_path)
    if not os.path.exists(error_dir):
        os.makedirs(error_dir)

    target_path = error_dir + file_sep + root_dir[1]
    move_func(orgional_path, target_path)


def move_func(orgional_path, target_path):
    try:
        shutil.move(orgional_path, target_path)
    except Exception:
        shutil.rmtree(target_path)
        shutil.move(orgional_path, target_path)


def move_failed_sub(orgional_path, failed_dir, dest_path, current_sub_object):
    error_path = failed_dir + file_sep + dest_path
    move_target_folder(orgional_path, error_path, current_sub_object.File_Name)


def check_multi_sub(curr_loc, new_path, samp_file, iterZ):
    if len(os.listdir(curr_loc)) > 1:
        file_list = os.listdir(curr_loc)
        if not os.path.exists(new_path):
            os.makedirs(new_path)

        curr_submission = [i for i in file_list if samp_file["Submission_Name"][iterZ] in i][0]
        curr_loc = curr_loc + file_sep + curr_submission
        new_path = new_path + file_sep + curr_submission
    return curr_loc, new_path


def move_updated(summary_file, root_dir):
    # print("\n#####   Checking for Submissions flagged as Updated   #####\n")
    samp_file = summary_file.query("Submission_Status in ['Updated']")
    move_count = 0
    for iterZ in samp_file.index:
        curr_loc = (samp_file["Folder_Location"][iterZ] + file_sep + samp_file["CBC_Num"][iterZ] +
                    file_sep + samp_file["Date_Timestamp"][iterZ])
        new_path = (root_dir + file_sep + "Files_To_Validate" + file_sep + samp_file["CBC_Num"][iterZ] +
                    file_sep + samp_file["Date_Timestamp"][iterZ])

        curr_loc, new_path = check_multi_sub(curr_loc, new_path, samp_file, iterZ)
        move_func(curr_loc, new_path)
        move_count = move_count + 1
    if move_count > 0:
        print("There are " + str(move_count) + " Submissions Found that had Updates (Changes)")
        print("These submissions have been moved back into the Files_To_Validate Folder to be reproccesed")
    return summary_file


def check_passed_file_validation(os, curr_file, root_dir):
    list_of_folders = os.listdir(curr_file)
    passing_msg = ("File is a valid Zipfile. No errors were found in submission. " +
                   "Files are good to proceed to Data Validation")
    result_message = check_result_message(root_dir, list_of_folders, curr_file, passing_msg)
    return result_message


def check_result_message(root_dir, list_of_folders, file_path, passing_msg):
    result_message = "Result File Is Missing"
    if "File_Validation_Results" not in list_of_folders:
        print(colored("File-Validation has not been run on this submission\n", "yellow"))
        shutil.rmtree(file_path)
    else:
        curr_file = os.listdir(file_path + file_sep + "File_Validation_Results")
        if "Result_Message.txt" in curr_file:
            result_file = (file_path + file_sep + "File_Validation_Results" + file_sep + "Result_Message.txt")
            result_message = open(result_file, "r").read()
            if result_message != passing_msg:
                print("Submitted File FAILED the File-Validation Process. With Error Message: " + result_message + "\n")
            else:
                result_message = "File Passed File-Validation"
    return result_message


def populate_object(current_sub_object, Subpath, list_of_files, Support_Files, study_type):
    for iterF in list_of_files:
        file_path = Subpath + file_sep + "Unzipped_Files" + file_sep + iterF
        current_sub_object.get_data_tables(iterF, file_path, study_type)
        if iterF not in ["study_design.csv"]:   # do not check columns for this file
            current_sub_object.column_validation(iterF, Support_Files)
    study_name = current_sub_object.get_submission_metadata(Support_Files)
    return current_sub_object, study_name


def move_file_and_update(orgional_path, root_dir, current_sub_object,
                         curr_dict, folder_str, error_str):
    root_dir = root_dir + file_sep + "Files_Processed"
    move_failed_sub(orgional_path, root_dir, folder_str, current_sub_object)
    curr_dict["Validation_Status"] = error_str
    curr_dict["Folder_Location"] = root_dir + file_sep + folder_str


def write_excel_sheets(writer, summary_file, file_path, new_sheet_name):
    df1 = summary_file[summary_file["Folder_Location"].apply(lambda x: file_path in x)]
    df1.to_excel(writer, sheet_name=new_sheet_name, index=False)
    return writer


def clear_empty_folders(root_dir):
    process_path = root_dir + file_sep + "Files_Processed"
    processed_folders = os.listdir(process_path)
    for iterF in processed_folders:
        cbc_folders = os.listdir(process_path + file_sep + iterF)
        for iterC in cbc_folders:
            curr_cbc = os.listdir(process_path + file_sep + iterF + file_sep + iterC)
            if len(curr_cbc) == 0:
                shutil.rmtree(process_path + file_sep + iterF + file_sep + iterC)


def check_for_typo(summary_file):
    # print("#####   Checking for Errors/Typos in the Submission Status Field  #####")
    error_count = 0
    for iterZ in summary_file.index:
        curr_status = summary_file["Submission_Status"][iterZ]
        try:
            curr_status = curr_status.lower()
        except AttributeError:
            print(curr_status)
            continue
        if curr_status in ["updated", "update", "fixed"]:
            summary_file["Submission_Status"][iterZ] = "Updated"
        elif ("upload" in curr_status) or ("uploaded" in curr_status):
            if ("pass" in curr_status) or ("passed" in curr_status):
                summary_file["Submission_Status"][iterZ] = "Uploaded_to_Passed_S3_Bucket"
            elif ("fail" in curr_status) or ("faileded" in curr_status):
                summary_file["Submission_Status"][iterZ] = "Uploaded_to_Failed_S3_Bucket"
            else:
                summary_file["Submission_Status"][iterZ] = "Unknown"
        elif ("major" in curr_status) or ("errors" in curr_status):
            summary_file["Submission_Status"][iterZ] = "Major_Errors_Found"
        elif ("pending" in curr_status) or ("feedback" in curr_status):
            summary_file["Submission_Status"][iterZ] = "Pending_Feedback"
        elif curr_status not in ["downloaded", "pending review"]:
            summary_file["Submission_Status"][iterZ] = "Unknown"
        if summary_file["Submission_Status"][iterZ] == "Unknown":
            error_count = error_count + 1
            print(curr_status + " is not a valid Submission Status Option. Defaulting to Unknown")
#    if error_count == 0:
#        print("No Submission Status Errors were found")
    return summary_file


def zero_pad_ids(curr_obj):
    data_dict = curr_obj.Data_Object_Table
    try:
        if "aliquot.csv" in data_dict:
            z = data_dict["aliquot.csv"]["Data_Table"]["Aliquot_ID"].tolist()
            z = [i[0:13] + "_0" + i[14] if i[-2] == '_' else i for i in z]
            data_dict["aliquot.csv"]["Data_Table"]["Aliquot_ID"] = z
        if "shipping_manifest.csv" in data_dict:
            z = data_dict["shipping_manifest.csv"]["Data_Table"]["Current Label"].tolist()
            for index in range(len(z)):
                if len(z[index]) <= 16:   # valid aliquot length
                    pass
                elif z[index][15].isnumeric():
                    z[index] = z[index][:16]
                elif z[index][14].isnumeric():
                    z[index] = z[index][:15]
                else:
                    print("unknown string")

            z = [i[0:13] + "_0" + i[14] if i[-2] == '_' else i for i in z]
            data_dict["shipping_manifest.csv"]["Data_Table"]["Current Label"] = z
    except Exception as e:
        print(e)
    curr_obj.Data_Object_Table = data_dict
    return curr_obj


def generate_rec_report(sql_tuple, s3_client, bucket):
    print("\n Generating Requestion Excel Files for BSI")
    start_time = time.time()
    curr_file = "Serology_Data_Files/biorepository_id_map/Biorepository_ID_map.xlsx"
    parent_data = pd_s3.get_df_from_keys(s3_client, bucket, curr_file, suffix="xlsx", sheet_name="BSI_Parent_Aliquots",
                                         format="xlsx", na_filter=False, output_type="pandas")
    child_data = pd_s3.get_df_from_keys(s3_client, bucket, curr_file, suffix="xlsx", sheet_name="BSI_Child_Aliquots",
                                        format="xlsx", na_filter=False, output_type="pandas")
    parent_data = parent_data.query("`Material Type` == 'SERUM'")

    parent_data["Participant_ID"] = [i[:9] for i in parent_data["CBC_Biospecimen_Aliquot_ID"].tolist()]
    z = parent_data['Participant_ID'].value_counts()
    z = z.to_frame()
    z.reset_index(inplace=True)
    z.columns = ["Participant_ID", "Vial_Count"]

    # single_ids = parent_data.merge(z.query('Vial_Count == 1'))
    parent_data = parent_data.merge(z.query('Vial_Count > 1'))
#    parent_data = parent_data.query("`Vial Status` in ['In']")

    parent_data.sort_values(by="CBC_Biospecimen_Aliquot_ID", inplace=True)
    parent_data.drop_duplicates("Participant_ID", keep="first", inplace=True)

    child_data["Participant_ID"] = [i[:9] for i in child_data["CBC_Biospecimen_Aliquot_ID"].tolist()]
    child_data.drop_duplicates("Participant_ID", keep="first", inplace=True)

    z = parent_data.merge(child_data["Biorepository_ID"], on='Biorepository_ID',
                          how="outer", indicator=True)
    z = z.query("_merge == 'left_only'")
    z["CBC_ID"] = [i[:2] for i in z["Participant_ID"].tolist()]

    Mount_Sinai = z.query("CBC_ID == '14'")
    Minnesota = z.query("CBC_ID == '27'")
    Arizona = z.query("CBC_ID == '32'")
    Feinstein = z.query("CBC_ID == '41'")
    write_cgr_file(Mount_Sinai, "Mount_Sinai_to_CGR.xlsx")
    write_cgr_file(Minnesota, "UMN_to_CGR.xlsx")
    write_cgr_file(Arizona, "ASU_to_CGR.xlsx")
    write_cgr_file(Feinstein, "Feinstein_to_CGR.xlsx")
    print("Reports took %.2f seconds" % (time.time() - start_time))


def write_cgr_file(df, file_name):
    df = df[~df["Vial Modifiers"].str.contains('Missing PCR Result')]
    if len(df) > 0:
        df.drop(["Participant_ID", "_merge", "CBC_ID", "Vial_Count"], axis=1, inplace=True)
        writer = pd.ExcelWriter(f"C:\\Python_Code\\Serology_Reports\\{file_name}", engine='xlsxwriter')
        df.to_excel(writer, index=False)
        writer.save()
        writer.close()
        writer.handles = None


def check_serology_submissions(s3_client, colored, bucket, assay_data, assay_target):
    key = "Serology_Data_Files/serology_confirmation_test_result/"
    serology_code = '12'
    resp = s3_client.list_objects_v2(Bucket=bucket, Prefix=key)
    file_name = "serology_confirmation_test_results.csv"
    for curr_serology in resp["Contents"]:
        if ("Test_Results_Passed" in curr_serology["Key"]) or ("Test_Results_Failed" in curr_serology["Key"]):
            pass
        elif ".xlsx" in curr_serology["Key"]:
            current_sub_object = Submission_Object("Serology")
            serology_data = pd_s3.get_df_from_keys(s3_client, bucket, prefix=curr_serology["Key"], suffix="xlsx",
                                                   format="xlsx", na_filter=False, output_type="pandas")

            data_table, drop_list = current_sub_object.validate_serology(file_name, serology_data,
                                                                         assay_data, assay_target, serology_code)
            current_sub_object = vald_rules.Validation_Rules(re, datetime, current_sub_object, data_table,
                                                  file_name, serology_code, drop_list, study_type)
            error_list = current_sub_object.Error_list
            error_count = len(error_list)
            if error_count == 0:
                print(colored("\n## Serology File has No Errors, Submission is Valid\n", "green"))
            else:
                print(colored(f"\n## Serology File has {error_count} Errors, Check submission\n", "red"))


def populate_md5_table(pd, sql_tuple, study_type):
    convert_table = pd.read_sql(("SELECT * FROM Deidentifed_Conversion_Table"), sql_tuple[2])
    if study_type == "Refrence_Pannel":
        demo_ids = pd.read_sql(("SELECT Research_Participant_ID FROM Participant"), sql_tuple[2])
        bio_ids = pd.read_sql(("SELECT Biospecimen_ID FROM Biospecimen"), sql_tuple[2])
        aliquot_ids = pd.read_sql(("SELECT Aliquot_ID FROM Aliquot"), sql_tuple[2])
    elif study_type == "Vaccine_Response":
        pass
    all_ids = set_tables(demo_ids, "Participant_ID")
    all_ids = pd.concat([all_ids, set_tables(bio_ids, "Biospecimen_ID")])
    all_ids = pd.concat([all_ids, set_tables(aliquot_ids, "Aliquot_ID")])
    all_ids = all_ids.merge(convert_table, how="left", indicator=True)
    all_ids = all_ids.query("_merge == 'left_only'")
    if len(all_ids) > 0:  # new ids need to be added
        all_ids.drop("_merge", inplace=True, axis=1)
        all_ids["MD5_Value"] = [hashlib.md5(i.encode('utf-8')).hexdigest() for i in all_ids["ID_Value"].tolist()]
        all_ids.to_sql(name="Deidentifed_Conversion_Table", con=sql_tuple[1], if_exists="append", index=False)


def set_tables(df, id_type):
    df.columns = ["ID_Value"]
    df["ID_Type"] = id_type
    df = df[["ID_Type", "ID_Value"]]
    return df


Data_Validation_Main(study_type)
