# -*- coding: utf-8 -*-
"""
Created on Mon Nov  7 13:41:20 2022

Udpdate visit 1 offset table as well as date of first visit from accrual report
function needs to be run each time a new participant is added
@author: breadsp2
"""

from import_loader_v2 import pd, sd
from connect_to_sql_db import connect_to_sql_db
pd.options.mode.chained_assignment = None  # default='warn'

def update_participant_info():
    sql_column_df, engine, conn = connect_to_sql_db(pd, sd, "seronetdb-Vaccine_Response")
#########  Update the first visit offset correction table for new participants ###################
    offset_data = pd.read_sql(("SELECT * FROM Visit_One_Offset_Correction;"), conn)

    visit_data = pd.read_sql(("SELECT Research_Participant_ID, Visit_Date_Duration_From_Index, Visit_Info_ID, Visit_Number FROM Participant_Visit_Info"), conn)
    visit_data = visit_data.query("Visit_Number == '1'")        #normalize to baseline,  pre_baseline will be negative durations
    visit_data = visit_data[['Research_Participant_ID', 'Visit_Date_Duration_From_Index']]
    #visit_data.drop_duplicates("Research_Participant_ID", keep="first", inplace=True)   #get first visit (lowest duration from index)
    
    merged_data = visit_data.merge(offset_data, left_on=visit_data.columns.tolist(), right_on=offset_data.columns.tolist(), how="left", indicator=True)
    new_data = merged_data.query("_merge not in ['both']")
    new_data = new_data.drop(["Offset_Value", "_merge"], axis=1)
    new_data = new_data.rename(columns={"Visit_Date_Duration_From_Index":"Offset_Value"})
    
    check_data = new_data.merge(offset_data["Research_Participant_ID"], how="left", indicator=True)
    new_data = check_data.query("_merge not in ['both']").drop("_merge", axis=1)
    update_data = check_data.query("_merge in ['both']").drop("_merge", axis=1)
    
    if len(new_data) == 0:
        print("there are no new particicpants to add to Visit_One_Offset_Correction")
    else:
        print(f"Adding {len(new_data)} particicpants to to Visit_One_Offset_Correction")
    try:
        new_data.to_sql(name="Visit_One_Offset_Correction", con=engine, if_exists="append", index=False)
    except Exception as e:
        print(e)
    finally:
        conn.connection.commit()
        
    for curr_part in update_data.index:
        try:
            sql_qry = (f"update Visit_One_Offset_Correction set Offset_Value = '{update_data.loc[curr_part, 'Offset_Value']}' " +
                       f"where Research_Participant_ID = '{update_data.loc[curr_part, 'Research_Participant_ID']}'")
            engine.execute(sql_qry)
        except Exception as e:
            print(e)
        finally:
            conn.connection.commit()

    primary_cohort = pd.read_sql(("SELECT Research_Participant_ID, Site_Cohort_Name, Primary_Cohort FROM `seronetdb-Vaccine_Response`.Accrual_Visit_Info "), conn)
    data_cohort = pd.read_sql(("SELECT Research_Participant_ID,  Primary_Study_Cohort, CBC_Classification FROM `seronetdb-Vaccine_Response`.Participant_Visit_Info " + 
                               "where Primary_Study_Cohort is NULL"), conn)
    cohort_update = data_cohort.merge(primary_cohort, left_on=["Research_Participant_ID", "CBC_Classification"], right_on=["Research_Participant_ID", "Site_Cohort_Name"]).drop_duplicates()

    for curr_idx in cohort_update.index:
        part_id = cohort_update["Research_Participant_ID"][curr_idx]
        old_cohort = cohort_update["CBC_Classification"][curr_idx]
        cohort = cohort_update["Primary_Cohort"][curr_idx]

        #sql_str = (f"Update Participant set Sunday_Prior_To_First_Visit = '{date_sunday}' where Research_Participant_ID = '{part_id}'")
        #engine.execute(sql_str)
        
        sql_str = (f"Update  Participant_Visit_Info set Primary_Study_Cohort = '{cohort}' where Research_Participant_ID = '{part_id}' and CBC_Classification = '{old_cohort}'")
        engine.execute(sql_str)

#########  Update the Update Accural demographics based on submitted data (assumuing truth) ###################
    #accrual_data = pd.read_sql(("SELECT Research_Participant_ID, Age, Sex_At_Birth, Race, Ethnicity FROM Accrual_Participant_Info;"), conn)
    #part_data = pd.read_sql(("SELECT Research_Participant_ID, Age, Sex_At_Birth, Race, Ethnicity FROM Participant;"), conn)
    
    #x = accrual_data.merge(part_data, how="outer", indicator=True)
    #x = x.query("_merge not in ['both']")
    #dup_logic = x["Research_Participant_ID"].duplicated(keep=False)
    #x2 = x[dup_logic]   #records where accrual demo are diffent then submitted demo for same participant
    #x2 = x2.query("_merge == 'right_only'")
    
    #for curr_part in x2.index:
    #    try:
    #        sql_qry = (f"update Accrual_Participant_Info set Sex_At_Birth = '{x2.loc[curr_part, 'Sex_At_Birth']}', " +
    #                   f"Age = '{x2.loc[curr_part, 'Age']}', " +
    #                   f"Race = '{x2.loc[curr_part, 'Race']}', " +
    #                   f"Ethnicity = '{x2.loc[curr_part, 'Ethnicity']}' " +
    #                   f"where Research_Participant_ID = '{x2.loc[curr_part, 'Research_Participant_ID']}'")
    #        engine.execute(sql_qry)
    #    except Exception as e:
    #        print(e)
    #    finally:
    #        conn.connection.commit()


#########  Update the "Sunday_Prior_To_First_Visits" feild using accrual reports ###################
    accrual_data = pd.read_sql(("SELECT Research_Participant_ID, Sunday_Prior_To_Visit_1 FROM Accrual_Participant_Info;"), conn)
    part_data = pd.read_sql(("SELECT Research_Participant_ID, Sunday_Prior_To_First_Visit FROM Participant;"), conn)

    merged_data = part_data.merge(accrual_data, on="Research_Participant_ID", how="left", indicator=True)
    check_data = merged_data.query("_merge in ['both']")
    check_data = check_data.query("Sunday_Prior_To_First_Visit !=Sunday_Prior_To_Visit_1")
    check_data = check_data.query("Sunday_Prior_To_First_Visit == Sunday_Prior_To_First_Visit")

    if len(check_data) == 0:
        print("there are no new particicpants to update Participant table")
    else:
        print(f"Updating {len(check_data)} particicpants to Participant Table")

    for curr_part in check_data.index:
        try:
            sql_qry = (f"update Participant set Sunday_Prior_To_First_Visit = '{check_data.loc[curr_part, 'Sunday_Prior_To_Visit_1']}' " +
                       f"where Research_Participant_ID = '{check_data.loc[curr_part, 'Research_Participant_ID']}'")
            engine.execute(sql_qry)
        except Exception as e:
            print(e)
        finally:
            conn.connection.commit()
#########  Update the Primary Cohort feild using accrual reports ###################
    accrual_data = pd.read_sql(("SELECT distinct Research_Participant_ID, Site_Cohort_Name, Primary_Cohort FROM Accrual_Visit_Info;"), conn)

    visit_data = pd.read_sql(("SELECT Research_Participant_ID, Visit_Info_ID, Primary_Study_Cohort, CBC_Classification FROM Participant_Visit_Info " +
                              "where Primary_Study_Cohort is NULL;"), conn)
    x = visit_data.merge(accrual_data, how="left", on="Research_Participant_ID")
    
    x.fillna("No Data", inplace=True)
    y = x.query("Primary_Study_Cohort != Primary_Cohort")
    y = y.query("Primary_Cohort not in ['No Data']")  #no accrual data to update
    if len(y) == 0:
        print("there are no new particicpants to update Primary_Study_Cohort")
    else:
        print(f"Updating Primary_Study_Cohort for  {len(y)} particicpants")
    
    for curr_part in y.index:
        try:
            sql_qry = (f"update Participant_Visit_Info set CBC_Classification = '{y.loc[curr_part, 'Site_Cohort_Name']}', " +
                       f"Primary_Study_Cohort = '{y.loc[curr_part, 'Primary_Cohort']}' " +
                       f"where Visit_Info_ID = '{y.loc[curr_part, 'Visit_Info_ID']}'")
            engine.execute(sql_qry)
        except Exception as e:
            print(e)
        finally:
            conn.connection.commit()



def check_cormbid(curr_part, cond_list):
    has_comorbid = 0
    for curr_col in cond_list:
        y = curr_part[curr_col]
        count = len(y[y.str.contains("New Condition|Yes|Obesity")])
        if count > 0:
            has_comorbid = has_comorbid + 1
    return has_comorbid

update_participant_info()