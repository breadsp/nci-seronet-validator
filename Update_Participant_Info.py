# -*- coding: utf-8 -*-
"""
Created on Mon Nov  7 13:41:20 2022

Udpdate visit 1 offset table as well as date of first visit from accrual report
function needs to be run each time a new participant is added
@author: breadsp2
"""

from import_loader_v2 import pd, sd
from connect_to_sql_db import connect_to_sql_db

def update_participant_info():
    sql_column_df, engine, conn = connect_to_sql_db(pd, sd, "seronetdb-Vaccine_Response")
#########  Update the first visit offset correction table for new participants ###################
    offset_data = pd.read_sql(("SELECT * FROM `seronetdb-Vaccine_Response`.Visit_One_Offset_Correction;"), conn)

    visit_data = pd.read_sql(("SELECT Research_Participant_ID, Visit_Date_Duration_From_Index FROM `seronetdb-Vaccine_Response`.Participant_Visit_Info " +
                              "where Type_Of_Visit = 'Baseline' and Visit_Number = '1';"), conn)
    
    merged_data = visit_data.merge(offset_data, left_on=visit_data.columns.tolist(), right_on=offset_data.columns.tolist(), how="left", indicator=True)
    new_data = merged_data.query("_merge not in ['both']")
    new_data = new_data.drop(["Offset_Value", "_merge"], axis=1)
    new_data = new_data.rename(columns={"Visit_Date_Duration_From_Index":"Offset_Value"})
    new_data.to_sql(name="Visit_One_Offset_Correction", con=engine, if_exists="append", index=False)
    conn.connection.commit()

#########  Update the "Sunday_Prior_To_First_Visits" feild using accrual reports ###################
    accrual_data = pd.read_sql(("SELECT Research_Participant_ID, Week_Of_Visit_1 FROM Accrual_Participant_Info;"), conn)
    part_data = pd.read_sql(("SELECT Research_Participant_ID, Sunday_Prior_To_First_Visit FROM Participant;"), conn)

    merged_data = part_data.merge(accrual_data, on="Research_Participant_ID", how="left", indicator=True)
    check_data = merged_data.query("_merge in ['both']")
    check_data = check_data.query("Sunday_Prior_To_First_Visit != Week_Of_Visit_1")

    for curr_part in check_data.index:
        try:
            sql_qry = (f"update Participant set Sunday_Prior_To_First_Visit = '{check_data.loc[curr_part, 'Week_Of_Visit_1']}' " +
                       f"where Research_Participant_ID = '{check_data.loc[curr_part, 'Research_Participant_ID']}'")
            engine.execute(sql_qry)
        except Exception as e:
            print(e)
        finally:
            conn.connection.commit()
#########  Update the Primary Cohort feild using accrual reports ###################
    accrual_data = pd.read_sql(("SELECT Research_Participant_ID, Visit_Number, Site_Cohort_Name, Primary_Cohort FROM Accrual_Visit_Info;"), conn)
    visit_data = pd.read_sql(("SELECT Visit_Info_ID, Primary_Study_Cohort, CBC_Classification FROM Participant_Visit_Info;"), conn)
    accrual_data["New_Visit_Info_ID"] = (accrual_data["Research_Participant_ID"] + " : V" + ["%02d" % (int(i),) for i in accrual_data['Visit_Number']])
    x = visit_data["Visit_Info_ID"].str.replace(": B", ": V")
    visit_data["New_Visit_Info_ID"] = x.str.replace(": F", ": V")
    x = visit_data.merge(accrual_data, how="left", on= "New_Visit_Info_ID")
    x.fillna("No Data", inplace=True)
    y = x.query("Primary_Study_Cohort != Primary_Cohort or CBC_Classification != Site_Cohort_Name")
    y = y.query("Primary_Cohort not in ['No Data']")  #no accrual data to update
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
    print("x")

update_participant_info()