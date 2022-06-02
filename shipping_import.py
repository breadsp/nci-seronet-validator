# -*- coding: utf-8 -*-
"""
Created on Wed Oct 27 09:11:08 2021

@author: breadsp2
"""

from import_loader import *


def shipping_import(s3_client):
    bucket = "nci-cbiit-seronet-submissions-passed"
    key = "Serology_Data_Files/Secondary_Shipping_Manifests/Approved Manifests/"
    l_df = list()
    resp = s3_client.list_objects_v2(Bucket=bucket, Prefix=key)

    dup_id_list = ['LP09357 0001', 'LP09366 0001', 'LP09484 0001', 'LP09624 0001',
                   'LP09656 0001', 'LP09720 0001', 'LP09777 0001', 'LP09948 0001',
                   'LP12184 0001', 'LP12465 0001']

    if "Contents" not in resp:
        return []
    for curr_file in resp["Contents"]:
        Destination = "Unknown"
        if ".xlsx" in curr_file["Key"]:
            ship_data = pd_s3.get_df_from_keys(s3_client, bucket, prefix=curr_file["Key"], suffix="xlsx",
                                               format="xlsx", na_filter=False, output_type="pandas")
        else:
            continue

        Destination = ship_data.iloc[0, 3].strip()
        if ship_data.iloc[6, 3] == "Barcode_ID":
            ship_data.drop(ship_data.columns[10:], axis=1, inplace=True)
            ship_data.columns = ship_data.loc[6]
            ship_data.drop(ship_data.index[range(0, 7)], inplace=True)
            ship_data["Barcode_ID"] = ship_data["Readable_ID"]
            ship_data = ship_data[ship_data["Barcode_ID"].apply(lambda x: x not in "Empty")]
            ship_data["BSI ID"] = [i[:7] + " 0001" for i in ship_data["Barcode_ID"].tolist()]
            ship_data["Destination"] = Destination
            if ("Northwell_Testing Shipping Manifest_24Jan22.xlsx" in curr_file["Key"] or
               "Northwell_Testing Shipping Manifest_25Jan22.xlsx" in curr_file["Key"]):
                ship_data = ship_data[ship_data["BSI ID"].apply(lambda x: x not in dup_id_list)]
            ship_data["File_Name"] = curr_file["Key"]
            ship_data.reset_index(inplace=True, drop=True)

            z = [i[0] for i in enumerate(ship_data["Barcode_ID"]) if "S2" not in i[1]]
            ship_data["Barcode_ID"] = [i.replace("  ", " ") for i in ship_data["Barcode_ID"]]
            ship_data["Readable_ID"] = [i.replace("  ", " ") for i in ship_data["Readable_ID"]]
            ship_data = ship_data.loc[z]
            l_df.append(ship_data)
    return pd.concat(l_df, axis=0, ignore_index=True).reset_index(drop=True)
