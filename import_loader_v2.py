# -*- coding: utf-8 -*-
"""
Created on Thu Oct 14 16:47:28 2021
imports all utilities for the validation process
@author: breadsp2
"""
import boto3
import re
import os

import datetime
import dateutil.tz
import pathlib
import shutil
from dateutil.parser import parse

from colorama import init
from termcolor import colored
from termcolor import cprint

from pandas_aws import s3 as pd_s3
import pandas as pd
import numpy as np
import sqlalchemy as sd
import icd10

import aws_creds_prod
import get_box_data
import shipping_import
import warnings
import time
from collections import Counter
import random
from datetime import date


def set_up_function():
    warnings.simplefilter("ignore")

    file_sep = os.path.sep
    eastern = dateutil.tz.gettz("US/Eastern")
    validation_date = datetime.datetime.now(tz=eastern).strftime("%Y-%m-%d")
    pd.options.mode.chained_assignment = None  # default='warn'

    box_dir = "C:" + file_sep + "Users" + file_sep + os.getlogin() + file_sep + "Box"

    s3_client = boto3.client('s3', aws_access_key_id=aws_creds_prod.aws_access_id, aws_secret_access_key=aws_creds_prod.aws_secret_key,
                             region_name='us-east-1')
    s3_resource = boto3.resource('s3', aws_access_key_id=aws_creds_prod.aws_access_id, aws_secret_access_key=aws_creds_prod.aws_secret_key,
                                 region_name='us-east-1')

    cbc_codes = get_cbc_file(box_dir, "SeroNet DMS" + file_sep + "12 SeroNet Data Submitter Information", file_sep)
    return file_sep, s3_client, s3_resource, cbc_codes, validation_date, box_dir


def get_cbc_file(box_dir, box_path, file_sep):
    file_path = []
    cur_path = box_dir + file_sep + box_path
    for r, d, f in os.walk(cur_path):  # r=root, d=directories, f = files
        for file in f:
            if (file.endswith(".xlsx")):
                file_path.append(os.path.join(r, file))
    return file_path


def get_template_columns(template_dir):
    template_data = {}
    for r, d, f in os.walk(template_dir):  # r=root, d=directories, f = files
        if "Deprecated" in r:
            pass
        else:
            for file in f:
                if "~" in file or "$" in file:  # temp files, ignore these
                    pass
                elif file.endswith(".xlsx") or file.endswith(".xlsm"):
                    if file in ["vaccine_response_data_model.xlsx"]:
                        pass  # do not include this file
                    else:
                        file_path = os.path.join(r, file)
                        curr_data = pd.read_excel(file_path, na_filter=False, engine='openpyxl')
                        curr_data.drop([i for i in curr_data.columns if "Unnamed" in i], axis=1, inplace=True)
                        template_data[file] = {"Data_Table": curr_data}
    col_list = []
    sheet_name = []
    for file in template_data:
        sheet_name = sheet_name + [file]*len(template_data[file]["Data_Table"].columns.tolist())
        col_list = col_list + template_data[file]["Data_Table"].columns.tolist()
    return pd.DataFrame({"Sheet_Name": sheet_name, "Column_Name": col_list})


def get_template_data(box_dir, file_sep, study_name):
    if study_name == 'Refrence_Pannel':
        template_dir = (box_dir + file_sep + "CBC Data Submission Documents" + file_sep + "Data Submission Templates")
        template_df = get_template_columns(template_dir)
        dbname = "seronetdb-Validated"  # name of the SQL database where data is saved
    elif study_name == 'Vaccine_Response':
        template_dir = (box_dir + file_sep + "CBC Data Submission Documents" + file_sep + "Vaccine_Response_Study_Templates")

        template_df = get_template_columns(template_dir)
        dbname = "seronetdb-Vaccine_Response"  # name of the SQL database where data is saved
    else:
        template_df = []
    return template_df, dbname
