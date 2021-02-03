import difflib                                  #libraray to compare strings to determine spelling issues
from dateutil.parser import parse               #library to check if a value is a date type
import csv                                      #csv to read and write csv files
import icd10                                    #library that checks if a value is a valid ICD10 code
from io import StringIO
#####################################################################
def get_mysql_queries(file_dbname,conn,index,pd):
    if index == 1:  ## pulls positive and negative participant ids from database for validation purposes
        sql_querry = ("SELECT %s FROM `Participant_Prior_Test_Result` Where Test_Name = %s and `Test_Result` = %s;")
        pos_participants = pd.read_sql(sql_querry, conn,params=["Research_Participant_ID","SARS_Cov_2_PCR",'Positive'])
        neg_participants = pd.read_sql(sql_querry, conn,params=["Research_Participant_ID","SARS_Cov_2_PCR",'Negative'])
        return pos_participants,neg_participants
    elif index == 2:
        bio_ids = "SELECT %s,%s FROM Biospecimen;"
        bio_ids = pd.read_sql(bio_ids, conn, params=['Biospecimen_ID','Biospecimen_Type'])
        return bio_ids
#####################################################################
## creates a list of positive and negative Participant IDS based on the submitted Prior_Test_Result.csv
def split_participant_pos_neg_prior(file_object,pos_list,neg_list,pd):
    test_data = file_object.Data_Table[['Research_Participant_ID','SARS_CoV_2_PCR_Test_Result']]
    pos_list = pd.concat([pos_list,test_data[test_data['SARS_CoV_2_PCR_Test_Result'] == 'Positive']['Research_Participant_ID'].to_frame()])
    pos_list.drop_duplicates(inplace=True)

    neg_list = pd.concat([neg_list,test_data[test_data['SARS_CoV_2_PCR_Test_Result'] == 'Negative']['Research_Participant_ID'].to_frame()])
    neg_list.drop_duplicates(inplace=True)

    return pos_list,neg_list
def check_for_spelling_error(list_of_valid_file_names,uni_id):
    overall_match = 0
    for valid in list_of_valid_file_names:
       sequence = difflib.SequenceMatcher(isjunk = None,a = uni_id,b = valid)
       difference = sequence.ratio()*100
       if overall_match < difference:           #value of 100 is a perfect match
           overall_match = difference
    return overall_match
def string_logic_check(test_value):
    try:
        float(test_value)
        return False
    except Exception:
        try:
            parse(test_value, fuzzy=False)      #value is a date, return error
            return False
        except Exception:
            return True
#####################################################################
class Submitted_file:
    def __init__(self, file_name,ID_col_name):         #initalizes the Object
        '''Initailize the Object and assigns some inital parameters'''
        self.File_name = file_name                     #Name of the file
        self.ID_column_name = ID_col_name              #list of Primary Keys in the file
        self.header_name_validation = [['CSV_Sheet_Name','Column_Value','Error_message']]   #error list header for column name errors
    def set_error_list_header(self):    #initalizes header list for sheet wise errors
        if type(self.ID_column_name) == str:
            self.error_list_summary = [['Message_Type','CSV_Sheet_Name','Row_Number',self.ID_column_name,'Column_Name','Column_Value','Error_message']]
        else:
            list_1  = ['Message_Type','CSV_Sheet_Name','Row_Number']
            list_2 = ['Column_Name','Column_Value','Error_message']
            self.error_list_summary = [list_1 +  self.ID_column_name + list_2]
    def load_csv_file(self,s3_client,output_bucket_name,test_name,pd):  #retrieve file from S3 bucket and call load function
        csv_obj = s3_client.get_object(Bucket=output_bucket_name, Key=test_name)
        csv_string = csv_obj['Body'].read().decode('utf-8')
        load_string_name = StringIO(csv_string)
        self.get_csv_table(load_string_name,pd)
    def get_csv_table(self,file_name,pd):           #loads the data from a file into a pandas data frame
        self.Data_Table = pd.read_csv(file_name,encoding='utf-8',na_filter = False)           #flag to keep N/A values as string
        self.Data_Table.dropna(axis=0, how="all", thresh=None, subset=None, inplace=True)     #if a row is completely blank remove it
        self.Data_Table = self.Data_Table.loc[:,~ self.Data_Table.columns.str.startswith('Unnamed')]    #if a column is completely blank, remove it
        self.Column_Header_List = list(self.Data_Table.columns.values)                        #parse header of dataframe into a list

        blank_logic = [sum(self.Data_Table.iloc[i] == '') < len(self.Column_Header_List) for i in self.Data_Table.index]
        self.Data_Table = self.Data_Table[blank_logic]

        self.file_size = len(self.Data_Table)   #number of rows in the file
        self.set_error_list_header()
###############################################################################
## compare header names in csv sheet to mysql tables and create list of errors for names that do not match
    def compare_csv_to_mysql(self,valid_dbname,my_sql_table,conn):
        master_col_name = []
        for table_name in my_sql_table:     #loops over all SQL tables that the file write to, to get list of matching table names
            query_str = ("select * from {}").format(table_name)

            df = pd.read_sql(query_str,conn)
            Column_Header_List = list(df.columns.values)
            master_col_name = master_col_name + Column_Header_List

        spelling_list = []
        for val in enumerate(self.Column_Header_List):   #compares each column to the master list and checks for spelling match
            overall_match = check_for_spelling_error(master_col_name,val[1])
            spelling_list.append(overall_match)
        self.header_column_spelling_check = spelling_list
        self.get_column_errors()

        for val in enumerate(master_col_name):
           if (val[1] not in self.Column_Header_List):
               if val[1] not in ['Biorepository_ID','Submission_CBC','Test_Agreement','Shipping_ID']:   #list of varaibles known to not be in sheet
                   self.header_name_validation.append([self.File_name,val[1],"Column name is missing, exists in mySQL, not in submission file"])
    def get_column_errors(self):
        for i in enumerate(self.header_column_spelling_check):
            if i[1] < 80:               #value of 80 or less means no reasonable match was found, treats as unknown value
                self.header_name_validation.append([self.File_name,self.Column_Header_List[i[0]],"Unknown Column name, not in SQL tables"])
            elif i[1] < 100:             #value between 80 and 99.9 means that possible match was found, treats as possibe typo
                self.header_name_validation.append([self.File_name,self.Column_Header_List[i[0]],"Column name is misspelled, not in SQL tables"])
###############################################################################
    def remove_unknown_sars_results_v2(self): #if a Participant has a SARS_Cov2_PCR test that is not positive or negative, write an error
        unk_sars_result = self.Data_Table[~(self.neg_list_logic | self.pos_list_logic)]
        for index in unk_sars_result.iterrows():
            self.error_list_summary.append(['Error',self.File_name,index[0]+1,unk_sars_result['Research_Participant_ID'][index[0]]," ",
                                            " ","Unknown Prior SARS_CoV-2 test, not valid Participant"])
        self.Data_Table = self.Data_Table[(self.neg_list_logic | self.pos_list_logic)]          #Filter table
        self.pos_list_logic = self.pos_list_logic[(self.neg_list_logic | self.pos_list_logic)]  #Update the positive logical vector
        self.neg_list_logic = self.neg_list_logic[(self.neg_list_logic | self.pos_list_logic)]  #Update the negative logical vector
###############################################################################
    def check_data_type(self,test_column,header_name): ## logical vectors to see if there is data or missing values
        missing_logic = [(len(str(i)) == 0)  for i in test_column]
        has_logic = [(len(str(i)) > 0)  for i in test_column]

        missing_data_column = self.Data_Table[missing_logic][header_name]
        has_data_column = self.Data_Table[has_logic][header_name]
        return missing_logic,has_logic,missing_data_column,has_data_column
    def get_pos_neg_logic(self,pos_list,neg_list):  #logical vector for SARS_CoV_2_PCR_Test_Result Positive or Negative Participants
        self.pos_list_logic = self.Data_Table['Research_Participant_ID'].isin(pos_list['Research_Participant_ID']) #logic vector for positive Participants
        self.neg_list_logic = self.Data_Table['Research_Participant_ID'].isin(neg_list['Research_Participant_ID']) #logic vector for negative Participants
    def write_error_msg(self,test_value,column_name,error_msg,row_number,error_stat):  #updates the error message variable
        if type(self.ID_column_name) == str:
            self.error_list_summary.append([error_stat,self.File_name,row_number+2,self.Data_Table[self.ID_column_name][row_number],column_name,test_value,error_msg])
        else:
            error_list = [error_stat,self.File_name,row_number+2]
            for i in enumerate(self.ID_column_name):
                error_list = error_list + [self.Data_Table[i[1]][row_number]]
            error_list = error_list + [column_name,test_value,error_msg]
            self.error_list_summary.append(error_list)

    def in_list(self,column_name,test_value,check_str_list,error_msg,row_number,error_stat):           #writes error if test_value not in check_str_list
        try:
            check_str_list.index(test_value)
        except ValueError:
            self.write_error_msg(test_value,column_name,error_msg,row_number,error_stat)
    def valid_ID(self,column_name,test_value,pattern,valid_cbc_id,error_msg,row_number,error_stat):    #see if the ID variable has valid format
        res = (pattern.match(test_value))
        current_cbc = valid_cbc_id[0]
        try:
            if res.string == test_value:
                if int(test_value[:2]) != int(current_cbc):
                    error_msg = "two digit code on this ID does not match submission (expected CBC code: " + str(current_cbc) + ")"
                    self.write_error_msg(test_value,column_name,error_msg,row_number,error_stat)
        except Exception:
            self.write_error_msg(test_value,column_name,error_msg,row_number,error_stat)
    def is_numeric(self,column_name,na_allowed,test_value,lower_lim,error_msg,row_number,error_stat):   #writes error if test_value is not a number or N/A if allowed
        if (na_allowed == True):
             if (test_value =='N/A') or (test_value != test_value):
                 test_value = 10000
        try:
            if float(test_value) >= lower_lim:
                pass
            else:
                self.write_error_msg(test_value,column_name,error_msg,row_number,error_stat)
        except ValueError:
            self.write_error_msg(test_value,column_name,error_msg,row_number,error_stat)
    def is_date_time(self,column_name,test_value,na_allowed,error_msg,row_number,error_stat):           #writes an error if test_value is not a date or time, or N/A if allowed
        if (na_allowed == True) & (test_value == 'N/A'):
            pass
        else:
            try:
                parse(test_value, fuzzy=False)
            except ValueError:
                self.write_error_msg(test_value,column_name,error_msg,row_number,error_stat)
    def is_string(self,column_name,test_value,na_allowed,error_msg,row_number,error_stat):              #see if the value is a string (check for initals)
        if (na_allowed == True) & ((test_value != test_value) | (test_value == "N/A")):       #value is N/A
            pass
        elif (na_allowed == False) & ((test_value != test_value) | (test_value == "N/A")):    #value is N/A
            self.write_error_msg(test_value,column_name,error_msg,row_number,error_stat)
        else:
            logic_check = string_logic_check(test_value)
            if logic_check == False:
                self.write_error_msg(test_value,column_name,error_msg,row_number,error_stat)
    def check_icd10(self,column_name,test_value,error_msg,row_number,error_stat):  #checks if test_value is valid ICD10 code, writes error if not
        if icd10.exists(test_value):
            pass
        elif (test_value != test_value) or (test_value == "N/A"):
            pass
        else:
            self.write_error_msg(test_value,column_name,error_msg,row_number,error_stat)
            
    def is_required(self,column_name,test_value,req_type,row_number,error_stat):        #creates error and warning messages
        if error_stat == 'Error':
            if req_type.lower() == 'all':
                error_msg = "Missing Values are not allowed for this column, please check data"
            elif req_type.lower() == "sars_pos":
                error_msg = "Missing Values are not allowed for SARS_CoV-2 Positive Participants, please check data"
            elif req_type.lower() == "sars_neg":
                error_msg = "Missing Vales are not allowed for SARS_CoV-2 Negative Participants, please check data"
            else:
                error_msg = " ";
        elif error_stat == 'Warning':
            if req_type.lower() == 'all':
                error_msg = "Missing Values were found, please check data"
            elif req_type.lower() == "sars_pos":
                error_msg = "Missing Values were found for SARS_CoV-2 Positive Participants, please check data"
            elif req_type.lower() == "sars_neg":
                error_msg = "Missing Vales were found for SARS_CoV-2 Negative Participants, please check data"
            else:
                error_msg = " ";
        self.write_error_msg(test_value,column_name,error_msg,row_number,error_stat)
        
    def check_required(self,missing_logic,header_name,neg_status,pos_status):       #if column if only required for positive or negative Participants
        neg_test_value = self.Data_Table[self.neg_list_logic & missing_logic][header_name]
        for i in enumerate(neg_test_value):
            self.is_required(header_name,i[1],"SARS_Neg",neg_test_value.index[i[0]],neg_status)
        
        pos_test_value = self.Data_Table[self.pos_list_logic & missing_logic][header_name]
        for i in enumerate(pos_test_value):
            self.is_required(header_name,i[1],"SARS_Pos",pos_test_value.index[i[0]],pos_status)
    def getKey(self,item):   #sorts the outputed data by the 3rd column (row value)
        return item[2]
    
    def write_error_file(self,file_name,s3_resource,temp_path):
        file_path = temp_path + "/" + file_name
    
        sort_list = sorted(self.error_list_summary[1:], key = self.getKey)
        
        self.error_list_summary = [self.error_list_summary[0]]
        error_count = len(sort_list)
        if error_count > 0: 
            msg_status = list(zip(*sort_list))[0]
            err_cnt = msg_status.count('Error')
            warn_cnt = msg_status.count('Warning')
        else:
            err_cnt = 0
            warn_cnt = 0
        
        print(self.File_name + " containing " + str(self.file_size) + " rows has been checked for errors and warnings")
        print(str(err_cnt) + ' errors were found, and ' + str(warn_cnt) + ' warnings were found.') 
            
        if (error_count + warn_cnt) > 0:  
            print('A file has been created\n')
     
            with open(file_path, 'w', newline='') as csvfile:
                submission_errors = csv.writer(csvfile, delimiter=',')
                submission_errors.writerow(self.error_list_summary[0])
                for file_indx in enumerate(sort_list):
                    if file_indx[1][0] == 'Error':
                        submission_errors.writerow(file_indx[1])
                submission_errors.writerow([' ']*len(file_indx[1]))
                for file_indx in enumerate(sort_list):
                    if file_indx[1][0] == 'Warning':
                        submission_errors.writerow(file_indx[1])
                        
            s3_file_path = self.Error_dest_key + "/" + file_name
            s3_resource.meta.client.upload_file(file_path, self.File_Bucket, s3_file_path)
        return err_cnt