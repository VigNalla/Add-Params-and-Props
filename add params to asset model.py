import os
from datetime import datetime

import cx_Oracle
import pandas
import numpy as np



def get_connection():

    dsn = cx_Oracle.makedsn("dev1-insight-oracle.cdtjnbhmqpam.us-east-1.rds.amazonaws.com",1521, service_name='insight')

    con = cx_Oracle.connect('VIGNESH',
                            password,
                            dsn,
                            threaded=True)

    return con

def insert(query):
    try:
        con = get_connection()
        cur =con.cursor()
        cur.execute(query)
        print('Success')
        con.commit()
        con.close()
    except cx_Oracle.Error as error:
        print('Error occurred(Insert):')
        print(error)

def execute(query):
    con = get_connection()
    cur = con.cursor()
    try:
        cur.execute(query)
        res=cur.fetchone()
        return res
    except cx_Oracle.Error as error:
        print('Error occurred(Select):')
        print(error)



if __name__ == '__main__':
    start_time = datetime.now()
    print(start_time)
    password= input("Password: ")
    new_model_type_prop_ids = []

    cx_Oracle.init_oracle_client(lib_dir=r"instantclient_21_3")

    props = ["CATEGORY","STD_NAME","SOURCE","DUR_CK_ALARM","HIDE_FROM_EXTERNAL_USER","KPI","ALARM_HIHI","ALARM_HI","ALARM_LOLO","ALARM_LO","ALARM_AUTO_ACK","ALARM_AUTO_ACK_DELAY","ALARM_AUTO_CLOSE","ALARM_AUTO_CLOSE_DELAY","DISABLE_ALARM_REMINDER"]

    prop_path = os.path.abspath(r"props.xlsx")
    param_path = os.path.abspath(r"NewParams.xlsx")

    #Reading the data from excel file
    prop_data = pandas.read_excel(prop_path)

    #Replacing the nan values with Null
    prop_data.replace(np.nan, '', inplace=True)

    prop_data = prop_data.iterrows()
    prop_data_dict = {i:[j] for i,j in prop_data}


    #Reading the data from excel file
    param_data = pandas.read_excel(param_path)

    #Replacing the nan values with Null
    param_data.replace(np.nan, '', inplace=True)

    param_data = param_data.iterrows()

    #Looping through all the params
    for i,j in param_data:
        j=j.tolist()

        #Inserting each params
        query_insert = f"INSERT INTO T_MODEL_TYPE_PROPERTY (MODEL_TYPE_ID, NAME,DEFAULT_VALUE, BASE_HISTORIAN_TAG_ID, VALUE_TYPE," \
                f" PROP_UNIT_TYPE_ID, REQUIRED, DEFAULT_UNIT_CODE_ID, DEFAULT_STORAGE_UNIT_CODE_ID) Values ({j[0]},'{j[1]}'," \
                f"{j[2]},'{j[3]}','{j[4]}',{j[5]},'{j[6]}',{j[7]},{j[8]})"
        # print(query_insert)
        insert(query_insert)

        # Getting the Model_type_prop_id of the inserted param
        query_select = f"SELECT MODEL_TYPE_PROP_ID FROM T_MODEL_TYPE_PROPERTY WHERE MODEL_TYPE_ID={j[0]} AND NAME='{j[1]}' AND BASE_HISTORIAN_TAG_ID='{j[3]}'"
        model_type_prop_id = execute(query_select)[0]

        new_model_type_prop_ids.append(model_type_prop_id)

        # print(new_model_type_prop_ids)
        new_data= prop_data_dict[i][0].tolist()

        #Looping through all the props of the inserted param
        for i in range(len(props)):

            if type(new_data[i]) is not int:
                if new_data[i]== 'Null':
                    new_data[i]=''
                    new_data[i] = f"'{new_data[i]}'"
                else:
                    new_data[i] = f"'{new_data[i]}'"
            query = f'''INSERT INTO T_MODEL_TYPE_PROP_ATTRIBUTE (MODEL_TYPE_PROP_ID, NAME, VALUE) VALUES ({model_type_prop_id},'{props[i]}',{new_data[i]})'''
            # print(query)
            insert(query)
    end_time = datetime.now()
    dict = {'Model Type ID':new_model_type_prop_ids}
    df = pandas.DataFrame(dict)
    df.to_csv(r'Model_IDs_preprod.csv')
    print(end_time)
    print(new_model_type_prop_ids)
    print(f'Task completed in {end_time-start_time}')