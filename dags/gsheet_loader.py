from datetime import datetime, timedelta
import airflow
from airflow import DAG


import psycopg2
import csv
import io
#from tkinter.messagebox import QUESTION
import mysql.connector
import pandas as pd
import os
import numpy as np
import time
import io
import csv
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
import requests
from dotenv import load_dotenv
from include.gsheet_to_postgres import run_gsheet_load



default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # 'trigger_rule': 'all_success'
}
def dbt_run():
    os.chdir('include')
    
    load_dotenv('enviroment_variables.env')
    myToken = os.getenv('dbt_token')
    myUrl = 'https://cloud.getdbt.com/api/v2/accounts/1335/jobs/2497/run/'

    #string  = {'Authorization': 'token {}'.format(myToken),'cause' :'Kick Off From Testing Script'}
    head ={'Authorization': 'token {}'.format(myToken)}
    body ={'cause' :'Kick Off From Testing Script'}
    r = requests.post(myUrl, headers=head,data=body)
    r_dictionary= r.json()
    print(r.text)

with DAG(
    dag_id="gsheet_loader",
    start_date=datetime.today() - timedelta(days=1),
    schedule_interval="0 */1 * * *",
    catchup=False,

    concurrency=100
) as dag:


    # t1, t2 and t3 are examples of tasks created by instantiating operators
    data_dog_log = DummyOperator(task_id='data_dog_log', retries=3)
    COPY_QR_KOLLEX_EXPRESS = PythonOperator(
                                                            task_id='COPY_QR_KOLLEX_EXPRESS'
                                                        , python_callable=run_gsheet_load,
                                                          op_kwargs={'pg_schema': 'kollex_express_cola_customers'
                                                                    , 'pg_tables_to_use': 'kollex_express'
                                                                    ,'url' :'https://docs.google.com/spreadsheets/d/1BZCcB5m66lkrhY2_kmTVuFYaMbPHA1pMEMz7prqLHiw/edit#gid=984401877'
                                                                    , 'sheet_name':'kollex express (Coca-Cola)'
                                                                   }, retries=5)
    COPY_QR_KOLLEX_SHOP = PythonOperator(
                                                                task_id='COPY_QR_KOLLEX_SHOP'
                                                                , python_callable=run_gsheet_load,
                                                           op_kwargs={'pg_schema': 'kollex_express_cola_customers'
                                                                    , 'pg_tables_to_use': 'kollex_shop'
                                                                    ,'url' :'https://docs.google.com/spreadsheets/d/1BZCcB5m66lkrhY2_kmTVuFYaMbPHA1pMEMz7prqLHiw/edit#gid=984401877'
                                                                    , 'sheet_name':'kollex (Coca-Cola)'
                                                                   }, retries=5
                                        )
    COPY_QR_KOLLEX_EXPRESS_SHEET_LOADER = PythonOperator(
                                                            task_id='COPY_QR_KOLLEX_EXPRESS_SHEET_LOADER'
                                                        , python_callable=run_gsheet_load,
                                                          op_kwargs={'pg_schema': 'sheet_loader'
                                                                    , 'pg_tables_to_use': 'kollex_express_qr_codes'
                                                                    ,'url' :'https://docs.google.com/spreadsheets/d/1BZCcB5m66lkrhY2_kmTVuFYaMbPHA1pMEMz7prqLHiw/edit#gid=984401877'
                                                                    , 'sheet_name':'kollex express (Coca-Cola)'
                                                                   }, retries=5
                                                                   )
    COPY_QR_KOLLEX_SHOP_SHEET_LOADER = PythonOperator (
                                                                task_id='COPY_QR_KOLLEX_SHOP_SHEET_LOADER'
                                                                , python_callable=run_gsheet_load,
                                                           op_kwargs={'pg_schema': 'sheet_loader'
                                                                    , 'pg_tables_to_use': 'kollex_shop_qr_codes'
                                                                    ,'url' :'https://docs.google.com/spreadsheets/d/1BZCcB5m66lkrhY2_kmTVuFYaMbPHA1pMEMz7prqLHiw/edit#gid=984401877'
                                                                    , 'sheet_name':'kollex (Coca-Cola)'
                                                                   }, retries=5
                                            )
    COPY_EXCLUDE_LIST = PythonOperator(
                                                                task_id='COPY_EXCLUDE_LIST'
                                                                , python_callable=run_gsheet_load,
                                                           op_kwargs={'pg_schema': 'stitch_special_cases_to_exclude'
                                                                    , 'pg_tables_to_use': 'special_cases_to_exclude'
                                                                    ,'url' :'https://docs.google.com/spreadsheets/d/1L1M9eo52Ok8OrB8dXRMoY2SMhmbhUdUF2sCW-FeQDU4/edit#gid=1397124555'
                                                                    , 'sheet_name':'special_cases_to_exclude'
                                                                   }, retries=5
                                        )
    COPY_EXCLUDE_LIST_sheet_loader = PythonOperator(
                                                                task_id='COPY_EXCLUDE_LIST_to_SHEET_LOADER'
                                                                , python_callable=run_gsheet_load,
                                                           op_kwargs={'pg_schema': 'sheet_loader'
                                                                    , 'pg_tables_to_use': 'special_cases_to_exclude'
                                                                    ,'url' :'https://docs.google.com/spreadsheets/d/1L1M9eo52Ok8OrB8dXRMoY2SMhmbhUdUF2sCW-FeQDU4/edit#gid=1397124555'
                                                                    , 'sheet_name':'special_cases_to_exclude'
                                                                   }, retries=5
                                        )
    COPY_HOLDING = PythonOperator(



        
                                                                task_id='COPY_HOLDINGS'
                                                                , python_callable=run_gsheet_load,
                                                           op_kwargs={'pg_schema': 'sheet_loader'
                                                                    , 'pg_tables_to_use': 'holdings'
                                                                    ,'url' :'https://docs.google.com/spreadsheets/d/1Pcw0T4smQHbEeAjePBLlW8WNHh79uUj-1ZoHAuw0K50/edit#gid=1547073459'
                                                                    , 'sheet_name':'holdings'
                                                                   }, retries=5
                                        )

                                        
    COPY_MERCHANT_ACTIVE = PythonOperator(
                                                                task_id='COPY_MERCHANT_ACTIVE'
                                                                , python_callable=run_gsheet_load,
                                                           op_kwargs={'pg_schema': 'prod_raw_layer'
                                                                    , 'pg_tables_to_use': 'merchants_active'
                                                                    ,'url' :'https://docs.google.com/spreadsheets/d/1qoMyAAgWpvaXCnR6oQzdBP8Rdz5_axki2uUTxY0XSkI/edit?pli=1#gid=1301213425'
                                                    
                                                                    , 'sheet_name':'active'
                                                                   }, retries=5
                                        )
    COPY_MERCHANT_ON_HOLD = PythonOperator(
                                                                task_id='COPY_MERCHANT_ON_HOLD'
                                                                , python_callable=run_gsheet_load,
                                                           op_kwargs={'pg_schema': 'prod_raw_layer'
                                                                    , 'pg_tables_to_use': 'merchants_on_hold'
                                                                    ,'url' :'https://docs.google.com/spreadsheets/d/1qoMyAAgWpvaXCnR6oQzdBP8Rdz5_axki2uUTxY0XSkI/edit?pli=1#gid=1301213425'
                                                                    , 'sheet_name':'on_hold'
                                                                   }, retries=5
                                        )
    COPY_MERCHANT_NEW = PythonOperator(
                                                                task_id='COPY_MERCHANT_NEW'
                                                                , python_callable=run_gsheet_load,
                                                           op_kwargs={'pg_schema': 'prod_raw_layer'
                                                                    , 'pg_tables_to_use': 'merchants_new'
                                                                    ,'url' :'https://docs.google.com/spreadsheets/d/1qoMyAAgWpvaXCnR6oQzdBP8Rdz5_axki2uUTxY0XSkI/edit?pli=1#gid=1301213425'
                                                                    , 'sheet_name':'new'
                                                                   }, retries=5
                                        )
    data_dog_log_final = DummyOperator(task_id='data_dog_log_final', retries=3,trigger_rule='none_failed')
data_dog_log >> [COPY_QR_KOLLEX_EXPRESS ,COPY_QR_KOLLEX_SHOP ,COPY_EXCLUDE_LIST  ,COPY_QR_KOLLEX_EXPRESS_SHEET_LOADER,  #>> dbt_job_raw_layers#>>run_All_SKUs 
COPY_QR_KOLLEX_EXPRESS_SHEET_LOADER ,COPY_QR_KOLLEX_SHOP_SHEET_LOADER ,COPY_HOLDING ,COPY_MERCHANT_ACTIVE,
COPY_MERCHANT_ACTIVE ,COPY_MERCHANT_ON_HOLD ,COPY_MERCHANT_NEW,COPY_EXCLUDE_LIST_sheet_loader] >>data_dog_log_final
    