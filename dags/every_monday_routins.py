from datetime import datetime, timedelta



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
from sqlalchemy import create_engine
from dotenv import load_dotenv
import requests
from include.dbt_run_all_layers import dbt_run_all_layers

import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash import BashOperator

from include.monday_api import run_monday_api



   
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


with DAG(
    dag_id="every_monday_routins",
    start_date=datetime.today() - timedelta(days=1),
    schedule_interval="0 0 * * 1",
    concurrency=100,
     catchup=False
) as dag:

    # t1, t2 and t3 are examples of tasks created by instantiating operators
    data_dog_log 	=  BashOperator        (
                                            task_id='Started_All_SKUs_DAG',
                                            bash_command='echo "{{ task_instance_key_str }} {{ ts }}"',
                                            dag=dag,
                                            
                                            )
    run_monday_api	= PythonOperator(
                                            task_id='run_monday_api'
                                            , python_callable=run_monday_api
                                            , retries=5
                                            )
    
data_dog_log >> run_monday_api# >>dbt_job_run_all_layers>>data_dog_log_final


    