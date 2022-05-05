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
from airflow.operators.dummy_operator import DummyOperator



def load_gfgh_data() :
    os.chdir('include')
    
    load_dotenv('enviroment_variables.env')
    mysql_host =  os.getenv('MYSQL_HOST')
    mysql_port =  os.getenv('MYSQL_PORT')
    mysql_schema = os.getenv('MYSQL_DATABASE_akeneo')
    mysql_user = os.getenv('MYSQL_USERNAME')
    mysql_password = os.getenv('MYSQL_PASSWORD')
    mydb = mysql.connector.connect(
        host=mysql_host,
        user=mysql_user,
        password=mysql_password,
        database=os.getenv('MYSQL_DATABASE'),
        pool_size=32
    )


    mycursor = mydb.cursor()
    sql = """ select id \n 
            , sku \n
            , base_unit_content \n
            , base_unit_content_uom \n
            , no_of_base_units \n

            , gtin \n
            , kollex_product_id \n
            , manufacturer \n
            , manufacturer_gln \n
            , manufacturer_id \n
            , flags \n
            , list_price  \n
            , refund_value \n
            , created_at \n
            , updated_at \n
            , gfgh_product_id \n
            , sales_unit_pkgg \n
            , name \n
            , category_code \n
            , direct_sku \n
            , direct_shop_release \n
            , kollex_active \n
            , active \n
            , qa \n
            , was_direct_release \n
        
            , predicted_category \n             
            , predicted_category  as category_predicted\n \n           
            , gfgh_id as merchant_key \n
            , false as special_case \n
            ,  now() as _sdc_extracted_at \n
    from gfghdata.product \n"""
    mycursor.execute(sql)
    query_df = pd.DataFrame(mycursor.fetchall())

    query_df.columns =  [x[0] for x in mycursor.description]
    # print("Before")
    # print(query_df.dtypes)

 
    max_id = 0

    #print(query_df.index.is_unique)


    # query_df.replace(np.nan,None,inplace=True)
    # query_df.replace("NaN",None,inplace=True)
    # query_df.replace("nan",None,inplace=True)
    # query_df.replace("",None,inplace=True)
    pg_database = os.getenv('PG_DATABASE')
    
    ###############Credentials for Using Staging or productionn
    pg_host =  os.getenv('PG_HOST_STAGING')
    pg_user = os.getenv('PG_USERNAME_WRITE_STAGING')
    pg_password = os.getenv('PG_PASSWORD_WRITE_STAGING')
    
    pg_connect_string= f"postgresql+psycopg2://{pg_user}:{pg_password}@{pg_host}/{pg_database}"

    pg_schema ='from_pim'
    pg_tables_to_use= 'cp_gfgh_product'



    pg_engine = create_engine(f"{pg_connect_string}", echo=False)
    connection = pg_engine.connect()

    connection.execute(f"drop table if exists {pg_schema}.{pg_tables_to_use};")
    print(f"dropped table  {pg_schema}.{pg_tables_to_use};")
    query_df.to_sql(os.getenv('PG_PIM_GFGH_TABLE'), pg_engine,schema='from_pim', if_exists='replace',index=False)
    # query_df.head(0).to_sql(pg_tables_to_use, pg_engine,schema=pg_schema, if_exists='replace',index=False)

    # print('inputed the head of the table')
    # print(query_df.head(0))


    # postgres_conn = psycopg2.connect(host=pg_host,
    #                                 user=pg_user
    #                                 , password=pg_password
    #                                 ,database=pg_database
    #                                 ,options="-c search_path=from_pim")
    # Postgres_cursor = postgres_conn.cursor()
    # sio = io.StringIO()
    # writer = csv.writer(sio,delimiter='~')
    # writer.writerows(query_df.values)
    # sio.seek(0)
    # with Postgres_cursor as c:
    #     c.copy_from(
    #         file=sio,
    #         table="cp_gfgh_product",
    #         columns=query_df.columns,
    #         sep="~"
    #     )
    #     postgres_conn.commit()


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
    dag_id="gfgh_all_layers",
    start_date=datetime.today() - timedelta(days=1),
    schedule_interval="0 18-23/4 * * *",
    concurrency=100,
     catchup=False
) as dag:

    # t1, t2 and t3 are examples of tasks created by instantiating operators
    data_dog_log 	= DummyOperator(
                                    task_id='data_dog_log'
                                    , retries=3
                                    )
    copy_gfgh_from_mySQL	= PythonOperator(
                                            task_id='copy_gfgh_from_mySQL'
                                            , python_callable=load_gfgh_data
                                            , retries=5)
    # dbt_job_run_all_layers  = PythonOperator(
    #                                             task_id='dbt_job_run_all_layers'
    #                                             , python_callable=dbt_run_all_layers
    #                                         )
    # data_dog_log_final = DummyOperator(task_id='data_dog_log_final', retries=3)
data_dog_log >> copy_gfgh_from_mySQL# >>dbt_job_run_all_layers>>data_dog_log_final


    