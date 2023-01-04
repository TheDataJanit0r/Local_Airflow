import logging
from datetime import datetime
from json import JSONDecodeError
from os import environ
import pandas as pd
import requests
import os
from sqlalchemy import create_engine

from airflow.models import Variable

def process_monday_api_board(board_name, board_id, limit):
   
    #load_dotenv('enviroment_variables.env')
    api_key = Variable.get("MONDAY_TOKEN")
    api_url = Variable.get("MONDAY_API_URL")
    headers = {'Authorization': api_key}


    pg_host =  Variable.get("PG_HOST")
    pg_user =  Variable.get("PG_USERNAME_WRITE")
    pg_password =  Variable.get("PG_PASSWORD_WRITE")



    pg_database = Variable.get("PG_DATABASE")
    pg_schema = Variable.get("PG_REPORTING_SCHEMA")
    pg_connect_string = f"postgresql://{pg_user}:{pg_password}@{pg_host}/{pg_database}"
    
    engine = create_engine(f"{pg_connect_string}", echo=False)
    logging.info('Getting {} board from monday.com...'.format(board_name))
    page = 1
    all_res = []
    while True:
        logging.info('Board {}, page {}'.format(board_name, page))
        query = f"""{{
            boards(ids:{board_id}){{
            name id description items (limit:{limit}, page:{page}) {{
                name column_values {{
                    title text 
                    }} 
                }} 
            }} 
        }}"""
        data = {'query': query}
        logging.info(f"Send request to {api_url}")
        r = requests.post(url=api_url, json=data, headers=headers)
        page += 1
        logging.info("Reading data...")
        try:
            for item in r.json()['data']['boards'][0]['items']:
                all_res.append(item)
            if len(r.json()['data']['boards'][0]['items']) < limit:
                break
        except JSONDecodeError as e:
            logging.error(e.msg)
            break
           # return False

    df = pd.DataFrame(all_res)
    logging.info('Got {} rows. Now transforming columns...'.format(len(df)))
    addies = list()
    corr = pd.DataFrame()
    for index, row in df.iterrows():
        merchant = row['name']
        corr = pd.DataFrame.from_records(row['column_values']).T
        corr.columns = corr.iloc[0]
        corr = corr[1:]
        corr['name'] = merchant
        addies.append(corr)
    adds = pd.concat(addies)
    bf = df.merge(adds, on="name").reset_index()
    col_list = corr.columns.tolist()
    col_list.remove("name")
    new_list = ["name"]
    proper_list = new_list + col_list
    final_df = bf[proper_list].copy()

    logging.info(f"Done. Now uploading to DWH...")
    final_df["_updated_at"] = datetime.now()
    final_df.to_sql(
        board_name,
        con=engine,
        schema= Variable.get("PG_REPORTING_SCHEMA"),
        chunksize=400,
        method="multi",
        if_exists="replace",
        index=False,
    )
    with engine.connect() as connection:
        connection.execute(f"GRANT USAGE ON SCHEMA prod_reporting_layer TO {pg_user}")
        connection.execute(f"GRANT SELECT ON ALL TABLES IN SCHEMA prod_reporting_layer TO {pg_user}")
    logging.info(f"Finished uploading, closing connection")
    engine.dispose()
    return True









def run_monday_api():
    boards = {
        "monday_funnel": "918285742",
        "monday_coke_signed_board": "684721905",
        "monday_merchant_keys": "921914689"
    }
    limits = {
        "monday_funnel": 200,
        "monday_coke_signed_board": 5,
        "monday_merchant_keys": 20
    }

    for board_name, board_id in boards.items():
        limit = limits[board_name]
        board_processed = process_monday_api_board(board_name, board_id, limit)
        while not board_processed:
            board_processed = process_monday_api_board(board_name, board_id, limit)

    return

