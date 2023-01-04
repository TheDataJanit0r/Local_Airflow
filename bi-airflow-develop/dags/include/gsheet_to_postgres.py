def run_gsheet_load(**kwargs):
    import gspread as gs
    import pandas as pd
    import os

    from gspread_dataframe import get_as_dataframe
    from sqlalchemy import create_engine, types
    from dotenv import load_dotenv
    from airflow.models import Variable
    # os.chdir('include')
    
    # load_dotenv('enviroment_variables.env')
    
    gsheet_credentials={
        
        "type": Variable.get("gsheet_creds_type"),
        "project_id":Variable.get("gsheet_creds_project_id"),
        "private_key_id":Variable.get("gsheet_creds_private_key_id"),
        "private_key": Variable.get("gsheet_creds_private_key"),
        "client_email":Variable.get("gsheet_creds_client_email"),
        "client_id": Variable.get("gsheet_creds_client_id"),
        "auth_uri": Variable.get("gsheet_creds_auth_uri"),
        "token_uri": Variable.get("gsheet_creds_token_uri"),
        "auth_provider_x509_cert_url":Variable.get("gsheet_creds_auth_provider_x509_cert_url"),
        "client_x509_cert_url": Variable.get("gsheet_creds_client_x509_cert_url")
    }
    # print(type(gsheet_credentials))
    # for key, value in gsheet_credentials.items():
    #     print(key, ' : ', value)
    gc = gs.service_account_from_dict(gsheet_credentials)
    #gc = gs.service_account(filename='gsheet-loading-into-dwh-d51d3a31aff8.json')
    print('loaded credentials')
    
    sh =  gc.open_by_url(kwargs['url'] )
    #sh = gc.open_by_url('https://docs.google.com/spreadsheets/d/1BZCcB5m66lkrhY2_kmTVuFYaMbPHA1pMEMz7prqLHiw/edit#gid=984401877')
    
    ws   = sh.worksheet(kwargs['sheet_name'] )
    #ws = sh.worksheet('kollex express (Coca-Cola)')
    pg_host =  Variable.get('PG_HOST')
    pg_database = Variable.get('PG_DATABASE')

    pg_user = Variable.get('PG_USERNAME_WRITE')

    pg_password = Variable.get('PG_PASSWORD_WRITE')
    pg_tables_to_use = kwargs['pg_tables_to_use']
    pg_schema =kwargs['pg_schema']  # Variable.get('PG_SCHEMA')
    pg_connect_string = f"postgresql://{pg_user}:{pg_password}@{pg_host}/{pg_database}"
    pg_engine = create_engine(f"{pg_connect_string}", echo=False)
    chunk_size = 1000  # Variable.get('CHUNK_SIZE')
    print(f'reading sheet {pg_tables_to_use}')
    
    df =  get_as_dataframe(ws,include_column_header=True,evaluate_formulas =True)
    df.head()
    df = df.loc[:,~df.columns.str.contains('^Unnamed', case=False)] 
    df.dropna(axis=0,inplace=True,how='all')  



    
    # Logging
   

    connection = pg_engine.connect()
    connection.execute(f"drop table if exists {pg_schema}.{pg_tables_to_use};")
    print(f'dropped table {pg_schema}.{pg_tables_to_use}')

    df.to_sql(pg_tables_to_use, pg_engine,schema=pg_schema, if_exists='replace',index=False)
    print(f'finished writing table {pg_schema}.{pg_tables_to_use}')