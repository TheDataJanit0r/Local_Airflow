
def run_gedat():
    import argparse
    import csv
    import fnmatch
    import logging
    import os
    import pandas as pd
    import paramiko
    import shutil
    import datetime
    from sqlalchemy import create_engine
    from datetime import datetime, timedelta
    from dotenv import load_dotenv
    import pandas as pd
    from airflow.models import Variable


    pg_host =   Variable.get("PG_HOST")
    pg_user = Variable.get("PG_USERNAME_WRITE")
    pg_password =  Variable.get("PG_PASSWORD_WRITE")
    pg_database =  Variable.get("PG_DATABASE")
    pg_connect_string = f"postgresql://{pg_user}:{pg_password}@{pg_host}/{pg_database}"
    pg_engine = create_engine(f"{pg_connect_string}", echo=False, pool_pre_ping=True, pool_recycle=800)

    ############ Create list of customers based on agreed format
    ############ with gedat
    connection = pg_engine.connect()
    
    query = f"""
                    select * 
                    from prod_info_layer.gedat_upload
        """
    from sqlalchemy import text
    df= pd.read_sql(query,con=pg_engine)
    logging.info(f'Data loaded, closing connection and tunnel')



    dt_string = (datetime.now() - timedelta(days=0)).strftime("%Y-%m-%d-%H-%M-00")



    filename = f"Kunden_{dt_string}.txt"
    df.to_csv(filename, sep="@", index=False
        #     , encoding="windows-1252"
    #           , encoding="iso8859-15"
    #                     , encoding="iso8859-15"
            
                        , encoding="latin9",errors='replace'
            , header=False,quoting=csv.QUOTE_ALL,
            escapechar='"',
            line_terminator='\r\n')

    ########## Upload List of customers to SFTP

    path = '/kollex-transfer/gfgh/gedat/kollex'
    SFTP_HOST = Variable.get("SFTP_HOST")
    SFTP_USER = Variable.get("SFTP_USER")
    SFTP_PASS = Variable.get("SFTP_PASS")


    ########## Upload the table to the SFTP
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(SFTP_HOST, username=SFTP_USER, password=SFTP_PASS)

    sftp = client.open_sftp()
    sftp.put(f"{filename}", f"{path}/{filename}")
    logging.info(f'Uploaded: {filename} to {path}')
    ########## Remove all Kunden files that were created to be uploaded to gedat

    import glob
    files_to_remove =glob.glob('Kunden_*.txt')
    import os

    [os.remove(file) for file in files_to_remove]

    sftp.close()


    ########## Download all files from SFTP

    path = '/kollex-transfer/gfgh/gedat/GEDAT/'
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(SFTP_HOST, username=SFTP_USER, password=SFTP_PASS)



    sftp = client.open_sftp()



    files= list(sftp.listdir(path))
    files_to_move = list(filter(lambda x :x if '.txt' in x else None,files))


    ############### Move everything to History
    for file in files_to_move:

        sftp.rename(f"{path}/{file}", f"{path}/history/{file}")
        # logging.info(f'Moved {file} to /history folder on remote')
    ############### Download everthing from History
    import os 
    if not os.path.exists("gedat"):
        os.makedirs("gedat")
    os.chdir("gedat")
    path = '/kollex-transfer/gfgh/gedat/GEDAT/history' 
    files= list(sftp.listdir(path))
    for file in files:
        # print(f"{path}/{file}")
        sftp.get(f"{path}/{file}", f"{file}")
        # logging.info(f'Downloaded locally: {file}')
        
        # sftp.rename(f"{path}/{file}", f"{path}/history/history/{file}")
        # logging.info(f'Moved {file} to /history folder on remote')
    import glob 

    Downloaded_files =glob.glob('Kunden_result*.txt')

    df_to_upload = pd.DataFrame()



    ########### Appending these TXT files into one Dataframe
    col_names = [
            'KOLLEX_ID',
            'KOLLEX_ADR_ID',
            'GEDAT_ID',
            'GEDAT_NAME_1',
            'GEDAT_NAME_2',
            'GEDAT_STRASSE_1',
            'GEDAT_PLZ',
            'GEDAT_ORT',
            'GEDAT_GT',
            'GEDAT_TEL',
            'GEDAT_EMAIL',
            'EXPDATE'
        ]
    for file in Downloaded_files:
     df_to_upload= df_to_upload.append(pd.read_csv(file, sep = "	",names=col_names))


    #### Lowering the case of the columns and removing headers from DF
    df_to_upload=df_to_upload.rename(columns=str.lower)
    df_to_upload = df_to_upload[df_to_upload['kollex_id']!='KOLLEX_ID']

    ########## Upload results to DWH
    df_to_upload.to_sql( con = pg_engine
                        ,name='gedat_results'
                        ,schema='sheet_loader'
                        ,if_exists='replace'
                        ,index=False)
    pg_engine.dispose()

    ### remove all trash files

    files_to_remove =glob.glob('Kunden_*.txt')
    import os
    ########## Remove all Kunden files that were downloaded
    [os.remove(file) for file in files_to_remove]
    os.chdir('..')
    os.rmdir("gedat")