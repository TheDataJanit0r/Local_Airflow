import argparse
import csv
import fnmatch
import logging
import os
import pandas as pd
import paramiko
import shutil
import datetime

from datetime import datetime, timedelta
from dotenv import load_dotenv
from utils import db, generic, google

load_dotenv()


def run(db_update):
    logging.getLogger().setLevel(logging.INFO)
    dt_string = (datetime.now() - timedelta(days=0)) \
        .strftime("%Y-%m-%d-%H-%M-00")

    filename = f"Kunden_{dt_string}.txt"

    query = """
                select * from
                prod_reporting_layer.gedat_batches
                where "Kurzbezeichnung"
                not like '%%Test%%'
                                    """

    logging.info(f'Creating DWH engine..')
    engine = db.postgres_engine_factory()
    logging.info(f'Reading from DWH with SQL query..')
    df = pd.read_sql_query(query, engine)
    logging.info(f'Data loaded, closing connection and tunnel')
    engine.dispose()

    df.to_csv(os.path.join(generic.get_gedat_path(), filename),
              sep="@",
              index=False,
              encoding="latin9", errors='replace',
              header=False, quoting=csv.QUOTE_ALL,
              escapechar='"',
              line_terminator='\r\n')

    path = '/kollex-transfer/gfgh/gedat/kollex'
    sftp_host = os.environ.get('SFTP_HOST')
    sftp_user = os.environ.get('SFTP_USER')
    sftp_pass = os.environ.get('SFTP_PASS')
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(sftp_host, username=sftp_user, password=sftp_pass)

    sftp = client.open_sftp()
    sftp.put(f"{os.path.join(generic.get_gedat_path(), filename)}",
             f"{path}/{filename}")
    logging.info(f'Uploaded: {filename} to {path}')
    sftp.close()

    path = '/kollex-transfer/gfgh/gedat/GEDAT'
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(sftp_host, username=sftp_user, password=sftp_pass)

    sftp = client.open_sftp()

    files = []
    for filename in sftp.listdir(path):
        if fnmatch.fnmatch(filename, "*.txt"):
            files.append(filename)

    kunden_path = generic.get_gedat_path()

    for file in files:
        local_download_kunden = os.path.join(kunden_path, file)
        sftp.get(f"{path}/{file}", f"{local_download_kunden}")
        logging.info(f'Downloaded locally: {file}')

        sftp.rename(f"{path}/{file}", f"{path}/history/{file}")
        logging.info(f'Moved {file} to /history folder on remote')

    sftp.close()

    kunden_path = generic.get_gedat_path()
    files = os.listdir(kunden_path)
    final_df = pd.DataFrame()
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
    logging.info("Reading gedat results...")

    for file in files:
        if file.startswith("Kunden_result"):
            df1 = pd.read_csv(os.path.join(kunden_path, file),
                              sep="\t",
                              usecols=col_names)
            final_df = final_df.append(df1)
        shutil.move(os.path.join(kunden_path, file),
                    generic.get_gedat_out_path())

    logging.info("Append to existing sheet.")

    # append to gedat_results sheet
    google.append_to_existing_sheet("gedat_results",
                                    "gedat_results",
                                    final_df,
                                    col_names)
    if db_update:
        query = """select * from sheet_loader.gedat_results"""
        gedat_results = pd.read_sql_query(query, engine)
        current_dates = gedat_results['EXPDATE'].tolist()
        new_dates = final_df['EXPDATE'].tolist()
        missing_dates = list(set(new_dates) - set(current_dates))
        filter_df = final_df[final_df['EXPDATE'].isin(missing_dates)]
        filter_df.to_sql(
            "gedat_results",
            schema="sheet_loader",
            con=engine,
            if_exists="append",
            method="multi"
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Preparing Gedat Batch"
    )
    parser.add_argument(
        '--db',
        dest="db",
        help="Update results to database directly",
        action="store_false"
    )

    args = parser.parse_args()
    run(args.db)
