import argparse
import logging
import pandas as pd
from datetime import datetime
from utils.db import mysql_engine_factory, postgres_engine_factory

logging.getLogger().setLevel(logging.INFO)

try:
    from pandas import read_sql_table
except ImportError:
    logging.warn("Please, check if Pandas library is installed.")
try:
    from sqlalchemy import create_engine, types
except ImportError:
    logging.warn("Please, check if SqlAlchmey library is installed.")
try:
    from sqlalchemy.exc import SQLAlchemyError
except ImportError:
    logging.warn("Please, check if SqlAlchmey library is installed.")
try:
    from psycopg2.extensions import register_adapter
except ImportError:
    logging.warn("Please, check if psycopg2 library is installed.")
try:
    from psycopg2.extras import Json
except ImportError:
    logging.warn("Please, check if psycopg2 library is installed.")
try:
    from psycopg2 import connect
except ImportError:
    logging.error("Please, check if psycopg2 library is installed.")
try:
    from boto3 import client as boto3_client
except ImportError:
    logging.warn("Please, check if AWS Boto3 library is installed.")


def cleaning_data(mysql_data):
    mysql_data['id'] = mysql_data['id']. \
        apply(lambda x: bytes(x, 'utf-8').decode("utf-8", errors="replace").replace("\x00", "\uFFFD"))
    mysql_data['import_id'] = mysql_data['import_id']. \
        apply(lambda x: bytes(x, 'utf-8').decode("utf-8", errors="replace").replace("\x00", "\uFFFD"))
    mysql_data['manufacturer'] = mysql_data['manufacturer']. \
        apply(lambda x: bytes(x, 'utf-8').decode("utf-8", errors="replace").replace("\x00", "\uFFFD"))
    mysql_data['gfgh_product_id'] = mysql_data['gfgh_product_id']. \
        apply(lambda x: bytes(x, 'utf-8').decode("utf-8", errors="replace").replace("\x00", "\uFFFD"))
    mysql_data['sales_unit_pkgg'] = mysql_data['sales_unit_pkgg']. \
        apply(lambda x: bytes(x, 'utf-8').decode("utf-8", errors="replace").replace("\x00", "\uFFFD"))
    mysql_data['name'] = mysql_data['name']. \
        apply(lambda x: bytes(x, 'utf-8').decode("utf-8", errors="replace").replace("\x00", "\uFFFD"))


def db_sync(mysql_db, mysql_table, pg_db, pg_schema, pg_table):
    logging.info('DB sync started with parameters:\n'
                 f'Source: {mysql_db} {mysql_table}\n'
                 f'Destination: {pg_db} {pg_schema} {pg_table}'
                 )
    mysql_engine = mysql_engine_factory(db=mysql_db)
    pg_engine = postgres_engine_factory(db=pg_db)
    dst_ids_query = f"""select id from {pg_schema}.{pg_table}"""
    src_ids_query = f"""select id from {mysql_db}.{mysql_table}"""
    try:
        logging.info('Reading ids from Destination table')
        pg_ids = pd.read_sql_query(dst_ids_query, pg_engine)
        logging.info('Reading ids from Source table')
        src_ids = pd.read_sql_query(src_ids_query, mysql_engine)

    except (ValueError, SQLAlchemyError) as e:
        logging.error(e)
        return
    else:
        dst_ids_list = pg_ids['id'].tolist()
        src_ids_list = src_ids['id'].tolist()
        logging.info('Finding missing ids')
        missing_ids_list = list(set(src_ids_list) - set(dst_ids_list))
        missing_ids_list = ','.join(
            [f"\'{str(_id)}\'" for _id in missing_ids_list]
        )
        if missing_ids_list:
            mysql_data_query = f"""
                  select * from {mysql_table} 
                  where id in ({missing_ids_list})
              """
        else:
            logging.info('Everything is up to date. No rows to update.')
            return
    try:
        logging.info('Reading data from Source table')
        mysql_data = pd.read_sql_query(mysql_data_query, mysql_engine)
    except (ValueError, SQLAlchemyError) as e:
        logging.error(e)
        return
    else:
        mysql_data["_updated_at"] = datetime.now()
        number_of_rows = len(mysql_data.index)
        logging.info('Writing source data to destination')
        try:
            mysql_data.to_sql(
                pg_table,
                dtype={'raw_values': types.JSON, 'data': types.JSON},
                con=pg_engine,
                if_exists="append",
                method="multi",
                schema=pg_schema
            )
        except (ValueError, SQLAlchemyError) as e:
            logging.info("Null characters found. Cleaning data process")
            cleaning_data(mysql_data)
        logging.info(f'Number of added rows to Destination table: '
                     f'{number_of_rows}')
        logging.info('DB synchronization finished')


def empty_table(pg_schema, pg_table, engine=None):
    logging.info('Empty table {}.{}'.format(pg_schema, pg_table))
    if not engine:
        engine = postgres_engine_factory()
    with engine.connect() as conn:
        conn.execute("DROP TABLE IF EXISTS {}.{}".format(pg_schema, pg_table))
        logging.info("Table {}.{}, emptied before adding updated data.".format(
            pg_schema, pg_table))


def copy_n_paste(mysql_db, mysql_table, pg_db, pg_schema, pg_table, chunk_size=10000):
    logging.info('DB Copy and Paste started with parameters:\n'
                 f'Source: {mysql_db} {mysql_table}\n'
                 f'Destination: {pg_db} {pg_schema} {pg_table}'
                 )
    mysql_engine = mysql_engine_factory(db=mysql_db)
    pg_engine = postgres_engine_factory(db=pg_db)
    try:
        logging.info('Reading data from Source table')
        df = read_sql_table(mysql_table, con=mysql_engine, chunksize=chunk_size)
    except (ValueError, SQLAlchemyError) as e:
        logging.error(e)
    else:
        empty_table(pg_schema, pg_table, pg_engine)
        logging.info(f'Started writing data in chunks with chunksize {chunk_size}')
        for i, df_chunk in enumerate(df):
            print(i, df_chunk.shape)
            if not df_chunk.empty:
                # TODO fix this and make dtype flexible ( dict(column,table))
                # col_dtype = {dtype_column: types.JSON} if pg_table == dtype_table else None
                df_chunk["_updated_at"] = datetime.now()
                df_chunk.to_sql(pg_table,
                                dtype={'raw_values': types.JSON,
                                       'data': types.JSON},
                                con=pg_engine,
                                chunksize=chunk_size,
                                if_exists="append",
                                method="multi",
                                schema=pg_schema
                                )
        logging.info('DB Copy and Paste finished')


if __name__ == "__main__":
    start_time = datetime.now()
    logging.info(f'DB copy and paste started at {start_time}')

    parser = argparse.ArgumentParser(description='DB COPY')
    parser.add_argument(
        '--mysql-db',
        dest='mysql_db',
        type=str,
        help='MySQL database',
    )
    parser.add_argument(
        '--mysql-table',
        dest='mysql_table',
        type=str,
        help='MySQL table which we want to copy',
    )
    parser.add_argument(
        '--pg-db',
        dest='pg_db',
        type=str,
        help='PG database',
    )
    parser.add_argument(
        '--pg-schema',
        dest='pg_schema',
        type=str,
        help='PG schema',
    )
    parser.add_argument(
        '--pg-table',
        dest='pg_table',
        type=str,
        help='PG tables to use',
    )
    parser.add_argument(
        '--db-sync',
        dest='db_sync',
        help='Using database sync',
        action='store_true'
    )
    args = parser.parse_args()

    if args.db_sync:
        db_sync(
            mysql_db=args.mysql_db,
            mysql_table=args.mysql_table,
            pg_db=args.pg_db,
            pg_schema=args.pg_schema,
            pg_table=args.pg_table,
        )
    else:
        copy_n_paste(
            mysql_db=args.mysql_db,
            mysql_table=args.mysql_table,
            pg_db=args.pg_db,
            pg_schema=args.pg_schema,
            pg_table=args.pg_table,
        )
    stop_time = datetime.now()
    logging.info(f'DB copy and paste finished at {stop_time}')
    logging.info(f'DB copy and paste execution time: {stop_time - start_time}')
