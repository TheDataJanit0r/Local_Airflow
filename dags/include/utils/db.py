import logging
import os
import psycopg2
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.engine.base import Engine

load_dotenv()
logging.root.setLevel(logging.ERROR)


def mysql_engine_factory(
        host=os.environ.get('MYSQL_HOST'),
        port=os.environ.get('MYSQL_PORT'),
        db=os.environ.get('MYSQL_DATABASE'),
        username=os.environ.get('MYSQL_USERNAME'),
        password=os.environ.get('MYSQL_PASSWORD')
) -> Engine:
    """
    Create a database engine from a dictionary of database info.
    """
    try:
        engine = create_engine(f"mysql+pymysql://{username}:{password}@{host}:{port}/{db}")
    except psycopg2.OperationalError:
        logging.error(f"OperationalError check hostname: {host}")
        engine = None

    return engine


def postgres_engine_factory(
        host=os.environ.get("PG_HOST"),
        port=os.environ.get("PG_PORT"),
        db=os.environ.get("PG_DATABASE"),
        username=os.environ.get("PG_USERNAME_WRITE"),
        password=os.environ.get("PG_PASSWORD_WRITE")
) -> Engine:
    """
    Create a database engine from a dictionary of database info.
    """
    return create_engine(
        f"postgresql://{username}:{password}@{host}:{port}/{db}"
    )
