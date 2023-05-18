import logging
from urllib.parse import urlencode, urlunsplit
import logging
import pandas as pd
import requests
from typing import Optional
import sqlalchemy as db
from collections import ChainMap
import mysql.connector
from mysql.connector import errorcode
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy import Column, Integer, String, DateTime
from sqlalchemy import create_engine

logger = logging.getLogger(__name__)

nameMap = {"gene_symbol": "GeneSymbol", "date_of_experiment": "date_of_experiment",
           "experiment_source_id": "experiment_name", "external_sample_id": "organism_name",
           "sex": "sex", "weight_days_old": "age", "zygosity": "zygosity", "colony_id": "JRNumber",
           "life_stage_name": "study", "procedure_stable_id": "IMPC_CODE"}


def connect_to_db(user: str,
                  password: str,
                  server: str,
                  database: str):
    """
    Function to build connection to backend database
    :return: mysql.connector.connection
    """
    try:
        conn = mysql.connector.connect(host=server, user=user, password=password, database=database)
        logger.info(f"Successfully connected to {database}")
        return conn

    except mysql.connector.Error as err:

        logger.error(err)

    return None


def BFS() -> list[dict]:
    pass


def filer_procedure_by() -> list[dict]:
    pass


def insert_to_db() -> None:
    pass
