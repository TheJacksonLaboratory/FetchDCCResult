import logging
from urllib.parse import urlencode, urlunsplit
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

filters = ["parameterKey", "genotype", "start", "rows"]
keys = {"parameter_stable_id": "ImpcCode", "date_of_birth": "DOB",
        "external_sample_id": "AnimalID", "allele_symbol": "Symbol",
        "download_url": "DownLoadFilePath", "jpeg_url": "JPEG",
        "experiment_source_id": "ExperimentName",
        "colony_id": "JR", "sex": "Sex"}


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


def BFS(graph: dict,
        result: list) -> None:
    """

    """
    if not graph:
        logger.warning("Empty Json Object!")
        return
    # print(graph)
    tempDict_ = {}
    for g in graph:
        for node in g.keys():
            # print(g[node])
            """Ignore the metadata"""
            if isinstance(g[node], list):
                logging.info("Ignore the metadata")
                continue
            """
            If we found a match with keys, add it to the temp dict
            """
            if node in keys:
                logging.debug(f"Adding {node} now")
                tempDict_[keys[node]] = g[node]

        data = pd.Series(tempDict_).to_frame()
        data = data.transpose()
        result.append(data)


def filter_image_by(colonyId: Optional[str],
                    parameter_stable_id: Optional[str],
                    format: Optional[str],
                    indent: Optional[bool],
                    rows: Optional[int]) -> list[pd.DataFrame]:
    """
    Function to build connection to backend database
    return: mysql.connector.connection
    """

    base_url = ""
    result = []

    return result


def insert_to_db(dataset: list[pd.DataFrame]) -> None:
    pass
