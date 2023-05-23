import datetime
import logging
import urllib
from typing import Optional
from urllib.parse import urlencode, urlunsplit

import mysql.connector
import pandas as pd
import requests
from sqlalchemy import create_engine
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

logger = logging.getLogger("__main__")

ReturnType = {"parameter_stable_id": "ImpcCode", "date_of_birth": "DOB",
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
            if node in ReturnType:
                logging.debug(f"Adding {node} now")
                tempDict_[ReturnType[node]] = g[node]

        data = pd.Series(tempDict_).to_frame()
        data = data.transpose()
        result.append(data)


def filter_image_by(colonyId: Optional[str] = None,
                    parameter_stable_id: Optional[str] = None,
                    formats: Optional[str] = None,
                    indent: Optional[bool] = None,
                    rows: Optional[int] = 0) -> list[pd.DataFrame]:
    """
    Function to build connection to backend database
    return: mysql.connector.connection
    """
    if rows > 2 ** 31 - 1 or rows < 0:
        logger.error("Invalid start or result size")
        return []

    dict_ = {"rows": rows}
    if colonyId:
        dict_["colony_id"] = colonyId

    if parameter_stable_id:
        dict_["parameter_stable_id"] = parameter_stable_id

    if formats:
        dict_["wt"] = indent

    filters = []
    for key, val in dict_.items():
        filter_ = key + ":" + val
        logger.debug(f"Add condition {filter_}")
        filters.append(filter_)

    selectCondition = " AND ".join(filters)
    logger.debug(f"Select condition is :{selectCondition}")
    params = {"q": selectCondition, "fl": ReturnType, "rows": rows}
    query = urllib.parse.urlencode(params, doseq=True, safe=', :').replace("+", " ")
    url = urlunsplit(("https", "www.ebi.ac.uk", "/mi/impc/solr/experiment/select", query, ""))
    print(url)
    logger.debug(f"URL is {url}")

    try:
        payload = {}
        headers = {}
        response = requests.request("GET", url, headers=headers, data=payload)
        json_objects = response.json()["response"]["docs"]
        result = []
        BFS(json_objects, result)
        return result

    except requests.exceptions.HTTPError as err1:
        error = str(err1.__dict__)
        print(error)

    except requests.exceptions.ConnectionError as err2:
        error = str(err2.__dict__)
        print(error)

    except requests.exceptions.Timeout as err3:
        error = str(err3.__dict__)
        print(error)

    except requests.exceptions.RequestException as err4:
        error = str(err4.__dict__)
        print(error)


def insert_to_db(dataset: list[pd.DataFrame],
                 user: str,
                 password: str,
                 server: str,
                 database: str) -> None:

    if not dataset:
        logger.warning("No record retrieved!")
        return

    temp = []
    for data in dataset:
        data = pd.Series(data).to_frame()
        data = data.transpose()
        temp.append(data)

    df = pd.concat(temp)

    df.drop("procedureKey", axis=1, inplace=True)
    currTime = [datetime.datetime.now() for i in range(len(df.index))]
    insertData = df.copy()
    insertData["modifiedTime"] = pd.Series(currTime).values

    try:
        engine = create_engine("mysql+mysqlconnector://{0}:{1}@{2}/{3}".
                               format(user, password, server, database),
                               pool_recycle=3600,
                               pool_timeout=57600,
                               future=True)
        # print(df)
        df.to_sql("ebiimages", engine, if_exists='append', index=False, chunksize=1000)
        result = engine.connect().execute(text("SELECT COUNT(*) FROM komp.ebiimages;"))
        logger.debug(f"Number of rows in table is {result.first()[0]}")

    except SQLAlchemyError as err:
        error = str(err.__dict__["orig"])
        logger.error("Error message: {error}".format(error=error))
