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

nameMap = {
            "parameter_stable_id": "ImpcCode",
            "date_of_birth": "DOB",
            "external_sample_id": "AnimalID",
            "allele_symbol": "Symbol",
            "download_url": "DownLoadFilePath",
            "jpeg_url": "JPEG",
            "experiment_source_id": "ExperimentName",
            "colony_id": "JR",
            "sex": "Sex"
            }

ReturnType = "parameter_stable_id, parameter_stable_id, date_of_birth, external_sample_id, allele_symbol" \
             "download_url, jpeg_url, experiment_source_id, colony_id, sex"


def connect_to_db(username: str,
                  password: str,
                  server: str,
                  database: str):
    """
    Function to build connection to backend database
    :return: mysql.connector.connection
    """
    try:
        conn = mysql.connector.connect(host=server, user=username, password=password, database=database)
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
            if node in nameMap:
                logging.debug(f"Adding {node} now")
                tempDict_[nameMap[node]] = g[node]

        data = pd.Series(tempDict_).to_frame()
        data = data.transpose()
        result.append(data)


def filter_image_by(colonyId: Optional[str] = None,
                    parameter_stable_id: Optional[str] = None,
                    formats: Optional[str] = None,
                    indent: Optional[bool] = None,
                    center: Optional[str] = None,
                    start: Optional[int] = 0,
                    rows: Optional[int] = 0) -> list[pd.DataFrame]:
    """
    Function to build connection to backend database
    return: mysql.connector.connection
    """
    if rows > 2 ** 31 - 1 or rows < 0:
        logger.error("Invalid start or result size")
        return []
    dict_ = {}
    if colonyId:
        dict_["colony_id"] = colonyId

    if parameter_stable_id:
        dict_["parameter_stable_id"] = parameter_stable_id

    if formats:
        dict_["wt"] = indent

    if center:
        dict_["phenotyping_center"] = center

    filters = []
    for key, val in dict_.items():
        filter_ = key + ":" + val
        logger.debug(f"Add condition {filter_}")
        filters.append(filter_)

    selectCondition = " AND ".join(filters)
    logger.debug(f"Select condition is :{selectCondition}")
    params = {"q": selectCondition, "indent": indent, "start": start, "rows": rows}
    query = urllib.parse.urlencode(params, doseq=True, safe=', :').replace("+", " ")
    url = urlunsplit(("https", "www.ebi.ac.uk", "/mi/impc/solr/experiment/select", query, ""))
    print(url)
    logger.debug(f"URL is {url}")

    try:
        payload = {}
        headers = {
            'Accept': 'application/json',
            'Authorization': 'Basic c3ZjLWxpbXNkYkBqYXgub3JnOnZBJmNlMyhST3pBTA=='
        }
        response = requests.request("GET", url, headers=headers, data=payload).json()["response"]
        data_found = 1 if response["numFound"] > 0 else 0
        '''Data found'''
        if data_found == 1:
            json_objects = response["docs"]
            result = []
            BFS(json_objects, result)
            return result

        else:
            logger.info(f"No record found at {url}")

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
                 username: str,
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

    # df.drop("procedureKey", axis=1, inplace=True)
    currTime = [datetime.datetime.now() for i in range(len(df.index))]
    insertData = df.copy()
    insertData["modifiedTime"] = pd.Series(currTime).values

    try:
        engine = create_engine("mysql+mysqlconnector://{0}:{1}@{2}/{3}".
                               format(username, password, server, database),
                               pool_recycle=3600,
                               pool_timeout=57600,
                               future=True)
        # print(df)
        df.to_sql("ebiimages", engine, if_exists='append', index=False, chunksize=1000)
        rows = engine.connect().execute(text("SELECT COUNT(*) FROM komp.ebiimages;")).scalar()
        logger.debug(f"Number of rows in table is {rows}")

    except SQLAlchemyError as err:
        error = str(err.__dict__["orig"])
        logger.error("Error message: {error}".format(error=error))


"""
db_server = "rslims.jax.org"
db_user = "dba"
db_password = "rsdba"
db_name = "komp"
call_back = filter_image_by(colonyId="JR18609",
                            parameter_stable_id="IMPC_XRY_034_001",
                            indent=True,
                            center="JAX",
                            rows=2 ** 31 - 1,
                            )
print(call_back[0].columns)
insert_to_db(dataset=call_back, username=db_user, password=db_password, server=db_server, database=db_name)
"""
