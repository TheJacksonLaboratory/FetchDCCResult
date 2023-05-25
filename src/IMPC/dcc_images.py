import logging
from datetime import datetime
from typing import Optional
from urllib.parse import urlencode, urlunsplit

import mysql
import pandas as pd
import requests
import sqlalchemy as db
from mysql.connector import errorcode
from sqlalchemy import Column, String, DateTime
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import declarative_base

logger = logging.getLogger("__main__")


def connect_to_db(user: str,
                  password: str,
                  server: str,
                  database: str) -> mysql.connector.connection:
    """
    Function to build connection to backend database
    :return: mysql.connector.connection
    """
    try:
        conn = mysql.connector.connect(host=server, user=user, password=password, database=database)
        logger.info(f"Successfully connected to {database}")
        return conn

    except mysql.connector.Error as err1:

        if err1.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            logger.error("Wrong user name or password passed")

        elif err1.errno == errorcode.ER_BAD_DB_ERROR:
            logger.error("No such schema")

        else:
            error = str(err1.__dict__["orig"])
            logger.error(error)

    except ConnectionError as err2:
        logger.error(err2)

    return None


def DFS(json_object: dict, columns: list[str], db_obj: dict) -> dict:
    """

    :param json_object:
    :param columns:
    :param db_obj:
    :return:
    """
    if not json_object:
        return db_obj

    for key, val in json_object.items():
        if key in columns and key not in db_obj:
            db_obj[key] = val

        if isinstance(val, dict):
            DFS(val, columns, db_obj)

        if isinstance(val, list):
            for item in val:
                if isinstance(item, dict):
                    DFS(item, columns, db_obj)

    return db_obj


Base = declarative_base()


# {'id': 22118286, 'xmlId': 1122765, 'experimentName': 'EKG v3 - D87884 - 3563120', 'pipeline': 'JAX_001',
# 'procedure': 'IMPC_ECG_003', 'specimen': 'D87884', 'phenotypingCentre': 'J', 'sequenceId': None, 'statusId': 2,
# 'logs': [], 'status': 'active', 'created': 1677351816, 'age': '11.43 weeks', 'dateOfExperiment':
# '2023-01-04T00:00:00', 'createdDate': '2023-02-25T19:03:36'}

class dccImage(Base):
    __tablename__ = "dccImages"

    url = Column(String, primary_key=True)
    animalName = Column(String)
    genotype = Column(String)
    strain = Column(String)
    status = Column(String)
    parameterKey = Column(String)
    phase = Column(String)
    xmlFileName = Column(String)
    dateModified = Column(DateTime)


# -------------------------------------------- Images --------------------------------------------#

def filter_image_by(parameterKey: Optional[str] = None,
                    colonyId: Optional[str] = None,
                    animalId: Optional[str] = None,
                    strain: Optional[str] = None,
                    procedureKey: Optional[str] = None,
                    status: Optional[str] = None,
                    phase: Optional[str] = None,
                    pipelineKey: Optional[str] = None,
                    xmlFileName: Optional[str] = None,
                    start: Optional[int] = None,
                    resultsize: Optional[int] = None) -> list[dict]:
    """
    Function to get all results related to one specific parameter key
    @:param
        parameterKey:IMPC code, e.g. IMPC_EYE_050_001
        colonyId: JR number of an animal. usually starts with "JR"
        animalId: Name of animal
        strain: ID of a given strain
        procedureKey:
        status:
        phase:
        pipelineKey:
        xmlFileName:
        start: start index of the querying
        resultsize: number of json object displayed on one page
    """

    result = []
    filters = {}

    if start < 0 or start > 2 ** 31 - 1 \
            or resultsize > 2 ** 31 - 1 or resultsize <= 0:
        raise ValueError("Invalid start or result size")

    else:
        """Generate the url"""
        if parameterKey is not None:
            filters["parameterKey"] = parameterKey

        if colonyId is not None:
            filters["colonyId"] = colonyId

        if animalId is not None:
            filters["animalId"] = animalId

        if start:
            filters["start"] = str(start)

        if resultsize:
            filters["resultsize"] = str(resultsize)

        if strain:
            filters["strain"] = strain

        if procedureKey:
            filters["procedureKey"] = procedureKey

        if pipelineKey:
            filters["pipelineKey"] = pipelineKey

        if phase:
            filters["phase"] = phase

        if status:
            filters["status"] = status

        if xmlFileName:
            filters["xmlFileName"] = xmlFileName

        logger.debug(f"Filtering conditions are {filters}")
        query = urlencode(query=filters, doseq=True)
        url = urlunsplit(("https", "api.mousephenotype.org", "/media/J", query, ""))
        logger.debug(url)
        print(url)

        """Get data back from impc"""
        try:
            response = requests.get(url).json()
            status = 1 if response["total"] > 0 else 0
            '''Data found'''
            if status == 1:
                for dict_ in response["mediaFiles"]:
                    result.append(dict_)
                    # print(result)
            else:
                logger.info(f"No record found at {url}")
                return []

        except requests.exceptions.HTTPError as err1:
            error = str(err1.__dict__)
            logger.error(error)

        except requests.exceptions.ConnectionError as err2:
            error = str(err2.__dict__)
            logger.error(error)

        except requests.exceptions.Timeout as err3:
            error = str(err3.__dict__)
            logger.error(error)

        except requests.exceptions.RequestException as err4:
            error = str(err4.__dict__)
            logger.error(error)

    return result


def insert_to_db(dataset: list[dict],
                 username: str,
                 password: str,
                 server: str,
                 database: str):
    """

    :param dataset:
    :param insert_type:
    :param username:
    :param password:
    :param server:
    :param database:
    :return:
    """

    """
    insertStmt = "INSERT INTO komp.dccQualityIssues (AnimalName, Taskname, TaskInstanceKey, ImpcCode, StockNumber,
    DateDue, Issue) VALUES ( '{0}','{1}',{2},'{3}','{4}','{5}','{6}')". \ format(msg['AnimalName'], msg['TaskName'],
    int(msg['TaskInstanceKey']), msg['ImpcCode'], msg['StockNumber'], msg['DateDue'], msg['Issue'].replace("'", "\""))
    """

    if not dataset:
        logger.warning("No record retrieved!")
        return

        # Concat datatset into a pandas dataframe
    temp = []
    for data in dataset:
        data = pd.Series(data).to_frame()
        data = data.transpose()
        temp.append(data)

    df = pd.concat(temp)

    df.drop("procedureKey", axis=1, inplace=True)
    currTime = [datetime.now() for i in range(len(df.index))]
    insertData = df.copy()
    insertData["modifiedTime"] = pd.Series(currTime).values

    try:
        engine = create_engine("mysql+mysqlconnector://{0}:{1}@{2}/{3}".
                               format(username, password, server, database),
                               pool_recycle=3600,
                               pool_timeout=57600,
                               future=True)

        with engine.connect() as conn:
            logger.debug("Getting the column names")
            keys = conn.execute(db.text("SELECT * FROM komp.dccimages;")).keys()

        insertData.columns = keys
        print(insertData)
        insertData.to_sql("dccimages", engine, schema="komp", if_exists='append', index=False, chunksize=1000)
        rows = engine.connect().execute(db.text("SELECT COUNT(*) FROM komp.dccImages;")).scalar()
        logger.debug(f"Number of rows in table is {rows}")

    except SQLAlchemyError as err:
        error = str(err.__dict__["orig"])
        logger.error("Error message: {error}".format(error=error))


'''
db_server = "rslims.jax.org"
db_user = "dba"
db_password = "rsdba"
db_name = "komp"
parameterCode = "IMPC_EYE_050_001"
call_back = filter_image_by(parameterKey=parameterCode, start=0, resultsize=2 ** 31 - 1)
insert_to_db(dataset=call_back, username=db_user, password=db_password, server=db_server, database=db_name)
'''
