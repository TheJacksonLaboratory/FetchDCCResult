import logging
from logging.handlers import RotatingFileHandler
import os
from datetime import datetime
from urllib.parse import urlencode, urlunsplit
import mysql
import pandas as pd
import requests
from typing import Optional
import sqlalchemy as db
from collections import ChainMap
from mysql.connector import errorcode
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy import Column, Integer, String, DateTime
from sqlalchemy import create_engine

outputDir = os.path.join(os.getcwd(), "Logs")

try:
    os.mkdir(outputDir)

except FileExistsError as e:
    print("File exists")

"""Setup logger"""

logger = logging.getLogger("Core")
FORMAT = "[%(asctime)s->%(filename)s->%(funcName)s():%(lineno)s]%(levelname)s: %(message)s"
logging.basicConfig(format=FORMAT, filemode="w", level=logging.DEBUG, force=True)
logging_filename = outputDir + "/" + 'App.log'
handler = RotatingFileHandler(logging_filename, maxBytes=10000000000, backupCount=10)
handler.setFormatter(logging.Formatter(FORMAT))
logger.addHandler(handler)


# -------------------------------------------- General Functions --------------------------------------------#

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


class specimens(Base):
    __tablename__ = "experimentsonspecimens"

    _ExperimentsOnspecimens_key = Column(Integer, primary_key=True)
    id = db.Column(String)
    xmlId = db.Column(String)
    experimentName = db.Column(String)
    pipeline = db.Column(String)
    _procedure = db.Column(String)
    specimen = db.Column(String)
    phenotypingCentre = db.Column(String)
    sequenceId = db.Column(String)
    statusId = db.Column(Integer)
    logs = db.Column(String)
    status = db.Column(String)
    created = db.Column(db.BigInteger)
    ageAtExperiment = db.Column(String)
    dateOfExperiment = db.Column(DateTime)
    createdDate = db.Column(DateTime)




# -------------------------------------------- Procedure --------------------------------------------#

def filter_experiment_procedure_by(animalId: Optional[str] = None,
                                   showDetails: Optional[bool] = True,
                                   status: Optional[str] = None,
                                   createdDateFrom: Optional[datetime] = None,
                                   createdDateTo: Optional[datetime] = None,
                                   centre="J",
                                   ) -> list[dict]:
    """

    :param centre:
    :param animalId: ID of the specimen/animal
    :param showDetails: Options to display details of the experiments_procedure
    :param status: Status of given
    :param createdDateFrom:
    :param createdDateTo:
    :return: Collections of data retrieved by API calls

    """

    result = []
    filters = {"centre": centre}

    if animalId is not None:
        filters["specimenId"] = animalId

    if status:
        filters["status"] = status

    if showDetails:
        filters["showdetails"] = str(showDetails).lower()

    if createdDateFrom:
        filters["createdDateFrom"] = createdDateFrom

    if createdDateFrom:
        filters["createdDateTo"] = createdDateTo

    query = urlencode(query=filters, doseq=True)
    url = urlunsplit(("https", "api.mousephenotype.org", "/tracker/search/experimentprocedures", query, ""))
    print(url)

    try:
        response = requests.get(url)
        json_objects = response.json()
        for json_obj in json_objects:
            # result.append(json_obj)
            log = json_obj["logs"]
            json_obj.update(logs=str(log))
            json_obj["_procedure"] = json_obj["procedure"]
            del json_obj["procedure"]
            json_obj["ageAtExperiment"] = json_obj["age"]
            del json_obj["age"]
            result.append(json_obj)

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

    return result


def filer_specimen_by(animalId: Optional[str],
                      status: Optional[str],
                      pipeline: Optional[str],
                      createdDateFrom: Optional[datetime],
                      createdDateTo: Optional[datetime],
                      centre="J"
                      ) -> list[dict]:
    """

    :param animalId:
    :param status:
    :param pipeline:
    :param createdDateFrom:
    :param createdDateTo:
    :param centre:
    :return:
    """

    result = []
    filters = {"centre": centre}

    if animalId is not None:
        filters["specimenId"] = animalId

    if status:
        filters["status"] = status

    if pipeline:
        filters["pipeline"] = pipeline

    if createdDateFrom:
        filters["createdDateFrom"] = createdDateFrom

    if createdDateFrom:
        filters["createdDateTo"] = createdDateTo

    query = urlencode(query=filters, doseq=True)
    url = urlunsplit(("https", "api.mousephenotype.org", "/tracker/search/specimens", query, ""))
    logger.info(url)

    try:
        response = requests.get(url)
        json_objects = response.json()
        for json_obj in json_objects:
            result.append(json_obj)

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

    return result


def filter_line_procedure_by(columns: list[str],
                             centre: str,
                             colonyId: Optional[str] = None,
                             showdetails: Optional[bool] = True,
                             status: Optional[str] = None,
                             createdDateFrom: Optional[datetime] = None,
                             createdDateTo: Optional[datetime] = None,
                             start: Optional[int] = None,
                             resultsize: Optional[int] = None,
                             ) -> list[dict]:
    """

    :param centre:
    :param columns:
    :param colonyId:
    :param showdetails:
    :param status:
    :param createdDateFrom:
    :param createdDateTo:
    :param start:
    :param resultsize:
    :return:
    """
    if start < 0 or start > 2 ** 31 - 1 \
            or resultsize > 2 ** 31 - 1 or resultsize <= 0:
        raise ValueError("Invalid start or result size")

    if not columns:
        raise ValueError("No column headers")

    filters = {"centre": centre, "showdetails": str(showdetails), "start": start, "resultsize": resultsize}

    if colonyId is not None:
        filters["colonyId"] = colonyId

    if status:
        filters["status"] = status

    if createdDateFrom:
        filters["createdDateFrom"] = createdDateFrom

    if createdDateFrom:
        filters["createdDateTo"] = createdDateTo

    query = urlencode(query=filters, doseq=True)
    url = urlunsplit(("https", "api.mousephenotype.org", "/tracker/search/lineprocedures", query, ""))
    print(url)
    logger.info(url)

    try:
        logger.info(f"Found data at {url}")
        response = requests.get(url)
        json_objects = response.json()

        result = []
        for json_obj in json_objects:
            db_obj = {"colonyId": "JR36697"}
            db_obj = DFS(json_object=json_obj, columns=columns, db_obj=db_obj)
            logger.info(f"Result dict is {db_obj}")
            result.append(db_obj)

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


# -------------------------------------------- File --------------------------------------------#

def track_zipFile_by(centre="J",
                     status: Optional[str] = None,
                     phase: Optional[str] = None,
                     start: Optional[int] = None,
                     resultsize: Optional[int] = None) -> list:
    """
    Function to track info of submitted zipped files
    :param centre: Name of data center, e.g. JAX is "J"
    :param status: Status of zipfile you want to filter, e.g. done
    :param phase: Phase of file, e.g. downloaded
    :param start: Start page of your query
    :param resultsize: Number of JSON objects you want in the webpage
    :return: list of json objects
    For more info, please visit https://www.mousephenotype.org/phenodcc/tracker-documentation/
    """

    result = []
    filters = {}

    if (start and start < 0) or (start and start > 2 ** 31 - 1) \
            or (resultsize and resultsize > 2 ** 31 - 1) or (resultsize and resultsize <= 0):
        raise ValueError("Invalid start or result size")

    else:

        """Generate the url"""
        if centre is not None:
            filters["centre"] = centre

        if start:
            filters["start"] = str(start)

        if status:
            filters["status"] = status

        if phase:
            filters["phase"] = phase

        if resultsize:
            filters["resultsize"] = str(resultsize)

        query = urlencode(query=filters, doseq=True)
        url = urlunsplit(("https", "api.mousephenotype.org", "/tracker/centre/zip", query, ""))
        print(url)
        wantedKey = ["id", "filename", "phase", "phaseId", "status", "statusId"]

        """Get data back from impc"""
        try:
            response = requests.get(url)
            payload = response.json()
            print(len(payload))
            # Getting data pending implementation

            for dict_ in payload:
                record = {i: dict_[i] for i in wantedKey}
                # errorMessage = dict_["issues"][0]
                # result.append(record.update(errorMessage))
                result.append(record)

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

    print(len(result))
    return result


def track_xml_by(centre: Optional[str] = None,
                 status: Optional[str] = None,
                 phase: Optional[str] = None,
                 start: Optional[int] = None,
                 resultsize: Optional[int] = None,
                 columns: Optional[dict] = None
                 ) -> list:
    """

    Function to track submitted DCC_Files files
    :param centre: Name of data center, e.g. JAX is "J"
    :param status: Status of zipfile you want to filter, e.g. done
    :param phase: Phase of file, e.g. downloaded
    :param start: Start page of your query
    :param resultsize: Number of JSON objects you want in the webpage
    :param columns:
    :return: list of json objects with info of DCC_Files files
    For more info, please visit https://www.mousephenotype.org/phenodcc/tracker-documentation/
    """
    result = []
    filters = {}

    if (start and start < 0) or (start and start > 2 ** 31 - 1) \
            or (resultsize and resultsize > 2 ** 31 - 1) or (resultsize and resultsize <= 0):
        raise ValueError("Invalid start or result size")

    else:
        """Generate the url"""

        if centre is not None:
            filters["centre"] = centre

        if start:
            filters["start"] = str(start)

        if status:
            filters["status"] = status

        if phase:
            filters["phase"] = phase

        if resultsize:
            filters["resultsize"] = str(resultsize)

        query = urlencode(query=filters, doseq=True)
        url = urlunsplit(("https", "api.mousephenotype.org", "/tracker/centre/zip", query, ""))
        print(url)
        wantedKey = ["id", "filename", "phase", "phaseId", "status", "statusId", "message"]

        """Get data back from impc"""
        try:
            response = requests.get(url)
            payload = response.json()
            print(len(payload))
            # Getting data pending implementation

            for dict_ in payload:
                record = {i: dict_[i] for i in wantedKey}
                result.append(record)

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

    print(result)
    return result


def track_zipFile_issues(centre: Optional[str] = None,
                         updatedSinceDate: Optional[str] = None,
                         validationIssues: Optional[bool] = True,
                         ignoreWarnings: Optional[bool] = True,
                         xmlErrors: Optional[bool] = True,
                         zipErrors: Optional[bool] = True,
                         start: Optional[int] = None,
                         resultsize: Optional[int] = None) -> list:
    result = []
    filters = {}

    if (start and start < 0) or (start and start > 2 ** 31 - 1) \
            or (resultsize and resultsize > 2 ** 31 - 1) or (resultsize and resultsize <= 0):
        raise ValueError("Invalid start or result size")

    else:
        if centre:
            filters["centre"] = centre

        if validationIssues:
            filters["validationIssues"] = validationIssues

        if updatedSinceDate:
            filters["updatedSinceDate"] = updatedSinceDate

        filters["validationIssues"] = False if not validationIssues else True
        filters["ignoreWarnings"] = False if not ignoreWarnings else True
        filters["xmlErrors"] = False if not xmlErrors else True
        filters["zipErrors"] = False if not zipErrors else True

        query = urlencode(query=filters, doseq=True)
        url = urlunsplit(("https", "api.mousephenotype.org", "/tracker/centre/zip/issues", query, ""))
        print(url)
        wantedKey = ["id", "filename", "phase", "phaseId", "status", "statusId"]

        try:
            response = requests.get(url)
            payload = response.json()
            # print(payload)

            '''Get data back'''
            for dict_ in payload:
                info = {i: dict_[i] for i in wantedKey}
                errorMessage = dict_["issues"][0]
                # print(record.update(errorMessage))
                record = dict(ChainMap({}, errorMessage, info))
                result.append(record)

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

    print(result)
    return result


def filter_line_by(colonyId: Optional[str] = True,
                   onlyActive: Optional[bool] = True,
                   onlyInvalid: Optional[bool] = True,
                   start: Optional[int] = None,
                   resultsize: Optional[int] = None,
                   headers: Optional[str] = None) -> list:
    """
    Function to get data of a line associated with one genotype/colonyId from IMPC
    :param colonyId:
    :param onlyActive:
    :param onlyInvalid:
    :param start:
    :param resultsize:
    :param headers:column names that you want to use when to store to DB
    :return: list of pandas dataframe
    """

    if onlyActive and onlyInvalid:
        raise ValueError("No filtering will be performed if both onlyActive and onlyInvalid are both set "
                         "to true.")

    if (start and start < 0) or (start and start > 2 ** 31 - 1) \
            or (resultsize and resultsize > 2 ** 31 - 1) or (resultsize and resultsize <= 0):
        raise ValueError("Invalid start or result size")

    """Generate URL for querying data"""
    result = []
    filters = {"onlyActive": False if not onlyActive else True, "onlyInvalid": False if not onlyInvalid else True}
    query = urlencode(query=filters, doseq=True)
    url = urlunsplit(("https", "api.mousephenotype.org", f"/tracker/linesummary/J/{colonyId}", query, ""))
    print(url)
    wantedKey = {"colonyId", "centre", "pipeline", "procedure", "sex", "specimen"}

    try:
        response = requests.get(url)
        payload = response.json()
        # print(len(payload))

        for dict_ in payload:
            record = {i: dict_[i] for i in wantedKey}
            result.append(record)

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

    print(result)
    return result


def insert_to_db(dataset: list[dict],
                 insert_type: str,
                 tableName: str,
                 username: str,
                 password: str,
                 server: str,
                 database: str):
    """

    :param dataset:
    :param insert_type:
    :param tableName:
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

    if insert_type == "images":

        # Concat datatset into a pandas dataframe
        temp = []
        for data in dataset:
            data = pd.Series(data).to_frame()
            data = data.transpose()
            temp.append(data)

        df = pd.concat(temp)

        if tableName == "dccimages":

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
                    keys = conn.execute(db.text("SELECT * FROM komp.dccImages;")).keys()
                insertData.columns = keys
                print(insertData)

                insertData.to_sql(tableName, engine, if_exists='append', index=False, chunksize=1000)
                insertionResult = engine.connect().execute(db.text(f"SELECT * FROM {tableName};"))
                logger.debug(f"Insertion result is:{insertionResult}")
                result = engine.connect().execute(db.text(f"SELECT COUNT(*) FROM {tableName};"))
                print(result.first()[0])

            except SQLAlchemyError as err:
                error = str(err.__dict__["orig"])
                logger.error("Error message: {error}".format(error=error))

        elif tableName == "ebiimages":

            try:
                engine = create_engine("mysql+mysqlconnector://{0}:{1}@{2}/{3}".
                                       format(username, password, server, database),
                                       pool_recycle=3600,
                                       pool_timeout=57600,
                                       future=True)
                # print(df)
                df.to_sql(tableName, engine, if_exists='append', index=False, chunksize=1000)
                result = engine.connect().execute(db.text("SELECT COUNT(*) FROM komp.ebiimages;"))
                logger.debug(f"Number of rows in table is {result.first()[0]}")
            except SQLAlchemyError as err:
                error = str(err.__dict__["orig"])
                logger.error("Error message: {error}".format(error=error))

    if insert_type == "specimen":
        conn = connect_to_db(user=username, password=password, server=server, database=database)
        cursor = conn.cursor()

        for data in dataset:
            placeholders = ', '.join(['%s'] * len(data))
            columns = ', '.join(data.keys())
            stmt = "INSERT INTO %s ( %s ) VALUES ( %s );" % ("komp.experimentsonspecimens", columns, placeholders)
            cursor.execute(stmt, list(data.values()))
            conn.commit()

    if insert_type == "line":
        conn = connect_to_db(user=username, password=password, server=server, database=database)
        cursor = conn.cursor()

        for data in dataset:
            placeholders = ', '.join(['%s'] * len(data))
            columns = ', '.join(data.keys())
            stmt = "INSERT INTO %s ( %s ) VALUES ( %s );" % ("komp.experimentsonlines", columns, placeholders)
            cursor.execute(stmt, list(data.values()))
            conn.commit()

    if insert_type == "embryo":
        pass

    if insert_type == "zipFiles":
        pass

    if insert_type == "DCC_Files":
        pass

'''
db_server = "rslims.jax.org"
db_user = "dba"
db_password = "rsdba"
db_name = "komp"

#call_back = filter_experiment_procedure_by(animalId="D87884", showDetails=True, status="active")
conn = connect_to_db(user=db_user, password=db_password, server=db_server, database="rslims")
cursor = conn.cursor()
sql = "SELECT OrganismID FROM Organism INNER JOIN OrganismStudy USING (_Organism_key) WHERE _Study_key IN (27, 28, 57);"
cursor.execute(sql)
animalIds = cursor.fetchall()
for item in animalIds:
    animalId = item[0]
    call_back = filter_experiment_procedure_by(animalId=animalId, showDetails=True, status="active")
    insert_to_db(dataset=call_back, insert_type="specimen", tableName="komp.experimentsonspecimens",
                 username=db_user,
                 password=db_password,
                 server=db_server,
                 database=db_name)

'''

db_server_prod = "rslims.jax.org"
db_username = "dba"
db_password = "rsdba"
db_name = "komp"
conn_to_prod = mysql.connector.connect(host=db_server_prod, user=db_username, password=db_password, database=db_name)

cursor = conn_to_prod.cursor()
cursor.execute("SHOW COLUMNS FROM experimentsonlines;")
queryResult = cursor.fetchall()
print(type(queryResult[0]))

cols = []
for item in queryResult[1:]:
    cols.append(item[0])
conn_to_prod.close()

#Statement to select 
stmt = """SELECT DISTINCT
                CONCAT('JR', RIGHT(StockNumber, 5)) AS ColonyID
            FROM
                Line
            INNER JOIN
                Project USING (_Line_key)
            INNER JOIN
                ProcedureInstance USING (_Project_key)
            WHERE
                 _ProcedureStatus_key = 5
            AND 
                _ProcedureDefinitionVersion_key IN (239 , 276, 87, 243, 245, 244, 246);"""

db_server_dev = "rslims-dev.jax.org"
db_username = "dba"
db_password = "rsdba"
db_name = "rslims"
conn_to_dev = mysql.connector.connect(host=db_server_dev, user=db_username, password=db_password, database=db_name)
cursor = conn_to_dev.cursor()
cursor.execute(stmt)
colonyIds = cursor.fetchall()
print(colonyIds)

for pair in colonyIds:
    colonyId = pair[0]
    result = filter_line_procedure_by(columns=cols, centre="J", colonyId=colonyId, showdetails=True, start=0, resultsize=2**31-1)
    insert_to_db(dataset=result,
                insert_type="line",
                username=db_username,
                password=db_password,
                server=db_server_prod,
                database=db_name,
                tableName="komp.experimentsonlines")

