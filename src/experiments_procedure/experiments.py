import logging
from datetime import datetime
from errno import errorcode
from typing import Optional
from urllib.parse import urlunsplit, urlencode
import mysql
import requests

logger = logging.getLogger(__name__)

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


def insert_to_db(dataset: list[dict],
                 insert_type: str,
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

    if insert_type == "specimen":
        conn = connect_to_db(user=username, password=password, server=server, database=database)
        cursor = conn.cursor()

        for data in dataset:
            placeholders = ', '.join(['%s'] * len(data))
            columns = ', '.join(data.keys())
            stmt = "INSERT INTO %s ( %s ) VALUES ( %s );" % ("komp.experimentsonspecimens", columns, placeholders)
            cursor.execute(stmt, list(data.values()))
            conn.commit()

    else:
        logger.warning("Invalid insert type")
        return