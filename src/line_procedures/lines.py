import logging
from datetime import datetime
from typing import Optional
from urllib.parse import urlencode, urlunsplit
import mysql.connector
import requests

logger = logging.getLogger(__name__)


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


def insert_to_db(dataset: list[dict],
                 insert_type: str,
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

    if insert_type == "line":
        logger.info("Connecting to db")
        conn = connect_to_db(user=username, password=password, server=server, database=database)
        cursor = conn.cursor()

        for data in dataset:
            placeholders = ', '.join(['%s'] * len(data))
            columns = ', '.join(data.keys())
            logger.debug(columns)
            stmt = "INSERT INTO %s ( %s ) VALUES ( %s );" % ("komp.experimentsonlines", columns, placeholders)
            cursor.execute(stmt, list(data.values()))
            conn.commit()

    else:
        logger.warning("Invalid insert type")
        return