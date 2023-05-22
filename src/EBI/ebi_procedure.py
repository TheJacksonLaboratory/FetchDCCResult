import urllib
from typing import Optional
from urllib.parse import urlencode, urlunsplit
import requests
import logging
import mysql.connector
from DateTime import DateTime
import datetime
from datetime import datetime

logger = logging.getLogger(__name__)

nameMap = {"gene_symbol": "GeneSymbol", "date_of_experiment": "date_of_experiment",
           "experiment_source_id": "experiment_name", "external_sample_id": "organism_name",
           "sex": "sex", "weight_days_old": "age", "zygosity": "zygosity", "colony_id": "JRNumber",
           "life_stage_name": "study", "procedure_stable_id": "IMPC_CODE"}
ReturnType = "date_of_experiment,experiment_source_id,external_sample_id,sex,gene_symbol,weight_days_old," \
             "zygosity,colony_id,life_stage_name,procedure_stable_id"


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


def BFS(graph: list[dict],
        result: list) -> None:
    if not graph:
        logger.error("Empty Json Object!")
        return

    source_id_set = set()
    for json_obj in graph:
        db_obj = {}
        if json_obj["experiment_source_id"] in source_id_set:
            logger.info("Duplicate records encountered")
            continue

        for key, val in json_obj.items():
            if key in nameMap:
                logger.debug(f"Adding value of key {key}")
                db_obj[nameMap[key]] = val

        result.append(db_obj)
        source_id_set.add(json_obj["experiment_source_id"])


def filer_procedure_by(columns: list[str],
                       gene_symbol: Optional[str] = None,
                       gene_accession_id: Optional[str] = None,
                       pipeline_stable_id: Optional[str] = None,
                       procedure_stable_id: Optional[str] = None,
                       colony_id: Optional[str] = None,
                       allele_accession_id: Optional[str] = None,
                       zygosity: Optional[str] = None,
                       sex: Optional[str] = None,
                       weight: Optional[float] = None,
                       parameter_stable_id: Optional[str] = None,
                       data_point: Optional[str] = None,
                       rows: Optional[int] = 0) -> list[dict]:
    if not columns:
        logger.error("No column headers")
        return []

    if rows > 2 ** 31 - 1 or rows < 0:
        logger.error("Invalid start or result size")
        return []

    # base_url = "https://www.ebi.ac.uk/mi/impc/solr/experiment/select?q={}&fl={}"
    dict_ = {}

    if gene_symbol:
        dict_["gene_symbol"] = gene_symbol

    if parameter_stable_id:
        dict_["parameter_stable_id"] = parameter_stable_id

    if zygosity:
        dict_["zygosity"] = zygosity

    if gene_accession_id:
        dict_["gene_accession_id"] = gene_accession_id

    if pipeline_stable_id:
        dict_["pipeline_stable_id"] = pipeline_stable_id

    if procedure_stable_id:
        dict_["procedure_stable_id"] = procedure_stable_id

    if allele_accession_id:
        dict_["allele_accession_id"] = allele_accession_id

    if weight:
        dict_["weight"] = weight

    if sex:
        dict_["sex"] = sex

    if data_point:
        dict_["data_point"] = data_point

    if colony_id:
        dict_["colony_id"] = colony_id

    filters = []
    for key, val in dict_.items():
        filter_ = key + ":" + val
        logger.debug(f"Add condition {filter_}")
        filters.append(filter_)

    selectCondition = " AND ".join(filters)
    logger.debug(f"Select condition is :{selectCondition}")
    params = {"q": selectCondition, "fl": ReturnType, "rows": rows}
    query = urllib.parse.urlencode(params, safe=', :').replace("+", " ")
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


def insert_to_db(dataset: list[dict],
                 username: str,
                 password: str,
                 server: str,
                 database: str) -> None:
    """

        :param dataset:
        :param username:
        :param password:
        :param server:
        :param database:
        :return:
    """

    if not dataset:
        logger.warning("No record retrieved!")
        return

    logger.info("Connecting to db")
    conn = connect_to_db(user=username, password=password, server=server, database=database)
    cursor = conn.cursor()

    for data in dataset:
        def strip_datetime(datetime_str) -> None:
            logger.info("datetime string : {}".format(datetime_str))
            datetime_obj = datetime.strptime(datetime_str,
                                             "%Y-%m-%dT%H:%M:%SZ")

            logger.debug(datetime_obj)
            date = datetime_obj.date()
            print(date)
            logger.debug(f"Stripped date is {date}")
            data["date_of_experiment"] = date

        strip_datetime(data["date_of_experiment"])
        print(data)
        placeholders = ', '.join(['%s'] * len(data))
        cols = ', '.join(data.keys())
        logger.info(f"Adding record for {data['experiment_name']}")
        stmt = "INSERT INTO %s ( %s ) VALUES ( %s );" % ("KOMP.ebi_procedures", cols, placeholders)
        cursor.execute(stmt, list(data.values()))
        conn.commit()

    logger.debug("Done")
    conn.close()


'''
db_server = "rslims.jax.org"
db_user = "dba"
db_password = "rsdba"
db_name = "komp"

conn = mysql.connector.connect(host=db_server, user=db_user, password=db_password, database=db_name)
cursor = conn.cursor()
cursor.execute("""SHOW COLUMNS FROM KOMP.ebi_procedures;""")
queryResult = cursor.fetchall()[1:]
columns = []
for item in queryResult:
    columns.append(item[0])

result = filer_procedure_by(columns=columns, colony_id="JR18553", procedure_stable_id="IMPC_ABR_002", rows = 2000)
print(result)
insert_to_db(dataset=result, username=db_user, password=db_password, server=db_server, database=db_name)
'''
