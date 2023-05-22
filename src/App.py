import sys
import utils
import os
import logging
from logging.handlers import RotatingFileHandler
import datetime
import mysql.connector

import IMPC.dcc_procedures as procedures
import IMPC.dcc_files as dcc_files
import IMPC.dcc_images as media
import src.EBI.ebi_images as ebi_media
import src.EBI.ebi_procedure as ebi_procedure

"""Setup logger"""

logging_dest = os.path.join(os.getcwd(), "logs")

logger = logging.getLogger(__name__)
FORMAT = "[%(asctime)s->%(filename)s->%(funcName)s():%(lineno)s]%(levelname)s: %(message)s"
logging.basicConfig(format=FORMAT, filemode="w", level=logging.DEBUG, force=True)
date = datetime.date.today().strftime("%B-%d-%Y")
logging_filename = logging_dest + "/" + f'{date}.log'
handler = RotatingFileHandler(logging_filename, maxBytes=10000000000, backupCount=10)
handler.setFormatter(logging.Formatter(FORMAT))
logger.addHandler(handler)


def main():
    parameterKeys = utils.parameterKeys
    db_server = utils.db_server
    db_user = utils.db_user
    db_password = utils.db_password
    db_name = utils.db_name

    if sys.argv[1] == "IMPC":

        print(f"\n------------ Fetching data for {sys.argv[1]} ----------")

        if sys.argv[2] == "-i":
            conn = media.connect_to_db(user=db_user, password=db_password, server=db_server, database="rslims")
            cursor = conn.cursor()
            cursor.execute("TRUNCATE TABLE komp.dccImages;")
            conn.commit()
            conn.close()

            for impcCode in parameterKeys:
                logger.info("Getting result from IMPC")
                result = media.filter_image_by(parameterKey=impcCode, start=0, resultsize=2 ** 31 - 1)
                logger.info("Inserting . . . ")
                media.insert_to_db(dataset=result,
                                   username=db_user,
                                   password=db_password,
                                   server=db_server,
                                   database=db_name)
                logger.info("Process finished")

            logger.info("Process finished")
            print("\n-----------------------------------------------------")
            print("Process finished")

        if sys.argv[2] == "-e":

            conn = media.connect_to_db(user=db_user, password=db_password, server=db_server, database="rslims")
            cursor = conn.cursor()
            cursor.execute("TRUNCATE TABLE komp.experimentsonspecimens;")
            conn.commit()
            conn.close()

            conn = procedures.connect_to_db(user=db_user, password=db_password, server=db_server, database="rslims")
            cursor = conn.cursor()
            sql = "SELECT OrganismID FROM Organism INNER JOIN OrganismStudy USING (_Organism_key) WHERE _Study_key IN (" \
                  "27, 28, 57);"
            cursor.execute(sql)
            animalIds = cursor.fetchall()
            conn.close()

            for item in animalIds:
                animalId = item[0]
                call_back = procedures.filter_experiment_procedure_by(animalId=animalId, showDetails=True,
                                                                      status="active")
                procedures.insert_to_db(dataset=call_back,
                                        insert_type="specimen",
                                        username=db_user,
                                        password=db_password,
                                        server=db_server,
                                        database=db_name)

            logger.info("Process finished")
            print("\n-----------------------------------------------------")
            print("Process finished")

        if sys.argv[2] == "-l":

            conn_to_prod = procedures.connect_to_db(user=db_user, password=db_password, server=db_server,
                                                    database="komp")
            cursor = conn_to_prod.cursor()
            cursor.execute("SHOW COLUMNS FROM experimentsonlines;")
            queryResult = cursor.fetchall()
            logger.info(queryResult)

            logger.info("Remove records from table komp.experimentsonlines")
            cursor.execute("TRUNCATE TABLE komp.experimentsonlines;")
            conn_to_prod.commit()

            conn_to_prod.close()

            cols = []
            for item in queryResult[1:]:
                logger.debug(f"Adding {item[0]}")
                cols.append(item[0])
            conn_to_prod.close()

            # Statement to select
            stmt = utils.stmt_for_colonyId

            db_server_dev = "rslims-dev.jax.org"
            conn_to_dev = mysql.connector.connect(host=db_server_dev, user=db_user, password=db_password,
                                                  database="rslims")
            cursor = conn_to_dev.cursor()
            cursor.execute(stmt)
            colonyIds = cursor.fetchall()
            print(colonyIds)
            conn_to_dev.close()

            for pair in colonyIds:
                colonyId = pair[0]
                logger.debug(f"Fetching result of experiments on line: {colonyId}")
                result = procedures.filter_line_procedure_by(columns=cols,
                                                             centre="J",
                                                             colonyId=colonyId,
                                                             showdetails=True,
                                                             start=0,
                                                             resultsize=2 ** 31 - 1)
                procedures.insert_to_db(dataset=result,
                                        insert_type="line",
                                        username=db_user,
                                        password=db_password,
                                        server=db_server,
                                        database=db_name)

            logger.info("Process finished")
            print("\n-----------------------------------------------------")
            print("Process finished")

        if sys.argv[2] == "-xml":

            conn = dcc_files.connect_to_db(user=db_user, password=db_password, server=db_server, database=db_name)
            cursor = conn.cursor(buffered=True, dictionary=True)
            cursor.execute("SELECT DISTINCT * FROM KOMP.submittedprocedures;")
            rows = cursor.fetchall()

            '''GET COLUMN NAMES'''

            cursor.execute("SELECT * FROM dccXmlFileStatus;")
            fileStatusColNames = list(cursor.fetchall()[0].keys())[1:]

            cursor.execute("TRUNCATE TABLE dccXmlFileStatus;")

            # db_obj = xml.filter_xml_by(fileName="J.2023-03-02.50.experiment.impc.xml", columns=fileStatusColNames)
            # print(db_obj)

            for row in rows:
                xmlFileName = row["XmlFilename"]
                db_obj = dcc_files.filter_xml_by(fileName=xmlFileName, columns=fileStatusColNames)
                print(db_obj)
                dcc_files.insert_to_db(db_object=db_obj, username=db_user, password=db_password, server=db_server,
                                       database=db_name)

            logger.info("Process finished")
            print("\n-----------------------------------------------------")
            print("Process finished")
            conn.close()

    if sys.argv[1] == "ebi":

        print(f"\n------------ Fetching data for {sys.argv[1]} ----------")

        if sys.argv[2] == "-i":
            pass

        if sys.argv[2] == "-p":
            conn = ebi_procedure.connect_to_db(user=db_user, password=db_password, server=db_server, database=db_name)
            cursor = conn.cursor()
            cursor.execute("""SHOW COLUMNS FROM KOMP.ebi_procedures;""")
            queryResult = cursor.fetchall()[1:]
            columns = []
            for item in queryResult:
                columns.append(item[0])
            conn.close()

            # Statement to select
            stmt = utils.stmt_for_colonyId

            logger.info("Connecting to development database")
            db_server_dev = "rslims-dev.jax.org"
            conn_to_dev = mysql.connector.connect(host=db_server_dev, user=db_user, password=db_password,
                                                  database="rslims")
            cursor = conn_to_dev.cursor()
            cursor.execute(stmt)
            colonyIds = cursor.fetchall()
            conn_to_dev.close()
            logger.info("Connection to development database closed")

            for parameterKey in parameterKeys:
                for pair in colonyIds:
                    colonyId = pair[0]
                    logger.debug(f"Fetching result for JR number: {colonyId}, IMPC CODE: {parameterKey} ")
                    result = ebi_procedure.filer_procedure_by(columns=columns,
                                                              colony_id=colonyId,
                                                              procedure_stable_id=parameterKey,
                                                              rows=2 ** 31 - 1)
                    logger.info(f"Getting number of {len(result)} result back")
                    ebi_procedure.insert_to_db(dataset=result,
                                               username=db_user,
                                               password=db_password,
                                               server=db_server,
                                               database=db_name)

            logger.info("Process finished")
            print("\n-----------------------------------------------------")
            print("Process finished")


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    main()
