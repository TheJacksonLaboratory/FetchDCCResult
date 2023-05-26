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
import EBI.ebi_images as ebi_media
import EBI.ebi_procedure as ebi_procedure

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


# Fetch and write to stdout, some mouse and phenotype info for a given CBA request eg. CBA36
#
# Usage:     python App.py IMPC  [-i] [-e] [-l]
#               -i = Fetch result of all submitted images
#               -e = Fetch result of all submitted experiments on specimens
#               -l = Fetch result of all submitted line procedures
#
#            python App.py ebi [-i][-p]
#               -i = Fetch result of all submitted images
#               -p = Fetch result of all submitted procedures
#
# Requires a file called 'utils.py' that looks like this:
#   parameterKeys = ["IMPC_XRY_034_001",
#                  "IMPC_EYE_050_001",
#                  "IMPC_XRY_048_001",
#                  "IMPC_XRY_049_001",
#                  "IMPC_XRY_050_001",
#                  "IMPC_XRY_051_001",
#                  "IMPC_CSD_085_001",
#                  "IMPC_ABR_014_001",
#                  "JAX_ERG_047_001",
#                  "JAX_SLW_016_001",
#                  "IMPC_ALZ_075_001",
#                  "IMPC_ALZ_076_001",
#                  "IMPC_ELZ_064_001",
#                  "IMPC_HIS_177_001",
#                  "IMPC_EOL_001_001",
#                  "IMPC_EOL_012_001",
#                  "IMPC_EMO_001_001",
#                  "IMPC_EMO_017_001",
#                  "IMPC_GEM_049_001",
#                  "IMPC_GEO_050_001",
#                  "IMPC_GEP_064_001",
#                  "IMPC_GEL_044_001",
#                  "IMPC_EMA_001_001",
#                  "IMPC_EMA_017_001",
#                  "IMPC_GPL_007_001",
#                  "IMPC_GPM_007_001",
#                  "IMPC_GPO_007_001",
#                  "IMPC_GPP_007_001",
#                  "IMPC_ECG_025_001",
#                  "JAX_ERG_028_001",
#                  "IMPC_WEL_003_001",
#                  "IMPC_PAT_057_002"]

#   db_server = ""
#   db_user = ""
#   db_password = ""
#   db_name = ""
#

#   stmt_for_colonyId = ""
#   stmt_for_animalId


def main():
    if len(sys.argv) < 2:
        print("Invalid command line arguments detected, please check the user manual.")
        sys.exit("usage eg:  python App.py IMPC -e")

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
                logger.info(f"Fetching result for {impcCode}")
                result = media.filter_image_by(parameterKey=impcCode, start=0, resultsize=2 ** 31 - 1)
                logger.debug(f"Getting number of {len(result)} records back")

                logger.info(f"Start to insert records for JR number: {impcCode}")
                media.insert_to_db(dataset=result,
                                   username=db_user,
                                   password=db_password,
                                   server=db_server,
                                   database=db_name)

            logger.info("Done")
            print()
            print("\n-----------------------------------------------------")
            print("Process finished")
            sys.exit()

        if sys.argv[2] == "-e":

            conn_1 = media.connect_to_db(user=db_user, password=db_password, server=db_server, database="rslims")
            cursor = conn_1.cursor()
            cursor.execute("TRUNCATE TABLE komp.experimentsonspecimens;")
            conn_1.commit()
            conn_1.close()

            conn_2 = procedures.connect_to_db(user=db_user, password=db_password, server=db_server, database="rslims")
            cursor = conn_2.cursor()
            sql = utils.stmt_for_animalId
            cursor.execute(sql)
            animalIds = cursor.fetchall()
            conn_2.close()

            for item in animalIds:
                animalId = item[0]
                logger.info(f"Fetching record for {animalId}")
                result = procedures.filter_experiment_procedure_by(animalId=animalId,
                                                                   showDetails=True,
                                                                   status="active")
                logger.debug(f"Getting number of {len(result)} records back")

                logger.info(f"Start to insert records for animal id: {animalId}")
                procedures.insert_to_db(dataset=result,
                                        insert_type="specimen",
                                        username=db_user,
                                        password=db_password,
                                        server=db_server,
                                        database=db_name)

            logger.info("Done")
            print()
            print("\n-----------------------------------------------------")
            print("Process finished")
            sys.exit()

        if sys.argv[2] == "-l":

            conn_1 = procedures.connect_to_db(user=db_user, password=db_password, server=db_server,
                                              database="komp")
            cursor = conn_1.cursor()
            cursor.execute("SHOW COLUMNS FROM experimentsonlines;")
            queryResult = cursor.fetchall()
            logger.info(queryResult)

            logger.info("Remove records from table komp.experimentsonlines")
            cursor.execute("TRUNCATE TABLE komp.experimentsonlines;")
            conn_1.commit()

            conn_1.close()

            cols = []
            for item in queryResult[1:]:
                logger.debug(f"Adding {item[0]}")
                cols.append(item[0])

            # Statement to select
            stmt = utils.stmt_for_colonyId

            db_server_dev = "rslims-dev.jax.org"
            conn_2 = mysql.connector.connect(host=db_server_dev, user=db_user, password=db_password,
                                             database="rslims")
            cursor = conn_2.cursor()
            cursor.execute(stmt)
            colonyIds = cursor.fetchall()
            conn_2.close()

            for pair in colonyIds:
                colonyId = pair[0]
                logger.debug(f"Fetching result of experiments on line: {colonyId}")
                result = procedures.filter_line_procedure_by(columns=cols,
                                                             centre="J",
                                                             colonyId=colonyId,
                                                             showdetails=True,
                                                             start=0,
                                                             resultsize=2 ** 31 - 1)
                logger.debug(f"Getting number of {len(result)} records back")

                logger.info(f"Start to insert records for JR number: {colonyId}")
                procedures.insert_to_db(dataset=result,
                                        insert_type="line",
                                        username=db_user,
                                        password=db_password,
                                        server=db_server,
                                        database=db_name)

            logger.info("Done")
            print()
            print("\n-----------------------------------------------------")
            print("Process finished")
            sys.exit()

        if sys.argv[2] == "-xml":

            conn = dcc_files.connect_to_db(username=db_user, password=db_password, server=db_server, database=db_name)
            cursor = conn.cursor(buffered=True, dictionary=True)
            cursor.execute("SELECT DISTINCT * FROM KOMP.submittedprocedures;")
            rows = cursor.fetchall()

            '''GET COLUMN NAMES'''

            cursor.execute("SELECT * FROM dccXmlFileStatus;")
            fileStatusColNames = list(cursor.fetchall()[0].keys())[1:]

            cursor.execute("TRUNCATE TABLE dccXmlFileStatus;")
            conn.close()

            for row in rows:
                xmlFileName = row["XmlFilename"]
                db_obj = dcc_files.filter_xml_by(fileName=xmlFileName, columns=fileStatusColNames)
                print(db_obj)
                logger.debug(f"Insert record for file: {xmlFileName} to table")
                dcc_files.insert_to_db(db_object=db_obj, username=db_user, password=db_password, server=db_server,
                                       database=db_name)

            logger.info("Done")
            print()
            print("\n-----------------------------------------------------")
            print("Process finished")
            sys.exit()

    if sys.argv[1] == "ebi":

        print(f"\n------------ Fetching data for {sys.argv[1]} ----------")

        if sys.argv[2] == "-i":

            conn = ebi_media.connect_to_db(username=db_user, password=db_password, server=db_server, database=db_name)
            cursor = conn.cursor()
            cursor.execute("TRUNCATE TABLE KOMP.ebiimages;")
            cursor.commit()
            cursor.execute("""SHOW COLUMNS FROM KOMP.ebiimages;""")
            queryResult = cursor.fetchall()[1:]
            columns = []
            for item in queryResult:
                columns.append(item[0])
            conn.close()

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
                    result = ebi_media.filter_image_by(colonyId=colonyId,
                                                       parameter_stable_id=parameterKey,
                                                       center="JAX",
                                                       indent=True,
                                                       rows=2 ** 31 - 1)
                    logger.info(f"Getting number of {len(result)} result back")

                    logger.info(f"Insert records for JR number: {colonyId}, IMPC code: {parameterKey}")
                    ebi_media.insert_to_db(dataset=result,
                                           username=db_user,
                                           password=db_password,
                                           server=db_server,
                                           database=db_name)

            logger.info("Done")
            print()
            print("\n-----------------------------------------------------")
            print("Process finished")
            sys.exit()

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

                    logger.info(f"Insert records for JR number: {colonyId}, IMPC code: {parameterKey}")
                    ebi_procedure.insert_to_db(dataset=result,
                                               username=db_user,
                                               password=db_password,
                                               server=db_server,
                                               database=db_name)

            logger.info("Done")
            print("\n")
            print("\n-----------------------------------------------------")
            print("Process finished")
            sys.exit()

    else:
        print("Invalid command line arguments detected, please check the user manual.")
        sys.exit("usage eg:  python App.py IMPC -e  (eg. CBA302)")


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    main()
