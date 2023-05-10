import sys
from http.client import HTTPConnection
from socket import socket
import utils
import os
import Media.media as media
import logging
from logging.handlers import RotatingFileHandler
import datetime
import mysql.connector
import experiments_procedure.experiments as specimen
import line_procedures.lines as line
import DCC_Files.xml as xml

"""Setup logger"""

logging_dest = os.path.join(os.getcwd(), "logs")
try:
    os.mkdir(logging_dest)

except OSError as e:
    print(e)

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
    db_server = "rslims.jax.org"
    db_user = "dba"
    db_password = "rsdba"
    db_name = "komp"

    if sys.argv[1] == "-i":
        conn = media.connect_to_db(user=db_user, password=db_password, server=db_server, database="rslims")
        cursor = conn.cursor()
        cursor.execute("TRUNCATE TABLE komp.dccImages;")
        conn.commit()
        conn.close()

        for impcCode in parameterKeys:
            logger.info("Getting result from IMPC")
            result = media.filter_image_by(parameterKey=impcCode, start=0, resultsize=2**31 - 1)
            logger.info("Inserting . . . ")
            media.insert_to_db(dataset=result,
                               insert_type="images",
                               username=db_user,
                               password=db_password,
                               server=db_server,
                               database=db_name)

    if sys.argv[1] == "-e":

        conn = media.connect_to_db(user=db_user, password=db_password, server=db_server, database="rslims")
        cursor = conn.cursor()
        cursor.execute("TRUNCATE TABLE komp.experimentsonspecimens;")
        conn.commit()
        conn.close()

        conn = specimen.connect_to_db(user=db_user, password=db_password, server=db_server, database="rslims")
        cursor = conn.cursor()
        sql = "SELECT OrganismID FROM Organism INNER JOIN OrganismStudy USING (_Organism_key) WHERE _Study_key IN (27, 28, 57);"
        cursor.execute(sql)
        animalIds = cursor.fetchall()
        conn.close()

        for item in animalIds:
            animalId = item[0]
            call_back = specimen.filter_experiment_procedure_by(animalId=animalId, showDetails=True, status="active")
            specimen.insert_to_db(dataset=call_back, insert_type="specimen",
                               username=db_user,
                               password=db_password,
                               server=db_server,
                               database=db_name)


    if sys.argv[1] == "-l":

        conn_to_prod = line.connect_to_db(user=db_user, password=db_password, server=db_server, database="komp")
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
        conn_to_dev = mysql.connector.connect(host=db_server_dev, user=db_user, password=db_password,
                                              database="rslims")
        cursor = conn_to_dev.cursor()
        cursor.execute(stmt)
        colonyIds = cursor.fetchall()
        print(colonyIds)
        conn_to_dev.close()

        for pair in colonyIds:
            colonyId = pair[0]
            result = line.filter_line_procedure_by(columns=cols,
                                                   centre="J",
                                                   colonyId=colonyId,
                                                   showdetails=True,
                                                   start=0,
                                                   resultsize=2 ** 31 - 1)
            logger.info("Inserting . . .")
            line.insert_to_db(dataset=result,
                         insert_type="line",
                         username=db_user,
                         password=db_password,
                         server=db_server,
                         database=db_name)

        logger.info("Done")
        

    if sys.argv[1] == "-xml":
        
        conn = xml.connect_to_db(user=db_user, password=db_password, server=db_server, database=db_name)
        cursor = conn.cursor(buffered=True, dictionary=True)
        cursor.execute("SELECT DISTINCT * FROM KOMP.submittedprocedures;")
        rows = cursor.fetchall()

        '''GET COLUMN NAMES'''
        
        cursor.execute("SELECT * FROM dccXmlFileStatus;")
        fileStatusColNames = list(cursor.fetchall()[0].keys())[1:]
        
        cursor.execute("TRUNCATE TABLE dccXmlFileStatus;")

        #db_obj = xml.filter_xml_by(fileName="J.2023-03-02.50.experiment.impc.xml", columns=fileStatusColNames)
        #print(db_obj)
        
        for row in rows:
            xmlFileName = row["XmlFilename"]
            db_obj = xml.filter_xml_by(fileName=xmlFileName, columns=fileStatusColNames)
            print(db_obj)
            xml.insert_to_db(db_object=db_obj, username=db_user, password=db_password, server=db_server, database=db_name)
        
        conn.close()


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    main()
