import logging
import mysql.connector
import requests

logger = logging.getLogger("__main__")


def connect_to_db(username: str,
                  password: str,
                  server: str,
                  database: str) -> mysql.connector.connection:
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


def filter_xml_by(fileName: str,
                  columns: list[str]) -> dict:
    if not fileName:
        logger.warning("No input found")
        return {}

    url = "https://api.mousephenotype.org/tracker/xml/" + fileName
    print(url)
    logger.debug(url)

    try:
        response = requests.get(url).json()
        status = 1 if response["total"] > 0 else 0
        '''Data found'''
        if status == 1:
            db_obj = {}
            for json_obj in response:
                for key, val in json_obj.items():

                    def is_substring_of(keyName: str, columns: list[str]) -> bool:
                        if not keyName or not columns:
                            return False
                        return any(filter(lambda x: keyName in x, columns))

                    if is_substring_of(keyName=key, columns=columns):
                        logger.debug(f"Adding {key} to database record")
                        db_obj[key] = val
                    db_obj["logs"] = json_obj["logs"]
                    db_obj["lastUpdatedDate"] = json_obj["lastUpatedDate"]
                    db_obj["xmlId"] = json_obj["filename"].split(".")[2]

            return db_obj

        else:
            logger.info(f"No record found at {url}")
            return {}

    except requests.exceptions.HTTPError as err1:
        logger.error(err1)

    except requests.exceptions.ConnectionError as err2:
        logger.error(err2)

    except requests.exceptions.Timeout as err3:
        logger.error(err3)

    except requests.exceptions.RequestException as err4:
        logger.error(err4)


def insert_to_db(db_object: dict,
                 username: str,
                 password: str,
                 server: str,
                 database) -> None:
    if not db_object:
        logger.error("No data associated with the given xml file")
        return

    print(db_object)
    conn = mysql.connector.connect(host=server, user=username, password=password, database=database)
    row = {}

    for key, val in db_object.items():
        if key == "logs":
            print(len(val))

            def process_logs(logs: list[dict]) -> list[dict]:
                if not logs:
                    return []

                cursor = conn.cursor()
                cursor.execute("SHOW COLUMNS FROM xmlFileLogMessage;")
                queryResult = cursor.fetchall()
                log_records = []
                for log in logs:
                    log["filename"] = db_object["filename"]

                    # Store filename to  the
                    record = {"filename": db_object["filename"]}
                    columns = queryResult[2:]
                    for col in columns:
                        for key, val in log.items():
                            # print(key)
                            if col[0].find(key) != -1:
                                logger.info(f"Adding {key}")
                                record[col[0]] = val

                    log_records.append(record)

                return log_records

            log_records = process_logs(val)
            logger.debug(len(log_records))
            print(len(log_records))

            logger.info("Start to insert to log table")
            for record in log_records:
                cursor = conn.cursor()
                placeholders = ', '.join(['%s'] * len(record))
                columns = ', '.join(record.keys())
                stmt = "INSERT INTO %s ( %s ) VALUES ( %s );" % ("KOMP.xmlFileLogMessage", columns, placeholders)
                logger.debug(stmt)
                cursor.execute(stmt, list(record.values()))
                conn.commit()

            logger.info("Done")

        else:
            logger.debug(f"Assigning {val} to key {key}")
            row[key] = val

    logger.info("Start to insert to file status table")
    cursor = conn.cursor()
    logger.info(row)
    placeholders = ', '.join(['%s'] * len(row))
    columns = ', '.join(row.keys())
    stmt = "INSERT INTO %s ( %s ) VALUES ( %s );" % ("KOMP.dccXmlFileStatus", columns, placeholders)
    logger.debug(stmt)
    cursor.execute(stmt, list(row.values()))

    conn.close()
    logger.info("All insertions has been done, db connection closed.")
