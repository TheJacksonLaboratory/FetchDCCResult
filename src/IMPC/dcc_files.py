import logging
import mysql.connector
import requests
import re
from datetime import datetime, timedelta

logger = logging.getLogger("__main__")


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

def get_date_days_earlier(days:int) -> str:
        today = datetime.today()
        two_months_earlier = today - timedelta(days=days)
        return two_months_earlier.strftime('%Y-%m-%d')


def get_xmls_with_issues(columns: list[str],days_to_search_back:int) -> list[dict]:

    start_date = get_date_days_earlier(days_to_search_back)
    
    url = f"https://api.mousephenotype.org/tracker/centre/xml/issues?centre=J&start=0&resultsize=5000&updatedSinceDate={start_date}" # TODO Make start date configurable
    #logger.debug(url)
    db_obj_ls = []
    try:
        response = requests.get(url).json()
        status = 1 if len(response) > 0 else 0
        '''Data found'''
        if status == 1:
            for json_obj in response:
                db_obj = {}
                for key, val in json_obj.items():
                    def is_substring_of(keyName: str, columns: list[str]) -> bool:
                        if not keyName or not columns:
                            return False
                        return any(filter(lambda x: keyName in x, columns))

                    if is_substring_of(keyName=key, columns=columns):
                        logger.debug(f"Adding {key} to database record")
                        db_obj[key] = val
                db_obj["lastUpdatedDate"] = json_obj["lastUpatedDate"]
                db_obj["xmlId"] = json_obj["filename"].split(".")[2]
                db_obj_ls.append(db_obj)
                
            return db_obj_ls

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
        
        
def sanitize_input(value):
    # Define the allowed characters (alphanumeric and space in this example)
    allowed_characters = re.compile(r'[^a-zA-Z0-9\.\(\){} ]')
    # Remove unallowed characters
    sanitized_value = allowed_characters.sub('', value)
    return sanitized_value

def get_xml_log_messages_for_db(expfilename:str, zipfilename:str) -> list[dict]:
    # Get the log files for the experiments in the zip file
    # For us it is one experiment per zip file
    url = f"https://api.mousephenotype.org/tracker/zip/{zipfilename}/{expfilename}"
    
    response = requests.get(url).json() 
    # Return the logs
    if response == None:
        return []
    if len(response) == 0:
        return []   
    if "logs" not in response[0]:
        return []   
    
    return response[0]["logs"]
 
def get_experiment_log_messages_for_db(experimentFileName:str) -> list[dict]:
    # Get the log files for the experiments in the zip file
    
    url = f"https://api.mousephenotype.org/tracker/xml/{experimentFileName}?detail=full"
    response = requests.get(url).json() # A list
    if response == None:
        return []   
    
    exp_objs = []  # A list of dicts ready for insertion
    exp_id_ls = response[0]["experimentProcedures"]
    for experiment_id in exp_id_ls:
        ls = get_experiment_log_messages_for_experiment(experiment_id)
        exp_objs.append(ls)
        
    return exp_objs

    
def get_experiment_log_messages_for_experiment(experimentId:int) -> list[dict]:
    # Get the log files for the experiments in the zip file
    # For us it is one experiment per zip file
    try:
        url = f"https://api.mousephenotype.org/tracker/trackerexperiment/{experimentId}"
        response = requests.get(url).json() # A dict
          
        if response == None:
            return []
        if len(response) == 0:
            return []   
        if "logs" not in response.keys():
            return []   
        if len(response["logs"]) == 0:
            return []   
    except:
        return []
    
    exp_objs = []  # A list of dicts ready for insertion
    for log in response["logs"]:
        exp_obj = {}
        exp_obj["experimentName"] = response["experimentName"]
        exp_obj["specimen"] = response["specimen"]
        exp_obj["age"] = response["age"]
        exp_obj["status"] = response["status"]
        exp_obj["parameterKey"] = log["parameterKey"]
        exp_obj["message"] = log["message"]
        exp_objs.append(exp_obj)
        
    return exp_objs
    
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
        logger.debug(f"Assigning {val} to key {key}")
        row[key] = val

    logger.info("Start to insert to file status table")
    cursor = conn.cursor()
    
    # First: KOMP.dccXmlFileStatus
    placeholders = ', '.join(['%s'] * len(row))
    columns = ', '.join(row.keys())
    stmt = "INSERT INTO %s ( %s ) VALUES ( %s );" % ("KOMP.dccXmlFileStatus", columns, placeholders)
    cursor.execute(stmt, list(row.values()))
    
    # Second: KOMP.xmlFileLogMessage
    logs = get_xml_log_messages_for_db(db_object["filename"], db_object["zipName"])
    for log in logs:
        stmt = "INSERT INTO KOMP.xmlFileLogMessage (filename,lineNumber,columnNumber,fatality, message) VALUES ( '{0}', {1}, {2}, '{3}', '{4}');".format(db_object["filename"], log["line"], log["column"], log["fatality"], sanitize_input(log["message"])) 
        #logger.debug(stmt)
        cursor.execute(stmt)
        
    # Third: komp.dccxmlprocedureissues
    experiment_logs_ls = get_experiment_log_messages_for_db(db_object["filename"]) # List of exp Ids
    for log_ls in experiment_logs_ls:
        for log in log_ls:
            """
            log looks like:
            {
            'experimentName': 'KOMP_OPEN_FIELD_EXPERIMENT - A-55490 - 248145326',
            'specimen': 'A-55490',
            'age': '8.14 weeks',
            'status': 'failed',
            'parameterKey': None,
            'message': 'Missing required parameter JAX_OFD_018_001 for procedure JAX_OFD_001'
            }
            """ 
            if log["parameterKey"] == None:
                log["parameterKey"] = "None"
                
            #logger.info(log)
            stmt = "INSERT INTO komp.dccxmlprocedureissues (xmlFileName,experimentName,specimen,age,status,parameterKey,message) VALUES ( '{0}', '{1}', '{2}', '{3}', '{4}', '{5}', '{6}')".format(db_object["filename"], log["experimentName"], log["specimen"], log["age"], log["status"], log["parameterKey"], log["message"])
            #logger.debug(stmt)
            cursor.execute(stmt)
    
    conn.commit()
    conn.close()
    logger.info("All insertions has been done, db connection closed.")

if __name__ == "__main__":
    get_date_two_months_earlier()
    

"""
                CREATE TABLE IF NOT EXISTS komp.dccxmlprocedureissues (
	_dccxmlprocedureissues	int(11) NOT NULL auto_increment,
	xmlFileName varchar(128)  NOT NULL,
	experimentName varchar(128)  NOT NULL,
	specimen varchar(64)  NOT NULL,
	age varchar(32)  NOT NULL,
	status varchar(32)  NOT NULL,
	parameterKey varchar(32),
	message varchar(1024)  NOT NULL,
	PRIMARY KEY  (_dccxmlprocedureissues)
) ENGINE=InnoDB;
"""