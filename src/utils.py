import logging
import os
from datetime import datetime
from logging.handlers import RotatingFileHandler
from pathlib import Path


db_server = "rslims.jax.org"
db_user = "dba"
db_password = "rsdba"
db_name = "komp"

parameterKeys = ["IMPC_XRY_034_001",
                 "IMPC_EYE_050_001",
                 "IMPC_XRY_048_001",
                 "IMPC_XRY_049_001",
                 "IMPC_XRY_050_001",
                 "IMPC_XRY_051_001",
                 "IMPC_CSD_085_001",
                 "IMPC_ABR_014_001",
                 "JAX_ERG_047_001",
                 "JAX_SLW_016_001",
                 "IMPC_ALZ_075_001",
                 "IMPC_ALZ_076_001",
                 "IMPC_ELZ_064_001",
                 "IMPC_HIS_177_001",
                 "IMPC_EOL_001_001",
                 "IMPC_EOL_012_001",
                 "IMPC_EMO_001_001",
                 "IMPC_EMO_017_001",
                 "IMPC_GEM_049_001",
                 "IMPC_GEO_050_001",
                 "IMPC_GEP_064_001",
                 "IMPC_GEL_044_001",
                 "IMPC_EMA_001_001",
                 "IMPC_EMA_017_001",
                 "IMPC_GPL_007_001",
                 "IMPC_GPM_007_001",
                 "IMPC_GPO_007_001",
                 "IMPC_GPP_007_001",
                 "IMPC_ECG_025_001",
                 "JAX_ERG_028_001",
                 "IMPC_WEL_003_001",
                 "IMPC_PAT_057_002"]

stmt_for_colonyId = """SELECT DISTINCT
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
                        _ProcedureDefinitionVersion_key IN (239, 276, 87, 243, 245, 244, 246);"""

stmt_for_animalId = "SELECT OrganismID FROM Organism INNER JOIN OrganismStudy USING (_Organism_key) WHERE _Study_key " \
                    "IN (27, 28, 57);"

"""Function to get work directory"""


def get_project_root() -> Path:
    return Path(__file__).parent.parent


"""Setup logger"""


def createLogHandler(job_name, log_file):
    logger = logging.getLogger(__name__)
    FORMAT = "[%(asctime)s->%(filename)s->%(funcName)s():%(lineno)s]%(levelname)s: %(message)s"
    logging.basicConfig(format=FORMAT, filemode="w", level=logging.DEBUG, force=True)
    handler = logging.FileHandler(log_file)
    handler.setFormatter(logging.Formatter(FORMAT))
    logger.addHandler(handler)

    return logger