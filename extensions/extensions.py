import logging

import pymysql

from utils.config import get_database_config, load_config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

try:
    config = load_config()
    db_config = get_database_config(config)

    # Set up MariaDB connection using pymysql
    try:
        conn = pymysql.connect(
            host=db_config['host'],
            user=db_config['username'],
            password=db_config['password'],
            db=db_config['database_name'],
            port=db_config['port'],
            cursorclass=pymysql.cursors.DictCursor
        )
        logger.info("Successfully connected to MariaDB.")

    except pymysql.MySQLError as e:
        logger.error(f"Error connecting to MariaDB: {e}")
        conn = None

except FileNotFoundError:
    logger.error("Config file not found.")
    config = None
    conn = None
except Exception as e:
    logger.error(f"Error loading config: {e}")
    config = None
    conn = None
