import yaml
import pymysql
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load configuration from YAML file
with open('config/dev_config.yaml', 'r') as config_file:
    config = yaml.safe_load(config_file)

db_config = config['database']

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
