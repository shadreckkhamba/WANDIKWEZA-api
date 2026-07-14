import yaml
import pymysql
import logging
from urllib.parse import urlparse

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load configuration from YAML file
with open('config/dev_config.yaml', 'r') as config_file:
    config = yaml.safe_load(config_file)

# Safely extract sections to prevent NoneType object subscriptable crashes
database_section = config.get('database') or {}
db_config = database_section.get('billing_import')

# Only attempt connection if the configurations were found successfully
if not db_config or 'db_uri' not in db_config:
    logger.error("CRITICAL: Could not find a valid 'billing_import' or 'db_uri' section in dev_config.yaml. Please verify your file indentation.")
    conn = None
else:
    # Set up MariaDB connection using pymysql by parsing the db_uri string
    try:
        parsed_url = urlparse(db_config['db_uri'])
        
        conn = pymysql.connect(
            host=parsed_url.hostname,
            user=parsed_url.username,
            password=parsed_url.password,
            db=parsed_url.path.lstrip('/'), 
            port=parsed_url.port if parsed_url.port else 3306,
            cursorclass=pymysql.cursors.DictCursor
        )
        logger.info("Successfully connected to MariaDB.")

    except pymysql.MySQLError as e:
        logger.error(f"Error connecting to MariaDB: {e}")
        conn = None