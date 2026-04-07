import os
from pathlib import Path

import yaml
import pymysql
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def resolve_config_path(config_file: str | None = None) -> Path:
    """
    Resolve the YAML config file path.

    Priority:
    1) Explicit argument
    2) CONFIG_FILE env var
    3) Repo defaults (config/dev_config.yaml or config/dev_config.yml)

    Using an absolute path avoids issues when the process CWD changes (e.g. gunicorn).
    """
    if config_file:
        return Path(config_file)

    env_path = os.getenv("CONFIG_FILE")
    if env_path:
        return Path(env_path)

    project_root = Path(__file__).resolve().parents[1]
    for rel in ("config/dev_config.yaml", "config/dev_config.yml"):
        candidate = project_root / rel
        if candidate.is_file():
            return candidate

    # Fallback for cases where project root resolution is unexpected.
    for rel in ("config/dev_config.yaml", "config/dev_config.yml"):
        candidate = Path(rel)
        if candidate.is_file():
            return candidate

    # Prefer .yml for this repo (config/dev_config.yml exists in-tree).
    return project_root / "config/dev_config.yml"


# Load configuration from YAML file
_config_path = resolve_config_path()
try:
    with _config_path.open("r") as config_file:
        config = yaml.safe_load(config_file) or {}
except FileNotFoundError:
    logger.error(
        "Config file not found: %s. Set CONFIG_FILE or create config/dev_config.yml.",
        _config_path,
    )
    config = {}
except yaml.YAMLError as e:
    logger.error("Invalid YAML in config file %s: %s", _config_path, e)
    config = {}

db_config = config.get("database") or {}

# Set up MariaDB connection using pymysql
conn = None
if db_config:
    try:
        conn = pymysql.connect(
            host=db_config.get("host"),
            user=db_config.get("username"),
            password=db_config.get("password"),
            db=db_config.get("database_name"),
            port=db_config.get("port"),
            cursorclass=pymysql.cursors.DictCursor,
            connect_timeout=5,
        )
        logger.info("Successfully connected to MariaDB.")
    except pymysql.MySQLError as e:
        logger.error(f"Error connecting to MariaDB: {e}")
        conn = None
else:
    logger.warning(
        "No 'database' section found in %s; DB connection is disabled.",
        _config_path,
    )
