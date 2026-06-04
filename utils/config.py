import os
import re
from typing import Any

import yaml


DEFAULT_CONFIG_FILE = "config/dev_config.yaml"


def _expand_env_vars(value: Any) -> Any:
    """Recursively expand environment variables inside config values."""
    if isinstance(value, dict):
        return {key: _expand_env_vars(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_expand_env_vars(item) for item in value]
    if isinstance(value, str):
        return re.sub(r"\$\{([^}]+)\}", lambda match: os.getenv(match.group(1), ""), value)
    return value


def load_config(config_file_path: str | None = None) -> dict[str, Any]:
    """Load the configured YAML file and expand any ${ENV_VAR} references."""
    path = config_file_path or os.getenv("CONFIG_FILE", DEFAULT_CONFIG_FILE)

    with open(path, "r") as config_file:
        loaded = yaml.safe_load(config_file) or {}

    return _expand_env_vars(loaded)


def get_database_config(config: dict[str, Any], db_key: str = "billing_import") -> dict[str, Any]:
    """Support both flat and keyed database configs."""
    db_root = config.get("database", {})

    if not isinstance(db_root, dict):
        raise ValueError("Invalid database configuration format")

    if "host" in db_root:
        return db_root

    db_config = db_root.get(db_key)
    if not db_config:
        raise ValueError(f"Database configuration '{db_key}' not found in config file")

    return db_config


def as_bool(value: Any, default: bool = False) -> bool:
    """Parse booleans safely from YAML values or environment variable strings."""
    if value is None:
        return default

    if isinstance(value, bool):
        return value

    if isinstance(value, (int, float)):
        return bool(value)

    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"1", "true", "yes", "on"}:
            return True
        if normalized in {"0", "false", "no", "off"}:
            return False

    return default
