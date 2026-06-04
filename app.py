import logging
import os
import secrets

from flask import Flask

from middlewares.security import configure_security
from utils.config import load_config


logger = logging.getLogger(__name__)

def create_app():
    app = Flask(__name__, template_folder="templates", static_folder="static")

    # Load config safely
    config_file = os.getenv("CONFIG_FILE", "config/dev_config.yaml")
    full_config = load_config(config_file)

    app_config = full_config.get("app", {})

    secret_key = app_config.get("SECRET_KEY") or secrets.token_hex(32)
    if not app_config.get("SECRET_KEY"):
        logger.warning("SECRET_KEY not set. Generated an ephemeral key for this process.")

    app.secret_key = secret_key

    configure_security(app, full_config)

    # Register Blueprints
    from routes.wandikweza_route import wandikweza_bp
    from routes.rr_route import rr_bp

    app.register_blueprint(wandikweza_bp, url_prefix="/wandikweza")
    app.register_blueprint(rr_bp, url_prefix="/rr")

    return app
