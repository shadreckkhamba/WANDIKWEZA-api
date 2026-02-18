# app.py
from flask import Flask
from extensions.extensions import config  # only needed to get the SECRET_KEY

def create_app():
    app = Flask(__name__, template_folder="templates", static_folder="static")

    # Load config just for the secret key
    
    app.secret_key = config.get('SECRET_KEY', 'default_secret_key')

    # Register Blueprints
    from routes.wandikweza_route import wandikweza_bp
    from routes.rr_route import rr_bp
    app.register_blueprint(wandikweza_bp, url_prefix='/wandikweza')
    app.register_blueprint(rr_bp, url_prefix='/rr')

    return app

if __name__ == '__main__':
    app = create_app()
    app.run(host='0.0.0.0', port=5001, debug=True)
