import threading
import os
from app import create_app
from services.wandikweza_patient_service import push_payload_to_virtual_server

app = create_app()

if os.environ.get("WERKZEUG_RUN_MAIN") != "true":  # only start thread once
    def run_payload_loop():
        print("Starting payload push thread...")
        push_payload_to_virtual_server()

    t = threading.Thread(target=run_payload_loop, daemon=True)
    t.start()
