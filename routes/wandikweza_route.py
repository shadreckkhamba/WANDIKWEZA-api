import traceback
from flask import jsonify, Blueprint
from extensions.extensions import logger
from datetime import datetime
import pytz
from services.wandikweza_patient_service import (
    get_patient_categories,
    get_patients_by_gender,
    get_patients_by_location,
    get_refunded_patients,
    get_refunded_patients,
    get_registered_patients
)

wandikweza_bp = Blueprint('wandikweza', __name__)

local_tz = pytz.timezone('Africa/Blantyre')

# Current time and start of month in UTC naive format, matching the service
end_date_local = datetime.now(local_tz)
start_date_local = end_date_local.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
start_date = start_date_local.astimezone(pytz.utc).replace(tzinfo=None)
end_date = end_date_local.astimezone(pytz.utc).replace(tzinfo=None)

@wandikweza_bp.route('/get_patient_data/', methods=['GET'])
def get_patient_demographics():
    try:
        age_categories = get_patient_categories()
        gender_counts = get_patients_by_gender()
        location_counts = get_patients_by_location()
        refunded_patients = get_refunded_patients()
        registered_patients = get_registered_patients()

        if None in (age_categories, gender_counts, location_counts, refunded_patients):
            raise ValueError("One of the queries returned None")

        return jsonify({
            "age_categories": age_categories,
            "gender_counts": gender_counts,
            "location_counts": location_counts,
            "refunded_patients": refunded_patients,
            "registered_patients": registered_patients
        }), 200
    except Exception as e:
        logger.error("Error in /get_patient_categories/:\n" + traceback.format_exc())
        return jsonify({"error": str(e)}), 500
