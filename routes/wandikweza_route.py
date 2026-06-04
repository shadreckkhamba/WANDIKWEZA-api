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
    get_registered_patients,
    get_patient_visits_current_month
)

wandikweza_bp = Blueprint('wandikweza', __name__)

local_tz = pytz.timezone('Africa/Blantyre')


# Demographics endpoint :aggregated data
@wandikweza_bp.route('/get_patient_data/', methods=['GET'])
def get_patient_demographics():
    try:
        age_categories = get_patient_categories()
        gender_counts = get_patients_by_gender()
        location_counts = get_patients_by_location()
        refunded_patients = get_refunded_patients()
        registered_patients = get_registered_patients()

        if None in (
            age_categories,
            gender_counts,
            location_counts,
            refunded_patients,
            registered_patients
        ):
            raise ValueError("One of the demographic queries returned None")

        return jsonify({
            "age_categories": age_categories,
            "gender_counts": gender_counts,
            "location_counts": location_counts,
            "refunded_patients": refunded_patients,
            "registered_patients": registered_patients
        }), 200

    except Exception:
        logger.error("Error in /get_patient_data/:\n" + traceback.format_exc())
        return jsonify({"error": "Failed to fetch patient demographics"}), 500


# Stay times / visits endpoint with time-series data
@wandikweza_bp.route('/stay_times/', methods=['GET'])
def get_patient_stay_times():
    try:
        patient_visits = get_patient_visits_current_month()

        if patient_visits is None:
            raise ValueError("Visits query returned None")

        return jsonify({
            "patient_visits": patient_visits,
            "count": len(patient_visits),
            "generated_at": datetime.now(local_tz).isoformat()
        }), 200

    except Exception:
        logger.error("Error in /stay_times/:\n" + traceback.format_exc())
        return jsonify({"error": "Failed to fetch patient stay times"}), 500