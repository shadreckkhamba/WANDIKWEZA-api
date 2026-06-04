from datetime import datetime, timezone

from flask import Blueprint, jsonify

rr_bp = Blueprint('rr', __name__)


@rr_bp.route('/health/', methods=['GET'])
def health_check():
    return jsonify({
        "status": "ok",
        "timestamp": datetime.now(timezone.utc).isoformat()
    }), 200
