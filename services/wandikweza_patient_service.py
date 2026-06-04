
import traceback
import os
import time
import threading
from datetime import date
from datetime import datetime
import gzip
import json
import logging
from urllib.parse import urljoin

import pytz
import pymysql
import requests
from pymysql.cursors import DictCursor

from extensions.extensions import logger
from utils.config import as_bool, get_database_config, load_config

def get_fresh_connection(db_key='billing_import'):
    """Get a fresh database connection using config file settings.

    Args:
        db_key (str): Key of the database in the YAML config.
                       Defaults to primary database.

    Returns:
        pymysql.connections.Connection: A fresh MySQL connection.
    """
    try:
        config = load_config()

        # Get the specific database config
        db_config = get_database_config(config, db_key)
        
        # Validate required config fields
        required_fields = ['host', 'username', 'password', 'database_name', 'port']
        missing_fields = [field for field in required_fields if not db_config.get(field)]
        if missing_fields:
            raise ValueError(f"Missing required database config fields for '{db_key}': {missing_fields}")
        
        return pymysql.connect(
            host=db_config['host'],
            user=db_config['username'],
            password=db_config['password'],
            database=db_config['database_name'],
            port=db_config['port'],
            cursorclass=DictCursor
        )

    except FileNotFoundError:
        config_file_path = os.getenv('CONFIG_FILE', 'config/dev_config.yaml')
        logger.error(f"Config file not found: {config_file_path}")
        raise Exception(f"Database config file not found: {config_file_path}")
    except Exception as e:
        config_file_path = os.getenv('CONFIG_FILE', 'config/dev_config.yaml')
        logger.error(f"Failed to load database config for '{db_key}' from {config_file_path}: {e}")
        raise Exception(f"Database configuration error for '{db_key}': {e}")

# --- File-based state tracking functions for incremental push ---
STATE_FILE_PATH = 'push_state.json'

def load_push_state():
    """Load push state from JSON file"""
    try:
        if os.path.exists(STATE_FILE_PATH):
            with open(STATE_FILE_PATH, 'r') as f:
                state = json.load(f)
                # Convert string timestamps back to datetime objects
                for data_type in state:
                    if state[data_type]['last_sent_timestamp']:
                        state[data_type]['last_sent_timestamp'] = datetime.fromisoformat(
                            state[data_type]['last_sent_timestamp']
                        )
                return state
        else:
            # Initialize empty state
            return {
                'age_categories': {'last_sent_timestamp': None, 'updated_at': None},
                'gender_counts': {'last_sent_timestamp': None, 'updated_at': None},
                'location_counts': {'last_sent_timestamp': None, 'updated_at': None},
                'refunded_patients': {'last_sent_timestamp': None, 'updated_at': None},
                'registered_patients': {'last_sent_timestamp': None, 'updated_at': None},
                'patient_visits': {'last_sent_timestamp': None, 'updated_at': None},
            }
    except Exception as e:
        logger.error(f"Error loading push state: {e}")
        return {}

def save_push_state(state):
    """Save push state to JSON file"""
    try:
        # Convert datetime objects to strings for JSON serialization
        state_to_save = {}
        for data_type, data in state.items():
            state_to_save[data_type] = {
                'last_sent_timestamp': data['last_sent_timestamp'].isoformat() if data['last_sent_timestamp'] else None,
                'updated_at': datetime.now().isoformat()
            }
        
        with open(STATE_FILE_PATH, 'w') as f:
            json.dump(state_to_save, f, indent=2)
        return True
    except Exception as e:
        logger.error(f"Error saving push state: {e}")
        return False

def get_last_sent_timestamp(data_type):
    """Get the last sent timestamp for a specific data type"""
    try:
        state = load_push_state()
        return state.get(data_type, {}).get('last_sent_timestamp')
    except Exception as e:
        logger.error(f"Error getting last sent timestamp for {data_type}: {e}")
        return None

def update_last_sent_timestamp(data_type, timestamp):
    """Update the last sent timestamp for a specific data type"""
    try:
        state = load_push_state()
        if data_type not in state:
            state[data_type] = {'last_sent_timestamp': None, 'updated_at': None}
        
        state[data_type]['last_sent_timestamp'] = timestamp
        state[data_type]['updated_at'] = datetime.now()
        
        if save_push_state(state):
            logger.info(f"Updated last sent timestamp for {data_type}: {timestamp}")
            return True
        return False
    except Exception as e:
        logger.error(f"Error updating last sent timestamp for {data_type}: {e}")
        return False

def get_max_timestamp_from_records(records, timestamp_field='time_stamp'):
    """Get the maximum timestamp from a list of records"""
    if not records:
        logger.warning(f"No records provided for timestamp extraction (field: {timestamp_field})")
        return None
    
    max_timestamp = None
    valid_timestamps = 0
    
    for record in records:
        ts = record.get(timestamp_field)
        if ts:
            # Convert to datetime if it's a string
            if isinstance(ts, str):
                try:
                    ts = datetime.strptime(ts, '%Y-%m-%d %H:%M:%S')
                    valid_timestamps += 1
                except ValueError:
                    logger.warning(f"Invalid timestamp format: {ts}")
                    continue
            elif isinstance(ts, date) and not isinstance(ts, datetime):
                ts = datetime.combine(ts, datetime.min.time())
                valid_timestamps += 1
            elif isinstance(ts, datetime):
                valid_timestamps += 1
            
            if max_timestamp is None or ts > max_timestamp:
                max_timestamp = ts
        else:
            logger.warning(f"Record missing timestamp field '{timestamp_field}': {record}")
    
    logger.info(f"Extracted max timestamp from {valid_timestamps}/{len(records)} records (field: {timestamp_field}): {max_timestamp}")
    return max_timestamp

local_tz = pytz.timezone('Africa/Blantyre')
# --- data query function ---
def get_patient_categories(last_sent_timestamp=None):
    """Get patient categories with incremental support"""
    try:
        with get_fresh_connection().cursor() as cur:
            if last_sent_timestamp:
                # Incremental query - only get records after last sent timestamp
                query = """
                    WITH detailed_data AS (
                        SELECT
                            'Under 5' AS category,
                            p.patient_id AS patient_id,
                            p.date_created AS time_stamp
                        FROM patient p
                        JOIN person per ON p.patient_id = per.person_id
                        WHERE p.voided = 0
                        AND per.voided = 0
                        AND TIMESTAMPDIFF(YEAR, per.birthdate, p.date_created) < 5
                        AND p.date_created > %s

                        UNION ALL

                        SELECT
                            'Pregnant Women' AS category,
                            p.patient_id AS patient_id,
                            o.order_date AS time_stamp
                        FROM order_entries o
                        JOIN patient p ON o.patient_id = p.patient_id
                        JOIN services s ON o.service_id = s.service_id
                        WHERE o.voided = 0
                        AND p.voided = 0
                        AND s.name = 'Female antenatal'
                        AND o.order_date > %s

                        UNION ALL

                        SELECT
                            'Adolescents' AS category,
                            p.patient_id AS patient_id,
                            p.date_created AS time_stamp
                        FROM patient p
                        JOIN person per ON p.patient_id = per.person_id
                        WHERE p.voided = 0
                        AND per.voided = 0
                        AND TIMESTAMPDIFF(YEAR, per.birthdate, p.date_created) BETWEEN 10 AND 19
                        AND p.date_created > %s
                    ),
                    totals AS (
                        SELECT category, COUNT(DISTINCT patient_id) AS total
                        FROM detailed_data
                        GROUP BY category
                    )
                    SELECT d.category, d.patient_id, d.time_stamp, t.total
                    FROM detailed_data d
                    JOIN totals t ON d.category = t.category
                    ORDER BY d.category, d.time_stamp;
                """
                cur.execute(query, (last_sent_timestamp, last_sent_timestamp, last_sent_timestamp))
            else:
                # Full query - get all records from current month
                query = """
                    WITH detailed_data AS (
                        SELECT
                            'Under 5' AS category,
                            p.patient_id AS patient_id,
                            p.date_created AS time_stamp
                        FROM patient p
                        JOIN person per ON p.patient_id = per.person_id
                        WHERE p.voided = 0
                        AND per.voided = 0
                        AND TIMESTAMPDIFF(YEAR, per.birthdate, p.date_created) < 5
                        AND p.date_created >= DATE(CONCAT(YEAR(CURDATE()), '-', MONTH(CURDATE()), '-01'))
                        AND p.date_created < DATE(CONCAT(YEAR(CURDATE()), '-', MONTH(CURDATE()) + 1, '-01'))

                        UNION ALL

                        SELECT
                            'Pregnant Women' AS category,
                            p.patient_id AS patient_id,
                            o.order_date AS time_stamp
                        FROM order_entries o
                        JOIN patient p ON o.patient_id = p.patient_id
                        JOIN services s ON o.service_id = s.service_id
                        WHERE o.voided = 0
                        AND p.voided = 0
                        AND s.name = 'Female antenatal'
                        AND o.order_date >= DATE(CONCAT(YEAR(CURDATE()), '-', MONTH(CURDATE()), '-01'))
                        AND o.order_date < DATE(CONCAT(YEAR(CURDATE()), '-', MONTH(CURDATE()) + 1, '-01'))

                        UNION ALL

                        SELECT
                            'Adolescents' AS category,
                            p.patient_id AS patient_id,
                            p.date_created AS time_stamp
                        FROM patient p
                        JOIN person per ON p.patient_id = per.person_id
                        WHERE p.voided = 0
                        AND per.voided = 0
                        AND TIMESTAMPDIFF(YEAR, per.birthdate, p.date_created) BETWEEN 10 AND 19
                        AND p.date_created >= DATE(CONCAT(YEAR(CURDATE()), '-', MONTH(CURDATE()), '-01'))
                        AND p.date_created < DATE(CONCAT(YEAR(CURDATE()), '-', MONTH(CURDATE()) + 1, '-01'))
                    ),
                    totals AS (
                        SELECT category, COUNT(DISTINCT patient_id) AS total
                        FROM detailed_data
                        GROUP BY category
                    )
                    SELECT d.category, d.patient_id, d.time_stamp, t.total
                    FROM detailed_data d
                    JOIN totals t ON d.category = t.category
                    ORDER BY d.category, d.time_stamp;
                """
                cur.execute(query)
            
            rows = cur.fetchall()
            logger.info(f"Fetched {len(rows)} age category records (incremental: {last_sent_timestamp is not None})")
            return rows

    except Exception:
        logger.error("Error in get_patient_categories:\n" + traceback.format_exc())
        return None

def get_patients_by_gender(last_sent_timestamp=None):
    """Get patients by gender with incremental support - based on registrations, not visits"""
    try:
        with get_fresh_connection().cursor(DictCursor) as cur:
            if last_sent_timestamp:
                query = """
                    SELECT
                        p.patient_id,
                        LOWER(per.gender) AS gender,
                        p.date_created AS first_visit_this_month
                    FROM patient p
                    JOIN person per ON p.patient_id = per.person_id
                    WHERE p.voided = 0
                      AND per.voided = 0
                      AND p.date_created > %s
                """
                cur.execute(query, (last_sent_timestamp,))
            else:
                query = """
                    SELECT
                        p.patient_id,
                        LOWER(per.gender) AS gender,
                        p.date_created AS first_visit_this_month
                    FROM patient p
                    JOIN person per ON p.patient_id = per.person_id
                    WHERE p.voided = 0
                      AND per.voided = 0
                      AND p.date_created >= DATE(CONCAT(YEAR(CURDATE()), '-', MONTH(CURDATE()), '-01'))
                      AND p.date_created < DATE(CONCAT(YEAR(CURDATE()), '-', MONTH(CURDATE()) + 1, '-01'))
                """
                cur.execute(query)
            
            rows = cur.fetchall()

            # Calculate gender totals
            gender_totals = {}
            for row in rows:
                gender = row['gender']
                gender_totals[gender] = gender_totals.get(gender, 0) + 1

            # Add total count per row
            for row in rows:
                row['total'] = gender_totals.get(row['gender'], 0)
                row['time_stamp'] = row['first_visit_this_month']

            logger.info(f"Fetched {len(rows)} gender records based on REGISTRATIONS (incremental: {last_sent_timestamp is not None})")
            return rows

    except Exception:
        logger.error("Error in get_patients_by_gender:\n" + traceback.format_exc())
        return None

def get_refunded_patients(last_sent_timestamp=None):
    """Get refunded patients with incremental support"""
    try:
        with get_fresh_connection().cursor(DictCursor) as cur:
            if last_sent_timestamp:
                query = """
                    SELECT 
                        r.patient_id,
                        op.updated_at AS refund_timestamp,
                        1 AS count,
                        (
                            SELECT COUNT(DISTINCT r2.patient_id)
                            FROM order_payments op2
                            JOIN receipts r2 ON op2.receipt_number = r2.receipt_number
                            WHERE op2.voided = 1
                              AND op2.updated_at IS NOT NULL
                              AND op2.updated_at > %s
                        ) AS total
                    FROM order_payments op
                    JOIN receipts r ON op.receipt_number = r.receipt_number
                    WHERE op.voided = 1
                      AND op.updated_at IS NOT NULL
                      AND op.updated_at > %s
                    ORDER BY op.updated_at;
                """
                cur.execute(query, (last_sent_timestamp, last_sent_timestamp))
            else:
                query = """
                    SELECT 
                        r.patient_id,
                        op.updated_at AS refund_timestamp,
                        1 AS count,
                        (
                            SELECT COUNT(DISTINCT r2.patient_id)
                            FROM order_payments op2
                            JOIN receipts r2 ON op2.receipt_number = r2.receipt_number
                            WHERE op2.voided = 1
                              AND op2.updated_at IS NOT NULL
                              AND op2.updated_at >= DATE(CONCAT(YEAR(CURDATE()), '-', MONTH(CURDATE()), '-01'))
                              AND op2.updated_at < DATE(CONCAT(YEAR(CURDATE()), '-', MONTH(CURDATE()) + 1, '-01'))
                        ) AS total
                    FROM order_payments op
                    JOIN receipts r ON op.receipt_number = r.receipt_number
                    WHERE op.voided = 1
                      AND op.updated_at IS NOT NULL
                      AND op.updated_at >= DATE(CONCAT(YEAR(CURDATE()), '-', MONTH(CURDATE()), '-01'))
                      AND op.updated_at < DATE(CONCAT(YEAR(CURDATE()), '-', MONTH(CURDATE()) + 1, '-01'))
                    ORDER BY op.updated_at;
                """
                cur.execute(query)
            
            rows = cur.fetchall()
            logger.info(f"Fetched {len(rows)} refund records (incremental: {last_sent_timestamp is not None})")
            return rows
    except Exception:
        get_fresh_connection().rollback()
        logger.error("Error in get_refunded_patients:\n" + traceback.format_exc())
        return None

def get_patients_by_location(last_sent_timestamp=None):
    """Get patients by location with incremental support - based on registrations, not visits"""
    try:
        with get_fresh_connection().cursor(DictCursor) as cur:
            if last_sent_timestamp:
                query = """
                    WITH total_patients AS (
                        SELECT COUNT(DISTINCT p2.patient_id) AS total
                        FROM patient p2
                        LEFT JOIN person_address pa2 ON pa2.person_id = p2.patient_id AND pa2.voided = 0
                        WHERE p2.voided = 0
                          AND p2.date_created > %s
                    )
                    SELECT
                        p.patient_id,
                        p.date_created AS time_stamp,
                        1 AS count,
                        tp.total,
                        COALESCE(pa.city_village, 'Unknown') AS location
                    FROM patient p
                    LEFT JOIN person_address pa ON pa.person_id = p.patient_id AND pa.voided = 0
                    CROSS JOIN total_patients tp
                    WHERE p.voided = 0
                      AND p.date_created > %s
                    ORDER BY p.date_created;
                """
                cur.execute(query, (last_sent_timestamp, last_sent_timestamp))
            else:
                query = """
                    WITH total_patients AS (
                        SELECT COUNT(DISTINCT p2.patient_id) AS total
                        FROM patient p2
                        LEFT JOIN person_address pa2 ON pa2.person_id = p2.patient_id AND pa2.voided = 0
                        WHERE p2.voided = 0
                          AND p2.date_created >= DATE(CONCAT(YEAR(CURDATE()), '-', MONTH(CURDATE()), '-01'))
                          AND p2.date_created < DATE(CONCAT(YEAR(CURDATE()), '-', MONTH(CURDATE()) + 1, '-01'))
                    )
                    SELECT
                        p.patient_id,
                        p.date_created AS time_stamp,
                        1 AS count,
                        tp.total,
                        COALESCE(pa.city_village, 'Unknown') AS location
                    FROM patient p
                    LEFT JOIN person_address pa ON pa.person_id = p.patient_id AND pa.voided = 0
                    CROSS JOIN total_patients tp
                    WHERE p.voided = 0
                      AND p.date_created >= DATE(CONCAT(YEAR(CURDATE()), '-', MONTH(CURDATE()), '-01'))
                      AND p.date_created < DATE(CONCAT(YEAR(CURDATE()), '-', MONTH(CURDATE()) + 1, '-01'))
                    ORDER BY p.date_created;
                """
                cur.execute(query)
            
            rows = cur.fetchall()
            logger.info(f"Fetched {len(rows)} location records based on REGISTRATIONS (incremental: {last_sent_timestamp is not None})")
            return rows
    except Exception:
        get_fresh_connection().rollback()
        logger.error("Error in get_patients_by_location:\n" + traceback.format_exc())
        return None

# --- Function to get all registered patients ---
def get_registered_patients(last_sent_timestamp=None):
    """Get registered patients with incremental support - ONLY CURRENT MONTH"""
    try:
        with get_fresh_connection().cursor(DictCursor) as cur:
            if last_sent_timestamp:
                # Incremental: get records after last sent timestamp, but still within current month
                query = """
                    SELECT
                        p.patient_id,
                        pn.given_name,
                        pn.family_name,
                        p.date_created,
                        per.birthdate,
                        per.gender
                    FROM patient p
                    JOIN person per ON p.patient_id = per.person_id
                    JOIN person_name pn ON per.person_id = pn.person_id
                    WHERE p.voided = 0
                    AND per.voided = 0
                    AND pn.voided = 0
                    AND p.date_created > %s
                    AND p.date_created >= DATE(CONCAT(YEAR(CURDATE()), '-', MONTH(CURDATE()), '-01'))
                    AND p.date_created < DATE(CONCAT(YEAR(CURDATE()), '-', MONTH(CURDATE()) + 1, '-01'))
                    ORDER BY p.date_created ASC;
                """
                cur.execute(query, (last_sent_timestamp,))
            else:
                # Full: get all registrations from current month only
                query = """
                    SELECT
                        p.patient_id,
                        pn.given_name,
                        pn.family_name,
                        p.date_created,
                        per.birthdate,
                        per.gender
                    FROM patient p
                    JOIN person per ON p.patient_id = per.person_id
                    JOIN person_name pn ON per.person_id = pn.person_id
                    WHERE p.voided = 0
                    AND per.voided = 0
                    AND pn.voided = 0
                    AND p.date_created >= DATE(CONCAT(YEAR(CURDATE()), '-', MONTH(CURDATE()), '-01'))
                    AND p.date_created < DATE(CONCAT(YEAR(CURDATE()), '-', MONTH(CURDATE()) + 1, '-01'))
                    ORDER BY p.date_created ASC;
                """
                cur.execute(query)
            
            rows = cur.fetchall()
            logger.info(f"Fetched {len(rows)} registered patient records from CURRENT MONTH (incremental: {last_sent_timestamp is not None})")
            return rows
    except Exception:
        logger.error("Error in get_registered_patients:\n" + traceback.format_exc())
        get_fresh_connection().rollback()
        return None
    
def get_patient_visits_current_month(last_sent_timestamp=None):
    """
    Latest updated visit per patient, current month.
    Uses updated_at for incremental detection so departure_time updates are pushed.
    Now includes ACTIVE patients (those without departure_time yet).
    """
    try:
        # Connect to the 'edim' database
        with get_fresh_connection(db_key='edim').cursor(DictCursor) as cur:
            if last_sent_timestamp:
                query = """
                    SELECT v.*
                    FROM edim_visits v
                    JOIN (
                        SELECT edim_patient_id, MAX(updated_at) AS latest_update
                        FROM edim_visits
                        WHERE arrival_time >= DATE(CONCAT(YEAR(CURDATE()), '-', MONTH(CURDATE()), '-01'))
                          AND arrival_time < DATE(CONCAT(YEAR(CURDATE()), '-', MONTH(CURDATE()) + 1, '-01'))
                          AND updated_at IS NOT NULL
                          AND updated_at > %s
                        GROUP BY edim_patient_id
                    ) lv
                      ON v.edim_patient_id = lv.edim_patient_id
                     AND v.updated_at = lv.latest_update
                    ORDER BY v.updated_at;
                """
                cur.execute(query, (last_sent_timestamp,))
            else:
                query = """
                    SELECT v.*
                    FROM edim_visits v
                    JOIN (
                        SELECT edim_patient_id, MAX(updated_at) AS latest_update
                        FROM edim_visits
                        WHERE arrival_time >= DATE(CONCAT(YEAR(CURDATE()), '-', MONTH(CURDATE()), '-01'))
                          AND arrival_time < DATE(CONCAT(YEAR(CURDATE()), '-', MONTH(CURDATE()) + 1, '-01'))
                          AND updated_at IS NOT NULL
                        GROUP BY edim_patient_id
                    ) lv
                      ON v.edim_patient_id = lv.edim_patient_id
                     AND v.updated_at = lv.latest_update
                    ORDER BY v.updated_at;
                """
                cur.execute(query)

            rows = cur.fetchall()
            
            # Count active vs completed visits
            active_count = sum(1 for r in rows if r.get('departure_time') is None)
            completed_count = len(rows) - active_count
            
            logger.info(
                f"Fetched {len(rows)} patient visit records from EDIM DB "
                f"(Active: {active_count}, Completed: {completed_count}, Current month)"
            )
            return rows

    except Exception:
        logger.error("Error in get_patient_visits_current_month:\n" + traceback.format_exc())
        return []

# --- Payload composition and server push ---
def compose_payload():
    """Compose payload with incremental data fetching"""
    def format_ts(value):
        if isinstance(value, datetime):
            return value.strftime("%Y-%m-%d %H:%M:%S")
        elif isinstance(value, date):
            # Convert to datetime to avoid defaulting to 00:00:00
            return datetime.combine(value, datetime.min.time()).strftime("%Y-%m-%d %H:%M:%S")
        else:
            try:
                parsed = datetime.fromisoformat(value)
                return parsed.strftime("%Y-%m-%d %H:%M:%S")
            except Exception:
                return str(value)

    # Get last sent timestamps for each data type
    age_last_sent = get_last_sent_timestamp('age_categories')
    gender_last_sent = get_last_sent_timestamp('gender_counts')
    location_last_sent = get_last_sent_timestamp('location_counts')
    refund_last_sent = get_last_sent_timestamp('refunded_patients')
    registered_last_sent = get_last_sent_timestamp('registered_patients')

    # Fetch only new data based on last sent timestamps
    age_categories = get_patient_categories(age_last_sent) or []
    gender_counts = get_patients_by_gender(gender_last_sent) or []
    location_counts = get_patients_by_location(location_last_sent) or []
    refunded_patients = get_refunded_patients(refund_last_sent) or []
    registered_patients = get_registered_patients(registered_last_sent) or []

    visits_last_sent = get_last_sent_timestamp('patient_visits')
    patient_visits = get_patient_visits_current_month(visits_last_sent) or []

    
    age_list = [
        {
            "patient_id": item["patient_id"],
            "category": item["category"],
            "total": 1,
            "time_stamp": format_ts(item["time_stamp"])
        }
        for item in age_categories
    ]
    gender_list = [
        {
            "patient_id": item["patient_id"],
            "gender": item["gender"],
            "total": item["total"],
            "time_stamp": format_ts(item["time_stamp"]),
        }
        for item in gender_counts
    ]
    location_list = [
        {
            "patient_id": item["patient_id"],
            "location": item["location"],
            "time_stamp": format_ts(item["time_stamp"]),
            "count": 1,
            "total": item["total"]
        }
        for item in location_counts
    ]
    refund_list = [
        {
            "patient_id": item["patient_id"],
            "time_stamp": format_ts(item["refund_timestamp"]),
            "count": item["count"],
            "total": item["total"]
        }
        for item in refunded_patients
    ]
    registered_list = [
        {
            "patient_id": item["patient_id"],
            "given_name": item["given_name"],
            "family_name": item["family_name"],
            "date_created": format_ts(item["date_created"]),
            "birthdate": format_ts(item["birthdate"]),
            "gender": item["gender"]
        }
        for item in registered_patients
    ]
    visit_list = [
        {
            "patient_id": v["edim_patient_id"],
            "arrival_time": format_ts(v["arrival_time"]),
            "departure_time": format_ts(v["departure_time"]) if v["departure_time"] else None,
            "visit_date": format_ts(v["visit_date"]),
            "updated_at": format_ts(v["updated_at"]) if v.get("updated_at") else None
        }
        for v in patient_visits
    ]

    payload = {
        "age_categories": age_list,
        "gender_counts": gender_list,
        "location_counts": location_list,
        "refunded_patients": refund_list,
        "registered_patients": registered_list,
        "patient_visits": visit_list
    }

    # Return payload along with metadata for state tracking
    return {
        'payload': payload,
        'metadata': {
            'age_categories': {'records': age_categories, 'count': len(age_list)},
            'gender_counts': {'records': gender_counts, 'count': len(gender_list)},
            'location_counts': {'records': location_counts, 'count': len(location_list)},
            'refunded_patients': {'records': refunded_patients, 'count': len(refund_list)},
            'registered_patients': {'records': registered_patients, 'count': len(registered_list)},
            'patient_visits': {'records': patient_visits, 'count': len(visit_list)}
        }
    }


import os
import yaml
import gzip
import json
import time
import requests
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

total_bytes_sent = 0
total_bytes_sent_compressed = 0

def push_payload_to_virtual_server():
    global total_bytes_sent, total_bytes_sent_compressed
    
    try:
        config = load_config(os.getenv('CONFIG_FILE', 'config/virtual_config.yaml'))
    except Exception as e:
        logger.error(f"Failed to load config file: {e}")
        return

    virtual_server = config.get('virtual_server', {})
    scheme = (virtual_server.get('scheme') or 'http').strip().lower()
    host = virtual_server.get('host')
    port = virtual_server.get('port')
    remote_path = virtual_server.get('remote_path', '/wandikweza/save_patient_data/')
    timeout_seconds = virtual_server.get('timeout_seconds', 120)
    api_key = (virtual_server.get('api_key') or '').strip()
    api_key_header = virtual_server.get('api_key_header', 'X-API-Key')
    ca_cert_path = (virtual_server.get('ca_cert_path') or '').strip()
    verify_ssl = ca_cert_path or as_bool(virtual_server.get('verify_ssl'), True)

    if not host or not port:
        logger.error("Virtual server host or port not configured.")
        return

    if scheme not in {'http', 'https'}:
        logger.error("Invalid virtual server scheme '%s'. Use 'http' or 'https'.", scheme)
        return

    try:
        timeout_seconds = int(timeout_seconds)
    except (TypeError, ValueError):
        logger.warning("Invalid timeout_seconds value '%s'. Falling back to 120 seconds.", timeout_seconds)
        timeout_seconds = 120

    base_url = f"{scheme}://{host}:{port}"
    url = urljoin(f"{base_url}/", remote_path.lstrip('/'))

    if scheme == 'https' and verify_ssl is False:
        logger.warning("HTTPS is enabled for virtual server push, but SSL certificate verification is disabled.")

    while True:
        try:
            payload_data = compose_payload()
            payload = payload_data['payload']
            metadata = payload_data['metadata']
            
            total_new_records = sum(meta['count'] for meta in metadata.values())

            if total_new_records == 0:
                logger.info("No new records to push. Skipping this cycle.")
                time.sleep(120)
                continue

            logger.info(
                f"Preparing to push {total_new_records} new records | "
                f"Age: {metadata['age_categories']['count']} | "
                f"Gender: {metadata['gender_counts']['count']} | "
                f"Location: {metadata['location_counts']['count']} | "
                f"Refund: {metadata['refunded_patients']['count']} | "
                f"Registered: {metadata['registered_patients']['count']} | "
                f"Visits: {metadata['patient_visits']['count']}"
            )

            payload_bytes = json.dumps(payload).encode('utf-8')
            total_bytes_sent += len(payload_bytes)

            compressed_bytes = gzip.compress(payload_bytes)
            total_bytes_sent_compressed += len(compressed_bytes)

            headers = {
                "Content-Type": "application/json",
                "Content-Encoding": "gzip"
            }

            if api_key:
                if api_key_header.lower() == 'authorization':
                    headers[api_key_header] = f"Bearer {api_key}"
                else:
                    headers[api_key_header] = api_key

            response = requests.post(
                url,
                data=compressed_bytes,
                headers=headers,
                timeout=timeout_seconds,
                verify=verify_ssl
            )
            response.raise_for_status()

            success_count = 0
            for data_type, meta in metadata.items():
                if meta['count'] == 0:
                    continue

                if data_type == 'refunded_patients':
                    timestamp_field = 'refund_timestamp'
                elif data_type == 'registered_patients':
                    timestamp_field = 'date_created'
                elif data_type == 'patient_visits':
                    timestamp_field = 'updated_at'
                else:
                    timestamp_field = 'time_stamp'

                max_timestamp = get_max_timestamp_from_records(
                    meta['records'],
                    timestamp_field
                )

                if max_timestamp and update_last_sent_timestamp(data_type, max_timestamp):
                    success_count += 1
                else:
                    logger.warning(
                        f"Failed to update timestamp for {data_type} "
                        f"(field={timestamp_field}, max={max_timestamp})"
                    )

            logger.info(
                f"Successfully pushed {total_new_records} new records | "
                f"Uncompressed: {len(payload_bytes)/1024:.2f} KB | "
                f"Compressed: {len(compressed_bytes)/1024:.2f} KB | "
                f"Updated {success_count}/{len([m for m in metadata.values() if m['count'] > 0])} timestamps | "
                f"Total sent uncompressed: {total_bytes_sent/1024/1024:.2f} MB | "
                f"Total sent compressed: {total_bytes_sent_compressed/1024/1024:.2f} MB | "
                f"Time: {datetime.now(local_tz)}"
            )

        except requests.Timeout:
            logger.error(
                "Timeout while pushing payload to virtual server. "
                "Will retry with same data in 120 seconds..."
            )
        except requests.RequestException as e:
            logger.error(
                f"Failed to push payload to virtual server: {e}. "
                "Will retry with same data in 120 seconds..."
            )
        except Exception:
            logger.error("Unexpected error in push cycle")
            logger.error(traceback.format_exc())

        time.sleep(120)

# ---- Flask app setup with background payload pushing ----

if __name__ == "__main__":
    import os
    import threading
    from app import create_app

    # Only start the thread once (to avoid double start with Flask reloader)
    if os.environ.get("WERKZEUG_RUN_MAIN") == "true":
        push_thread = threading.Thread(target=push_payload_to_virtual_server, daemon=True)
        push_thread.start()
