
import traceback
import requests
import yaml
import os
import time
import threading
from datetime import date
from extensions.extensions import logger
from pymysql.cursors import DictCursor
from datetime import datetime
import pytz
import pymysql
import gzip
import json
import time
import logging
from datetime import datetime
import pymysql

def get_fresh_connection():
    """Get a fresh database connection using config file settings"""
    config_file_path = os.getenv('CONFIG_FILE', 'config/dev_config.yaml')
    
    try:
        with open(config_file_path, 'r') as f:
            config = yaml.safe_load(f)
        
        db_config = config.get('database')
        if not db_config:
            raise ValueError("Database configuration not found in config file")
        
        # Validate required config fields
        required_fields = ['host', 'username', 'password', 'database_name', 'port']
        missing_fields = [field for field in required_fields if not db_config.get(field)]
        if missing_fields:
            raise ValueError(f"Missing required database config fields: {missing_fields}")
        
        return pymysql.connect(
            host=db_config['host'],
            user=db_config['username'],
            password=db_config['password'],
            database=db_config['database_name'],
            port=db_config['port'],
            cursorclass=DictCursor
        )
    except FileNotFoundError:
        logger.error(f"Config file not found: {config_file_path}")
        raise Exception(f"Database config file not found: {config_file_path}")
    except Exception as e:
        logger.error(f"Failed to load database config from {config_file_path}: {e}")
        raise Exception(f"Database configuration error: {e}")

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
                'registered_patients': {'last_sent_timestamp': None, 'updated_at': None}
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
                            o.patient_id AS patient_id,
                            o.order_date AS time_stamp
                        FROM order_entries o
                        JOIN person per ON o.patient_id = per.person_id
                        WHERE o.voided = 0
                        AND per.voided = 0
                        AND TIMESTAMPDIFF(YEAR, per.birthdate, o.order_date) BETWEEN 10 AND 19
                        AND o.order_date > %s
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
                            o.patient_id AS patient_id,
                            o.order_date AS time_stamp
                        FROM order_entries o
                        JOIN person per ON o.patient_id = per.person_id
                        WHERE o.voided = 0
                        AND per.voided = 0
                        AND TIMESTAMPDIFF(YEAR, per.birthdate, o.order_date) BETWEEN 10 AND 19
                        AND o.order_date >= DATE(CONCAT(YEAR(CURDATE()), '-', MONTH(CURDATE()), '-01'))
                        AND o.order_date < DATE(CONCAT(YEAR(CURDATE()), '-', MONTH(CURDATE()) + 1, '-01'))
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
            # logger.info(f"Fetched {len(rows)} age category records (incremental: {last_sent_timestamp is not None})")
            return rows

    except Exception:
        logger.error("Error in get_patient_categories:\n" + traceback.format_exc())
        return None

def get_patients_by_gender(last_sent_timestamp=None):
    """
    Get patients by gender based on VISITS (order_entries),
    restricted to current month, with incremental support.
    """
    conn = get_fresh_connection()
    try:
        with conn.cursor(DictCursor) as cur:
            monthly_total_query = """
                SELECT COUNT(*) AS total_month_visits
                FROM order_entries o
                WHERE o.order_date >= DATE_SUB(CURDATE(), INTERVAL DAYOFMONTH(CURDATE()) - 1 DAY)
                  AND o.order_date < DATE_ADD(DATE_SUB(CURDATE(), INTERVAL DAYOFMONTH(CURDATE()) - 1 DAY), INTERVAL 1 MONTH);
            """
            cur.execute(monthly_total_query)
            monthly_total_row = cur.fetchone() or {}
            monthly_total_visits = monthly_total_row.get('total_month_visits', 0)

            if last_sent_timestamp:
                query = """
                    SELECT
                        o.patient_id,
                        COALESCE(LOWER(per.gender), 'unknown') AS gender,
                        o.order_date AS time_stamp
                    FROM order_entries o
                    LEFT JOIN person per ON o.patient_id = per.person_id AND per.voided = 0
                    WHERE o.order_date > %s
                      AND o.order_date >= DATE_SUB(CURDATE(), INTERVAL DAYOFMONTH(CURDATE()) - 1 DAY)
                      AND o.order_date < DATE_ADD(DATE_SUB(CURDATE(), INTERVAL DAYOFMONTH(CURDATE()) - 1 DAY), INTERVAL 1 MONTH)
                    ORDER BY o.order_date;
                """
                cur.execute(query, (last_sent_timestamp,))
            else:
                query = """
                    SELECT
                        o.patient_id,
                        COALESCE(LOWER(per.gender), 'unknown') AS gender,
                        o.order_date AS time_stamp
                    FROM order_entries o
                    LEFT JOIN person per ON o.patient_id = per.person_id AND per.voided = 0
                    WHERE o.order_date >= DATE_SUB(CURDATE(), INTERVAL DAYOFMONTH(CURDATE()) - 1 DAY)
                      AND o.order_date < DATE_ADD(DATE_SUB(CURDATE(), INTERVAL DAYOFMONTH(CURDATE()) - 1 DAY), INTERVAL 1 MONTH)
                    ORDER BY o.order_date;
                """
                cur.execute(query)

            rows = cur.fetchall()
            logger.info(
                "Gender monthly fetched total=%s | fetched this cycle=%s | last_sent_timestamp=%s",
                monthly_total_visits,
                len(rows),
                last_sent_timestamp
            )

            # Calculate gender totals (visit-based)
            gender_totals = {}
            for row in rows:
                gender = row['gender']
                gender_totals[gender] = gender_totals.get(gender, 0) + 1

            # Attach total visits per gender to each row
            for row in rows:
                row['total'] = gender_totals.get(row['gender'], 0)

            return rows

    except Exception:
        logger.error("Error in get_patients_by_gender:\n" + traceback.format_exc())
        return None
    finally:
        conn.close() 

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
            #logger.info(f"Fetched {len(rows)} refund records (incremental: {last_sent_timestamp is not None})")
            return rows
    except Exception:
        get_fresh_connection().rollback()
        logger.error("Error in get_refunded_patients:\n" + traceback.format_exc())
        return None

def get_patients_by_location(last_sent_timestamp=None):
    """Get patients by location - based on VISITS (order_entries)"""
    try:
        with get_fresh_connection().cursor(DictCursor) as cur:

            query = """
                WITH total_patients AS (
                    SELECT COUNT(DISTINCT o.patient_id) AS total
                    FROM order_entries o
                    LEFT JOIN person_address pa2 
                           ON pa2.person_id = o.patient_id AND pa2.voided = 0
                    WHERE o.order_date >= DATE(CONCAT(YEAR(CURDATE()), '-', MONTH(CURDATE()), '-01'))
                      AND o.order_date < DATE(CONCAT(YEAR(CURDATE()), '-', MONTH(CURDATE()) + 1, '-01'))
                )
                SELECT
                    o.patient_id,
                    MIN(o.order_date) AS first_visit_time,
                    COUNT(*) AS visit_count,
                    tp.total AS total_patients_this_month,
                    COALESCE(pa.city_village, 'Unknown') AS location
                FROM order_entries o
                LEFT JOIN person_address pa 
                       ON pa.person_id = o.patient_id AND pa.voided = 0
                CROSS JOIN total_patients tp
                WHERE o.order_date >= DATE(CONCAT(YEAR(CURDATE()), '-', MONTH(CURDATE()), '-01'))
                  AND o.order_date < DATE(CONCAT(YEAR(CURDATE()), '-', MONTH(CURDATE()) + 1, '-01'))
                GROUP BY o.patient_id, pa.city_village
                ORDER BY first_visit_time;
            """

            cur.execute(query)
            rows = cur.fetchall()
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
            #logger.info(f"Fetched {len(rows)} registered patient records from CURRENT MONTH (incremental: {last_sent_timestamp is not None})")
            return rows
    except Exception:
        logger.error("Error in get_registered_patients:\n" + traceback.format_exc())
        get_fresh_connection().rollback()
        return None

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

    payload = {
        "age_categories": age_list,
        "gender_counts": gender_list,
        "location_counts": location_list,
        "refunded_patients": refund_list,
        "registered_patients": registered_list
    }

    total_payload_records = (
        len(age_list) +
        len(gender_list) +
        len(location_list) +
        len(refund_list) +
        len(registered_list)
    )
    logger.info(
        "Payload records included | total=%s | age_categories=%s | gender_counts=%s | "
        "location_counts=%s | refunded_patients=%s | registered_patients=%s",
        total_payload_records,
        len(age_list),
        len(gender_list),
        len(location_list),
        len(refund_list),
        len(registered_list),
    )
    
    # Return payload along with metadata for state tracking
    return {
        'payload': payload,
        'metadata': {
            'age_categories': {'records': age_categories, 'count': len(age_list)},
            'gender_counts': {'records': gender_counts, 'count': len(gender_list)},
            'location_counts': {'records': location_counts, 'count': len(location_list)},
            'refunded_patients': {'records': refunded_patients, 'count': len(refund_list)},
            'registered_patients': {'records': registered_patients, 'count': len(registered_list)}
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
    config_file_path = os.getenv('CONFIG_FILE', 'config/virtual_config.yaml')
    
    try:
        with open(config_file_path, 'r') as f:
            config = yaml.safe_load(f)
    except Exception as e:
        logger.error(f"Failed to load config file: {e}")
        return

    virtual_server = config.get('virtual_server', {})
    host = virtual_server.get('host')
    port = virtual_server.get('port')

    if not host or not port:
        logger.error("Virtual server host or port not configured.")
        return

    url = f"http://{host}:{port}/wandikweza/save_patient_data/"

    while True:
        try:
            # Compose payload with incremental data
            payload_data = compose_payload()
            payload = payload_data['payload']
            metadata = payload_data['metadata']
            
            # Check if there's any new data to send
            total_new_records = sum(meta['count'] for meta in metadata.values())
            
            if total_new_records == 0:
                logger.info("No new records to push. Skipping this cycle.")
                time.sleep(120)
                continue

            logger.info(f"Preparing to push {total_new_records} new records | "
                       f"Age: {metadata['age_categories']['count']} | "
                       f"Gender: {metadata['gender_counts']['count']} | "
                       f"Location: {metadata['location_counts']['count']} | "
                       f"Refund: {metadata['refunded_patients']['count']} | "
                       f"Registered: {metadata['registered_patients']['count']}")

            # Convert payload to bytes
            payload_bytes = json.dumps(payload).encode('utf-8')
            total_bytes_sent += len(payload_bytes)

            # Compress payload
            compressed_bytes = gzip.compress(payload_bytes)
            total_bytes_sent_compressed += len(compressed_bytes)

            headers = {
                "Content-Type": "application/json",
                "Content-Encoding": "gzip"
            }

            # Send the payload
            response = requests.post(url, data=compressed_bytes, headers=headers, timeout=120)
            response.raise_for_status()
            
            # If successful, update the last sent timestamps
            success_count = 0
            for data_type, meta in metadata.items():
                if meta['count'] > 0:  # Only update if there were records
                    # Map the correct timestamp field for each data type
                    if data_type == 'refunded_patients':
                        timestamp_field = 'refund_timestamp'
                    elif data_type == 'registered_patients':
                        timestamp_field = 'date_created'
                    else:
                        timestamp_field = 'time_stamp'
                    
                    max_timestamp = get_max_timestamp_from_records(meta['records'], timestamp_field)
                    if max_timestamp and update_last_sent_timestamp(data_type, max_timestamp):
                        success_count += 1
                    else:
                        logger.warning(f"Failed to update timestamp for {data_type} - max_timestamp: {max_timestamp}")

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
            logger.error("Timeout while pushing payload to virtual server. Will retry with same data in 120 seconds...")
        except requests.RequestException as e:
            logger.error(f"Failed to push payload to virtual server: {e}. Will retry with same data in 120 seconds...")
        except Exception as e:
            logger.error(f"Unexpected error in push cycle: {e}")
            logger.error(traceback.format_exc())

        # Wait before next cycle
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
