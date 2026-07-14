
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

def get_billing_connection():
    """Get a fresh connection to the reporting database used by Superset."""
    config_file_path = os.getenv('CONFIG_FILE', 'config/virtual_config.yaml')

    try:
        with open(config_file_path, 'r') as f:
            config = yaml.safe_load(f) or {}

        db_config = config.get('billing_import')
        if not db_config:
            db_config = (config.get('database') or {}).get('billing_import')
        if not db_config:
            raise ValueError("billing_import configuration not found")

        return pymysql.connect(
            host=db_config['host'],
            user=db_config['username'],
            password=db_config['password'],
            database=db_config['database_name'],
            port=db_config.get('port', 3306),
            cursorclass=DictCursor,
            autocommit=False
        )
    except FileNotFoundError:
        logger.error(f"Config file not found: {config_file_path}")
        raise Exception(f"Billing database config file not found: {config_file_path}")
    except Exception as e:
        logger.error(f"Failed to load billing database config from {config_file_path}: {e}")
        raise Exception(f"Billing database configuration error: {e}")

def save_patient_payload(payload):
    """Persist pushed patient data into reporting tables for Superset."""
    conn = get_billing_connection()
    now = datetime.now()

    try:
        with conn.cursor() as cur:
            # NOTE: Column layouts below MUST match the tables Superset reads.
            # These CREATE statements only run on a fresh reporting DB; existing
            # tables are left untouched by CREATE TABLE IF NOT EXISTS.
            cur.execute("""
                CREATE TABLE IF NOT EXISTS patient_age_categories (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    patient_id INT NOT NULL,
                    category VARCHAR(50) NOT NULL,
                    time_stamp DATETIME NOT NULL,
                    total INT NOT NULL DEFAULT 1,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    UNIQUE KEY uq_patient_age_entry (category, patient_id, time_stamp)
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS patient_gender_counts (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    patient_id INT NOT NULL,
                    gender VARCHAR(10) NOT NULL,
                    time_stamp DATETIME NOT NULL,
                    total INT DEFAULT 0,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    UNIQUE KEY uq_gender_count (patient_id, gender, time_stamp)
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS patient_location_counts (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    patient_id INT NOT NULL,
                    location VARCHAR(100) NOT NULL,
                    time_stamp DATETIME NOT NULL,
                    count INT NOT NULL DEFAULT 1,
                    total INT NOT NULL,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    UNIQUE KEY uq_patient_location (patient_id, location)
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS patient_refund_count (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    patient_id INT NOT NULL,
                    refund_timestamp DATETIME NOT NULL,
                    count INT NOT NULL DEFAULT 1,
                    total INT NOT NULL,
                    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    UNIQUE KEY unique_patient_refund (patient_id, refund_timestamp)
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS registered_patients (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    patient_id INT NOT NULL,
                    given_name VARCHAR(100) NULL,
                    family_name VARCHAR(100) NULL,
                    date_created DATETIME NOT NULL,
                    birthdate DATETIME NULL,
                    gender VARCHAR(20) NULL,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    UNIQUE KEY uq_registered_patient (patient_id, date_created)
                )
            """)

            counts = {
                'age_categories': 0,
                'gender_counts': 0,
                'location_counts': 0,
                'refunded_patients': 0,
                'registered_patients': 0
            }

            for item in payload.get('age_categories', []):
                cur.execute("""
                    INSERT INTO patient_age_categories
                        (patient_id, category, time_stamp, total, created_at, updated_at)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        total = VALUES(total), updated_at = VALUES(updated_at)
                """, (
                    item.get('patient_id'), item.get('category'), item.get('time_stamp'),
                    item.get('total', 1), now, now
                ))
                counts['age_categories'] += 1

            for item in payload.get('gender_counts', []):
                cur.execute("""
                    INSERT INTO patient_gender_counts
                        (patient_id, gender, time_stamp, total, created_at, updated_at)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        total = VALUES(total), updated_at = VALUES(updated_at)
                """, (
                    item.get('patient_id'), item.get('gender', 'unknown'), item.get('time_stamp'),
                    item.get('total', 1), now, now
                ))
                counts['gender_counts'] += 1

            for item in payload.get('location_counts', []):
                cur.execute("""
                    INSERT INTO patient_location_counts
                        (patient_id, location, time_stamp, count, total, created_at, updated_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        time_stamp = VALUES(time_stamp), count = VALUES(count),
                        total = VALUES(total), updated_at = VALUES(updated_at)
                """, (
                    item.get('patient_id'), item.get('location', 'Unknown'), item.get('time_stamp'),
                    item.get('count', 1), item.get('total', 1), now, now
                ))
                counts['location_counts'] += 1

            for item in payload.get('refunded_patients', []):
                cur.execute("""
                    INSERT INTO patient_refund_count
                        (patient_id, refund_timestamp, count, total, created_at, updated_at)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        count = VALUES(count), total = VALUES(total), updated_at = VALUES(updated_at)
                """, (
                    item.get('patient_id'), item.get('time_stamp'),
                    item.get('count', 1), item.get('total', 1), now, now
                ))
                counts['refunded_patients'] += 1

            for item in payload.get('registered_patients', []):
                cur.execute("""
                    INSERT INTO registered_patients
                        (patient_id, given_name, family_name, date_created, birthdate, gender, created_at, updated_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        given_name = VALUES(given_name),
                        family_name = VALUES(family_name),
                        birthdate = VALUES(birthdate),
                        gender = VALUES(gender),
                        updated_at = VALUES(updated_at)
                """, (
                    item.get('patient_id'), item.get('given_name'), item.get('family_name'),
                    item.get('date_created'), item.get('birthdate'), item.get('gender'), now, now
                ))
                counts['registered_patients'] += 1

        conn.commit()
        return counts
    except Exception:
        conn.rollback()
        logger.error("Error saving pushed patient payload:\n" + traceback.format_exc())
        raise
    finally:
        conn.close()

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
                            CASE
                                WHEN TIMESTAMPDIFF(YEAR, per.birthdate, p.date_created) BETWEEN 10 AND 14
                                    THEN 'Early Adolescents'
                                WHEN TIMESTAMPDIFF(YEAR, per.birthdate, p.date_created) BETWEEN 15 AND 19
                                    THEN 'Late Adolescents'
                            END AS category,
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
                        AND p.date_created < (DATE(CONCAT(YEAR(CURDATE()), '-', MONTH(CURDATE()), '-01')) + INTERVAL 1 MONTH)

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
                        AND o.order_date < (DATE(CONCAT(YEAR(CURDATE()), '-', MONTH(CURDATE()), '-01')) + INTERVAL 1 MONTH)

                        UNION ALL

                        SELECT
                            CASE
                                WHEN TIMESTAMPDIFF(YEAR, per.birthdate, p.date_created) BETWEEN 10 AND 14
                                    THEN 'Early Adolescents'
                                WHEN TIMESTAMPDIFF(YEAR, per.birthdate, p.date_created) BETWEEN 15 AND 19
                                    THEN 'Late Adolescents'
                            END AS category,
                            p.patient_id AS patient_id,
                            p.date_created AS time_stamp
                        FROM patient p
                        JOIN person per ON p.patient_id = per.person_id
                        WHERE p.voided = 0
                        AND per.voided = 0
                        AND TIMESTAMPDIFF(YEAR, per.birthdate, p.date_created) BETWEEN 10 AND 19
                        AND p.date_created >= DATE(CONCAT(YEAR(CURDATE()), '-', MONTH(CURDATE()), '-01'))
                        AND p.date_created < (DATE(CONCAT(YEAR(CURDATE()), '-', MONTH(CURDATE()), '-01')) + INTERVAL 1 MONTH)
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
            if last_sent_timestamp:
                query = """
                    SELECT
                        o.patient_id,
                        COALESCE(LOWER(per.gender), 'unknown') AS gender,
                        o.order_date AS time_stamp
                    FROM order_entries o
                    JOIN patient p ON o.patient_id = p.patient_id
                    JOIN person per ON p.patient_id = per.person_id
                    WHERE o.voided = 0
                      AND p.voided = 0
                      AND per.voided = 0
                      AND o.order_date > %s
                      AND o.order_date >= DATE_FORMAT(CURDATE(), '%%Y-%%m-01')
                      AND o.order_date < DATE_ADD(DATE_FORMAT(CURDATE(), '%%Y-%%m-01'), INTERVAL 1 MONTH)
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
                    JOIN patient p ON o.patient_id = p.patient_id
                    JOIN person per ON p.patient_id = per.person_id
                    WHERE o.voided = 0
                      AND p.voided = 0
                      AND per.voided = 0
                      AND o.order_date >= DATE_FORMAT(CURDATE(), '%Y-%m-01')
                      AND o.order_date < DATE_ADD(DATE_FORMAT(CURDATE(), '%Y-%m-01'), INTERVAL 1 MONTH)
                    ORDER BY o.order_date;
                """
                cur.execute(query)

            rows = cur.fetchall()

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
                              AND op2.updated_at < (DATE(CONCAT(YEAR(CURDATE()), '-', MONTH(CURDATE()), '-01')) + INTERVAL 1 MONTH)
                        ) AS total
                    FROM order_payments op
                    JOIN receipts r ON op.receipt_number = r.receipt_number
                    WHERE op.voided = 1
                      AND op.updated_at IS NOT NULL
                      AND op.updated_at >= DATE(CONCAT(YEAR(CURDATE()), '-', MONTH(CURDATE()), '-01'))
                      AND op.updated_at < (DATE(CONCAT(YEAR(CURDATE()), '-', MONTH(CURDATE()), '-01')) + INTERVAL 1 MONTH)
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
                          AND p2.date_created < (DATE(CONCAT(YEAR(CURDATE()), '-', MONTH(CURDATE()), '-01')) + INTERVAL 1 MONTH)
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
                      AND p.date_created < (DATE(CONCAT(YEAR(CURDATE()), '-', MONTH(CURDATE()), '-01')) + INTERVAL 1 MONTH)
                    ORDER BY p.date_created;
                """
                cur.execute(query)
            
            rows = cur.fetchall()
            #logger.info(f"Fetched {len(rows)} location records based on REGISTRATIONS (incremental: {last_sent_timestamp is not None})")
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
                    AND p.date_created < (DATE(CONCAT(YEAR(CURDATE()), '-', MONTH(CURDATE()), '-01')) + INTERVAL 1 MONTH)
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
                    AND p.date_created < (DATE(CONCAT(YEAR(CURDATE()), '-', MONTH(CURDATE()), '-01')) + INTERVAL 1 MONTH)
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
