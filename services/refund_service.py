from extensions.extensions import conn, logger
from mysql.connector import Error
import traceback

def get_total_refund_patients():
    """
    Returns the total number of distinct patients who received refunds.
    """
    query = """
        SELECT SUM(refunded_patients) AS total_refund_patients
        FROM (
            SELECT COUNT(DISTINCT r.patient_id) AS refunded_patients
            FROM (
                SELECT receipt_number
                FROM order_payments
                WHERE voided = 1
            ) op
            JOIN receipts r ON op.receipt_number = r.receipt_number
        ) AS virtual_table;
    """
    try:
        with conn.cursor(dictionary=True) as cur:
            cur.execute(query)
            result = cur.fetchone()
            return result["total_refund_patients"]
    except Error as e:
        conn.rollback()
        logger.error("Error in get_total_refund_patients:\n" + traceback.format_exc())
        return None