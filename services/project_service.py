from extensions.extensions import conn, logger  # conn must be a mysql.connector connection
import mysql.connector
from mysql.connector import Error

def get_project_data():
    """
    Fetches active and completed projects with their start/end dates and number of active employees.
    """
    query = """
        SELECT 
            p.short_name AS project_name,
            p.created_at AS project_start_date,
            p.completed_at AS project_end_date,
            COUNT(pt.employee_id) AS project_workforce,
            p.is_active AS is_project_active
        FROM projects p
        LEFT JOIN project_teams pt 
            ON pt.project_id = p.project_id AND pt.voided = FALSE
        GROUP BY p.project_id, p.short_name, p.created_at, p.completed_at, p.is_active
        ORDER BY p.created_at DESC;
    """

    try:
        cursor = conn.cursor()
        cursor.execute(query)
        results = cursor.fetchall()
        cursor.close()

        return [
            {
                "project_name": row[0],
                "project_start_date": row[1],
                "project_end_date": row[2],
                "project_workforce": row[3],
                "project_status": row[4]
            }
            for row in results
        ]
    except Error as e:
        conn.rollback()
        logger.error(f"Database error: {e}")
        return None