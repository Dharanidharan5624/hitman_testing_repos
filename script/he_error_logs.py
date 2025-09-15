from datetime import datetime
import traceback
import os
import sys
sys.path.append(os.path.abspath(os.path.dirname(__file__)))
from HE_Database_Connect import get_connection

try:
    from HE_Database_Connect import get_connection
except ImportError as e:
    print(f"[ERROR] Cannot import get_connection: {e}")
    sys.exit(1)

def log_error_to_db(file_name, error_description=None, created_by=None, env="dev"):
    try:
        if error_description is None:
            error_description = traceback.format_exc()
        if not created_by:
            created_by = os.getenv("USERNAME", "system")

        conn = get_connection(env=env)
        cursor = conn.cursor()

        cursor.execute("""
            INSERT INTO he_error_logs (file_name, error_description, created_at, created_by)
            VALUES (%s, %s, %s, %s)
        """, (file_name, error_description, datetime.now().strftime('%Y-%m-%d %H:%M:%S'), created_by))

        conn.commit()
        print(f"[INFO] Error logged from {file_name} by {created_by}")

    except Exception as db_err:
        print(f"[ERROR] Failed to log error: {db_err}")
        print(traceback.format_exc())
    finally:
        try:
            if cursor: cursor.close()
            if conn: conn.close()
        except: pass


