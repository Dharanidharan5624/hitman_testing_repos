from apscheduler.schedulers.blocking import BlockingScheduler
from datetime import datetime
import subprocess
import os
import mysql.connector
from win10toast import ToastNotifier
import traceback
import sys

# === Toast Notification ===
toaster = ToastNotifier()

# === DB Config ===
DB_CONFIG = {
    'host': 'localhost',
    'user': 'Hitman',
    'password': 'Hitman@123',
    'database': 'hitman_edge_dev'
}

# ✅ Script folder is current script’s folder
SCRIPT_FOLDER = os.path.dirname(os.path.abspath(__file__))

# === Read CLI Arguments ===
if len(sys.argv) != 6:
    print(" Error: Expected 5 arguments")
    print("Usage: python scheduler.py <job_name> <start_time> <frequency> <schedule_type> <created_by>")
    sys.exit(1)

job_name = sys.argv[1]
start_time = sys.argv[2]
schedule_frequency = sys.argv[3].lower()
schedule_type = sys.argv[4].title()
created_by = int(sys.argv[5])

print(f"✅ Job Name: {job_name}")
print(f"✅ Start Time: {start_time}")
print(f"✅ Frequency: {schedule_frequency}")
print(f"✅ Schedule Type: {schedule_type}")
print(f"✅ Created By: {created_by}")

# === Notification Helper ===
def show_notification(title, message):
    try:
        print(f"[NOTIFY] {title}: {message}")
        toaster.show_toast(title, message, duration=4)
    except Exception as e:
        print(f"[TOAST ERROR] {e}")

# === Get Next ID Helper ===
def get_next_id(table, column):
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor(buffered=True)
    cursor.execute(f"SELECT MAX({column}) FROM {table}")
    result = cursor.fetchone()[0]
    cursor.close()
    conn.close()
    return (result or 0) + 1

# === Insert or Update Job Metadata ===
def insert_or_update_job(job_name, schedule_time, schedule_frequency, schedule_type, created_by):
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor(buffered=True)

    cursor.execute("SELECT job_number FROM he_job_master WHERE job_name = %s", (job_name,))
    result = cursor.fetchone()

    if result:
        cursor.execute("""
            UPDATE he_job_master
            SET start_time = %s, schedule_frequency = %s, schedule_type = %s, updated_at = NOW()
            WHERE job_name = %s
        """, (schedule_time, schedule_frequency, schedule_type, job_name))
    else:
        job_number = get_next_id("he_job_master", "job_number")
        cursor.execute("""
            INSERT INTO he_job_master (
                job_number, job_name, start_time, schedule_frequency, schedule_type,
                created_by, created_at, updated_at
            ) VALUES (%s, %s, %s, %s, %s, %s, NOW(), NOW())
        """, (job_number, job_name, schedule_time, schedule_frequency, schedule_type, created_by))

    conn.commit()
    cursor.close()
    conn.close()

# === Get Next Run Number ===
def get_next_run_number(job_number):
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor(buffered=True)
    cursor.execute("SELECT MAX(job_run_number) FROM he_job_execution WHERE job_number = %s", (job_number,))
    result = cursor.fetchone()[0]
    cursor.close()
    conn.close()
    return (result or 0) + 1

# === Log to Job Logs Table ===
def log_job(job_number, run_number, description, created_by):
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO he_job_logs (job_number, job_run_number, job_log_description,
                                 created_by, created_at, updated_at)
        VALUES (%s, %s, %s, %s, NOW(), NOW())
    """, (job_number, run_number, description, created_by))
    conn.commit()
    cursor.close()
    conn.close()

# === Job Execution Logic ===
def run_scheduled_job(job_name, created_by=1):
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor(buffered=True)

    cursor.execute("SELECT job_number FROM he_job_master WHERE job_name = %s", (job_name,))
    job_number_row = cursor.fetchone()
    if not job_number_row:
        print(f"[ERROR] Job '{job_name}' not found.")
        return

    job_number = job_number_row[0]
    job_run_number = get_next_run_number(job_number)
    start_time_now = datetime.now()

    try:
        cursor.execute("""
            INSERT INTO he_job_execution (
                job_number, job_run_number, execution_status, start_datetime,
                created_by, created_at, updated_at
            )
            VALUES (%s, %s, %s, %s, %s, NOW(), NOW())
        """, (job_number, job_run_number, "RUNNING", start_time_now, created_by))
        conn.commit()

        log_job(job_number, job_run_number, f"{job_name} started at {start_time_now}", created_by)

        # ✅ Execute the target job script
        script_path = os.path.join(SCRIPT_FOLDER, f"{job_name}.py")
        print(f"[DEBUG] Executing: {script_path}")
        if not os.path.exists(script_path):
            raise FileNotFoundError(f"Script not found: {script_path}")

        subprocess.run(["python", script_path], check=True)

        end_time = datetime.now()

        cursor.execute("""
            UPDATE he_job_execution
            SET execution_status = %s, end_datetime = %s, updated_at = NOW()
            WHERE job_number = %s AND job_run_number = %s
        """, ("SUCCESS", end_time, job_number, job_run_number))

        cursor.execute("""
            UPDATE he_job_master
            SET end_time = %s, updated_at = NOW()
            WHERE job_number = %s
        """, (end_time, job_number))

        conn.commit()
        log_job(job_number, job_run_number, f"{job_name} completed successfully at {end_time}", created_by)
        show_notification("✅ Job Success", f"{job_name} finished at {end_time.strftime('%H:%M:%S')}")

    except subprocess.CalledProcessError as e:
        end_time = datetime.now()
        cursor.execute("""
            UPDATE he_job_execution
            SET execution_status = %s, end_datetime = %s, updated_at = NOW()
            WHERE job_number = %s AND job_run_number = %s
        """, ("FAILED", end_time, job_number, job_run_number))

        cursor.execute("""
            UPDATE he_job_master
            SET end_time = %s, updated_at = NOW()
            WHERE job_number = %s
        """, (end_time, job_number))

        conn.commit()
        log_job(job_number, job_run_number, f"{job_name} failed: {e}", created_by)
        show_notification("❌ Job Failed", f"{job_name} failed at {end_time.strftime('%H:%M:%S')}")

    except Exception as e:
        print("[UNEXPECTED ERROR]")
        traceback.print_exc()
        log_job(job_number, job_run_number, f"Unexpected error: {str(e)}", created_by)

    finally:
        cursor.close()
        conn.close()

# === Schedule Setup ===
def schedule_job(job_name, schedule_time, schedule_frequency):
    try:
        time_obj = datetime.strptime(schedule_time, "%H:%M:%S")
    except ValueError:
        print("❌ Error: start_time format must be HH:MM:SS")
        return

    scheduler = BlockingScheduler()

    if schedule_frequency == 'daily':
        scheduler.add_job(lambda: run_scheduled_job(job_name, created_by), 'cron',
                          hour=time_obj.hour, minute=time_obj.minute, second=time_obj.second)
    elif schedule_frequency == 'weekly':
        scheduler.add_job(lambda: run_scheduled_job(job_name, created_by), 'cron',
                          day_of_week='mon', hour=time_obj.hour, minute=time_obj.minute, second=time_obj.second)
    elif schedule_frequency == 'monthly':
        scheduler.add_job(lambda: run_scheduled_job(job_name, created_by), 'cron',
                          day=1, hour=time_obj.hour, minute=time_obj.minute, second=time_obj.second)
    else:
        print("❌ Error: Frequency must be daily, weekly, or monthly")
        return

    show_notification("Scheduler Started", f"{job_name} will run {schedule_frequency} at {schedule_time}")
    print(f"[SCHEDULER] {job_name} scheduled {schedule_frequency} at {schedule_time}")

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        print("[STOPPED] Scheduler stopped.")

# === Main Entry Point ===
def main():
    insert_or_update_job(job_name, start_time, schedule_frequency, schedule_type, created_by)
    schedule_job(job_name, start_time, schedule_frequency)

if __name__ == "__main__":
    main()
