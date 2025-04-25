from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import subprocess

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

def run_main_py(path):
    print(f"🔁 Đang chạy: {path}")
    result = subprocess.run(["python", path], capture_output=True, text=True)
    print("📄 STDOUT:", result.stdout)
    print("❗ STDERR:", result.stderr)
    result.check_returncode()

with DAG(
    dag_id='exercise_main_pipeline',
    default_args=default_args,
    description='Chạy tất cả các main.py trong mỗi Exercise hàng ngày lúc 10h sáng',
    schedule_interval='0 10 * * *',  # 10:00 UTC mỗi ngày
    start_date=datetime(2025, 4, 25),
    catchup=False,
    tags=["exercise"],
) as dag:

    exercises_dir = "/opt/airflow/Exercises"

    if not os.path.exists(exercises_dir):
        raise FileNotFoundError(f"Không tìm thấy thư mục: {exercises_dir}")

    previous_task = None

    for ex in sorted(os.listdir(exercises_dir)):
        ex_path = os.path.join(exercises_dir, ex, "main.py")
        if os.path.isfile(ex_path):
            task = PythonOperator(
                task_id=f'run_{ex.lower()}',
                python_callable=run_main_py,
                op_args=[ex_path],
            )
            if previous_task:
                previous_task >> task
            previous_task = task
