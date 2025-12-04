from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import sys
import os
import importlib.util

# Define base directories
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(BASE_DIR)
SRC_DIR = os.path.join(PROJECT_ROOT, "src")

# Function to load module from file path
def load_module_from_path(module_name, file_path):
    spec = importlib.util.spec_from_file_location(module_name, file_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module

# Load modules dynamically
scraper_module = load_module_from_path("scraper", os.path.join(SRC_DIR, "scraper.py"))
cleaner_module = load_module_from_path("cleaner", os.path.join(SRC_DIR, "cleaner.py"))
loader_module = load_module_from_path("loader", os.path.join(SRC_DIR, "loader.py"))

scrape_main = scraper_module.main
clean_main = cleaner_module.main
load_main = loader_module.main

# Default arguments for all tasks
default_args = {
    "owner": "zhalgas",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=1),
}

# Define the DAG
with DAG(
    dag_id="habr_articles_pipeline",
    default_args=default_args,
    description="Daily pipeline to scrape, clean, and load Habr articles into database",
    schedule_interval="0 2 * * *",
    start_date=days_ago(1),
    catchup=False,
    tags=["habr", "scraping", "etl"],
    max_active_runs=1,
) as dag:
    
    scrape_task = PythonOperator(
        task_id="scrape_habr_articles",
        python_callable=scrape_main,
        execution_timeout=timedelta(minutes=30),
    )
    
    clean_task = PythonOperator(
        task_id="clean_article_data",
        python_callable=clean_main,
        execution_timeout=timedelta(minutes=10),
    )
    
    load_task = PythonOperator(
        task_id="load_to_database",
        python_callable=load_main,
        execution_timeout=timedelta(minutes=10),
    )
    
    scrape_task >> clean_task >> load_task
