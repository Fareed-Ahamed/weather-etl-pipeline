from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.decorators import task,task_group
from datetime import datetime, timedelta

from extract.extract import ExtractClass
from load.load import LoadClass
from transform.transform import TransformClass

default_args = {
    'owner': 'Fareed',
    # 'email': ['fareedahamedj@gmail.com'],
    # 'email_on_failure': True,
    # 'retries': 3,
    # 'retry_delay': timedelta(minutes=5),
}

def _transform_weather_data():
    print("Transforming weather data")

def _load_weather_data():
    print("Loading weather data")

with DAG(
    dag_id='weather_etl_pipeline',
    default_args=default_args,
    description='ETL pipeline for weather data',
    schedule_interval='@hourly',
    start_date=datetime(2024, 1, 1),
    catchup=False
) as dag:
    
    @task_group
    def WEATHER_DATA_DAG() -> None:
        @task(task_id='extract_weather_data')
        def extract_weather_data():
            extract_object = ExtractClass()
            ewd = extract_object.extract_weather_data()
            print("Extracted weather data", ewd)
            return ewd
        

        @task(task_id='push_to_s3')
        def push_to_s3(wd):
            load_object = LoadClass()
            load_object.push_raw_data_to_s3(wd)

        @task(task_id='insert_raw_data_to_postgres')
        def insert_raw_data_to_postgres(wd):
            load_object = LoadClass()
            load_object.insert_raw_data_to_db(wd)

        
        @task(task_id='transform_weather_data')
        def transform_weather_data():
            transform_object = TransformClass()
            td = transform_object.transform_data()
            print("Transformed weather data", td)
            return td
        
        @task(task_id='load_weather_data')
        def load_weather_data(td):
            load_object = LoadClass()
            load_object.load_data(td)
            print("Loaded weather data")
        
        wd = extract_weather_data()
        pts3 = push_to_s3(wd)
        irdpg = insert_raw_data_to_postgres(wd)
        td = transform_weather_data()
        ld = load_weather_data(td)

        _ = wd >> [pts3,irdpg] >> td >> ld
        
    @task_group
    def COMPLETION_DAG() -> None:
        @task(task_id='completion')
        def completion():
            print("ETL pipeline completed")

        _ = completion()

    wdd = WEATHER_DATA_DAG()
    cd = COMPLETION_DAG()

    wdd >> cd