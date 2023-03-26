from datetime import datetime
from airflow.models import Variable
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
import requests
import pandas as pd
import psycopg2 as pg
from sqlalchemy import create_engine

ETL_NAME = 'practicum_project_5'
ETL_VERSION = 'v1'
ETL_DESC = 'Проектная работа по 5 спринту'
ETL_OWNER = 'Malyutin Dmitriy'

DAG_ID = '{}_{}'.format(ETL_NAME, ETL_VERSION)
IMAGE_NAME = '{}'.format(ETL_NAME)

EXEC_DATE = "{{(execution_date)}}"

# Get AIRFLOW variables
api_key = Variable.get('API_KEY')
api_url = Variable.get('API_URL')
POSTGRS_HOST = Variable.get('POSTGRS_HOST')
POSTGRS_DB_NAME = Variable.get('POSTGRS_DB_NAME')
POSTGRS_USER = Variable.get('POSTGRS_USER')
POSTGRS_PASSWORD = Variable.get('POSTGRS_PASSWORD')


default_args = {
    'owner': ETL_OWNER,
    'depends_on_past': True,
    'start_date': datetime(2023, 3, 25)
}

methods = ['restaurants', 'couriers', 'deliveries']


def api_to_pg():
    headers = {
        'X-Nickname': 'Dmitriy',
        'X-Cohort': '10',
        'X-API-KEY': api_key
    }

    def get_restaurants():
        url = f'{api_url}/{methods[0]}?sort_field=0&sort_direction=asc&limit=0&offset=0'
        resp = requests.get(url, headers=headers)
        restaurants = resp.json()
        return restaurants

    def get_couriers():
        url = f'{api_url}/{methods[1]}?sort_field=0&sort_direction=asc&limit=0&offset=0'
        resp = requests.get(url, headers=headers)
        couriers = resp.json()
        return couriers

    def get_deliveries():
        url = f'{api_url}/{methods[2]}?sort_field=0&sort_direction=asc&limit=0&offset=0'
        resp = requests.get(url, headers=headers)
        deliveries = resp.json()
        return deliveries

    df_restaurants = pd.json_normalize(get_restaurants())
    df_couriers = pd.json_normalize(get_couriers())
    df_deliveries = pd.json_normalize(get_deliveries())

    dbConnection = create_engine(f"postgresql+psycopg2://{POSTGRS_USER}:{POSTGRS_PASSWORD}@{POSTGRS_HOST}:5432/{POSTGRS_DB_NAME}")

    df_restaurants.to_sql('restaurants', dbConnection, schema='stg', if_exists='append', index=False)
    df_couriers.to_sql('couriers', dbConnection, schema='stg', if_exists='append', index=False)
    df_deliveries.to_sql('deliveries', dbConnection, schema='stg', if_exists='append', index=False)


with DAG(
        catchup=False,
        dag_id=DAG_ID,
        description=ETL_DESC,
        default_args=default_args,
        schedule_interval='0/15 * * * *',
        max_active_runs=1,
        tags=['practicum', 'project', 'sprint5']
) as dag:

    init_db = PostgresOperator(
        task_id='init_db',
        postgres_conn_id='POSTGRES',
        sql='src/sql/ddl.sql'
    )

    from_api_to_pg = PythonOperator(
        task_id='from_api_to_pg',
        python_callable=api_to_pg
    )

    insert_dds = PostgresOperator(
        task_id='insert_dds',
        postgres_conn_id='POSTGRES',
        sql='src/sql/stg_to_dds.sql'
    )

    insert_cdm = PostgresOperator(
        task_id='insert_cdm',
        postgres_conn_id='POSTGRES',
        sql='src/sql/dds_to_cdm.sql'
    )

    init_db >> from_api_to_pg >> insert_dds >> insert_cdm

