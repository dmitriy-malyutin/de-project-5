from datetime import datetime
from airflow.models import Variable
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from src.python.api_to_pg import api_to_pg

ETL_NAME = 'practicum_project_5'
ETL_VERSION = 'v1'
ETL_DESC = 'Проектная работа по 5 спринту'
ETL_OWNER = 'Malyutin Dmitriy'

DAG_ID = '{}_{}'.format(ETL_NAME, ETL_VERSION)
IMAGE_NAME = '{}'.format(ETL_NAME)

EXEC_DATE = "{{(execution_date)}}"

# Get AIRFLOW variables
vars = {
    'api_key': Variable.get('API_KEY'),
    'api_url': Variable.get('API_URL'),
    'POSTGRS_HOST': Variable.get('POSTGRS_HOST'),
    'POSTGRS_DB_NAME': Variable.get('POSTGRS_DB_NAME'),
    'POSTGRS_USER': Variable.get('POSTGRS_USER'),
    'POSTGRS_PASSWORD': Variable.get('POSTGRS_PASSWORD')
}


default_args = {
    'owner': ETL_OWNER,
    'depends_on_past': True,
    'start_date': datetime(2023, 3, 25)
}


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
        python_callable=api_to_pg,
        op_kwargs=vars
    )

    coureirs_dds = PostgresOperator(
        task_id='coureirs_dds',
        postgres_conn_id='POSTGRES',
        sql='src/sql/couriers_to_dds.sql'
    )

    deliveries_dds = PostgresOperator(
        task_id='deliveries_dds',
        postgres_conn_id='POSTGRES',
        sql='src/sql/deliveries_to_dds.sql'
    )

    insert_cdm = PostgresOperator(
        task_id='insert_cdm',
        postgres_conn_id='POSTGRES',
        sql='src/sql/dds_to_cdm.sql'
    )

    init_db >> from_api_to_pg >> [coureirs_dds, deliveries_dds] >> insert_cdm

