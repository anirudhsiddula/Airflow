from airflow import DAG
from datetime import datetime, timedelta
# from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

default_args={
    'owner':'anirudh',
    'retries':1,
    'retry_delay':timedelta(seconds=5)
}

with DAG(
    dag_id='postgres_test_v1',
    default_args=default_args,
    start_date = datetime(2023,3,22,2),
    schedule_interval='@daily'
) as dag:
    task1 = PostgresOperator(
        task_id='create_postgres_table',
        postgres_conn_id='postgres_localhost',
        sql = """
         create table if not exists dag_runs (
            dt date,
            dag_id character varying,
            primary key (dt, dag_id)
            );
        """
    )
    task2 = PostgresOperator(
        task_id='insert_into_postgres_table',
        postgres_conn_id='postgres_localhost',
        sql = """
            insert into dag_runs (dt,dag_id) values ('{{ ds }}','{{ dag.dag_id }}')
        """
    )

    # print('insert into dag_runs (dt, dag_id) values ('{{ ds }}','{{ dag.dag_id }}')')
    task1 >> task2