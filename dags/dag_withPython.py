from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator

default_args={
    'owner':'anirudh',
    'retries':5,
    'retry_delay':timedelta(minutes=5)
}

def greet(ti):
    first_name = ti.xcom_pull(task_ids='name', key='first_name')
    last_name = ti.xcom_pull(task_ids='name', key='last_name')
    age = ti.xcom_pull(task_ids='age', key='age')
    print(f'hello my name is {first_name} {last_name}'
            f'and i am {age} years old')

def get_name(ti):
    ti.xcom_push(key='first_name', value='Jeryy')
    ti.xcom_push(key='last_name', value='Fried')

def get_age(ti):
    ti.xcom_push(key='age',value=19)

with DAG(
    dag_id='pythondag1_v1',
    default_args=default_args,
    description='first python dag',
    start_date = datetime(2023,2,25,2),
    schedule_interval='@daily'
) as dag:

    task1 = PythonOperator(
        task_id='greet',
        python_callable=greet
        # op_kwargs={'age':18}
    )

    task2 = PythonOperator(
        task_id='name',
        python_callable=get_name
    )

    task3 = PythonOperator(
        task_id='age',
        python_callable=get_age
    )

    [task2, task3] >> task1
