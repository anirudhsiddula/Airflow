from airflow import DAG
from datetime import datetime,timedelta
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'anirudh',
     'retries' : 5,
     'retry_delay': timedelta(minutes=2)
}


with DAG(
    dag_id='our_first_dag_v3',
    default_args=default_args,
    description='This is out first dag we write',
    start_date=datetime(2023,2,22,2),
    schedule_interval='@daily'
) as dag:
    task1 = BashOperator(
        task_id='first_task',
        bash_command='echo hellow world!'
    )

    task2 = BashOperator(
        task_id='task2',
        bash_command='echo this is task 2'
    )
    
    task3 = BashOperator(
        task_id='task3',
        bash_command='echo this is task3'
    )

    # task1.set_downstream(task2)
    # task2.set_downstream(task3)

    task1 >> task2
    task1 >> task3
