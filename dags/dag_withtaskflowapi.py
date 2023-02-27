from airflow.decorators import dag, task
from datetime import datetime,timedelta

default_args={
    'owner':'anirudh',
    'retries':5,
    'retry_delay':timedelta(minutes=5)
}

@dag(
    dag_id='dag_with_taskflow_api_v1',
    start_date=datetime(2023,2,24),
    schedule_interval='@daily'
)
def hello_world_etl():

    @task()
    def get_name():
        return 'Jerry'

    @task()
    def get_age():
        return 19
        
    @task()
    def greet(name,age):
        print(f'My name is {name}'
            f' I am {age} years old!')
    
    name = get_name()
    age = get_age()
    greet(name=name, age=age)

greet_dag = hello_world_etl()