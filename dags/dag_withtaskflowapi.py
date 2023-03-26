from airflow.decorators import dag, task
from datetime import datetime,timedelta

default_args={
    'owner':'anirudh',
    'retries':5,
    'retry_delay':timedelta(minutes=5)
}

@dag(
    dag_id='dag_with_taskflow_api_v1',
    default_args=default_args,
    start_date=datetime(2023,2,24),
    schedule_interval='@daily'
)
def hello_world_etl():

    @task(multiple_outputs=True)
    def get_name():
        return {
            'firstname':'Jerry',
            'lastname':'Anirudh'  
        }

    @task()
    def get_age():
        print('this is get_age function')
        return 10
        
    @task()
    def greet(first_name,last_name,age):
        print(f'My name is {first_name} {last_name}'
            f' I am {age} years old!')
    
    name = get_name()
    print(name)
    age = get_age()
    greet(first_name=name['firstname'],
        last_name=name['lastname'],
        age=age)

greet_dag = hello_world_etl()