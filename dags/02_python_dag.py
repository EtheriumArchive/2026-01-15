import pendulum
from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator

import random
def random_language():
    lang_list = ["PYTHON", 'JAVA', 'RUST']
    lang = random.sample(lang_list, 1)
    print("SELECTED LANGUAGE : ", lang)
    return lang
 

default_args = dict(
    owner = 'sujuning',
    email = ['ggulmasida@gmail.com'],
    email_on_failure = False,
    retries = 5
    )

with DAG(
    dag_id="02_python_dag",
    start_date=pendulum.datetime(2025, 6, 1, tz='Asia/Seoul'),
    schedule="30 10 * * *", # cron 표현식
    tags = ['20250725'],
    default_args = default_args,
    catchup=False
):
    
    py1 = PythonOperator(task_id="py1", python_callable=random_language)

#    task1 >> task2 >> task3 >> task4 >> task5

py1