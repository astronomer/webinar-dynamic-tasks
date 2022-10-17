from airflow import DAG, AirflowException
from airflow.operators.python import PythonOperator
from airflow.decorators import task
from datetime import datetime

with DAG(
    dag_id="zip_toy_example",
    start_date=datetime(2022, 10, 1),
    schedule=None,
    catchup=False
):
    # upstream task 1 returning a list
    @task
    def get_123():
        return [1, 2, 3]

    # upstream task 2 returning a list
    @task 
    def get_abcd():
        return ["a","b","c","d"]

    zipped_input = get_123().zip(get_abcd(), fillvalue= "MISSING")

    # optional: having a reducing task after the mapping task
    @task
    def multiply_letters(zipped_input):
        num = zipped_input[0]
        letter = zipped_input[1]

        if num == "MISSING" or letter == "MISSING":
            return "An element is missing, no multiplication."
        else:
            return num * letter

    multiply_letters.expand(zipped_input=zipped_input)
    