from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.decorators import task
from datetime import datetime

with DAG(
    dag_id="1_basic_map_and_reduce",
    start_date=datetime(2022, 10, 1),
    schedule=None,
    catchup=False
):
    # upstream task returning a list or dictionary
    @task
    def get_123():
        return [1, 2, 3]

    # dynamically mapped task iterating over list returned by an upstream task
    @task
    def multiply_by_y(x, y):
        return x * y

    multiplied_vals = multiply_by_y.partial(y=42).expand(x=get_123())

    # optional: having a reducing task after the mapping task
    @task
    def get_sum(vals):
        total = sum(vals)
        return total

    get_sum(multiplied_vals)
