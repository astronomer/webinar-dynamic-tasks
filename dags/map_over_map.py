from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.decorators import task
from datetime import datetime

with DAG(
    dag_id="map_over_map",
    start_date=datetime(2022, 10, 1),
    schedule=None,
    catchup=False
):
    
    @task
    def sum_xy(x, y):
        return x + y

    # first map creating 3 mapped task instances
    mapped_sum = sum_xy.partial(y=19).expand(x=[1, 2, 3])


    @task
    def map_on_map(k, i):
        return k + i

    # mapping over a map -> still 3 mapped task instances
    mapped_over_map = map_on_map.partial(k=100).expand(i=mapped_sum)


    @task
    def map_on_map_on_map(j):
        return j * 1000

    # map over mapped map -> still 3 mapped task instances
    map_on_map_on_map.expand(j=mapped_over_map)
