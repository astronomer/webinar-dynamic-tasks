"""
### Dynamically map a task over the output of another dynamically mapped task

Simple DAG that shows repeated dynamic task mapping. 
Learn more at: https://docs.astronomer.io/learn/dynamic-tasks#repeated-mapping
"""

from airflow.decorators import dag, task
from pendulum import datetime


@dag(
    start_date=datetime(2023, 5, 1),
    schedule=None,
    catchup=False,
)
def map_over_map():
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


map_over_map()
