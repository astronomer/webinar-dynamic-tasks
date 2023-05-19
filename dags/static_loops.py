"""
### Use a static loop to create a static amount of tasks

This DAG show static loops to compare about the mapped task examples in this
repository.
"""

from airflow.operators.python import PythonOperator
from airflow.decorators import dag, task
from pendulum import datetime
from airflow.operators.empty import EmptyOperator


@dag(start_date=datetime(2022, 5, 1), schedule=None, catchup=False)
def static_loops():
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    @task
    def add_19_task_flow(x):
        return x + 19

    def add_19(x):
        return x + 19

    for i in range(5):
        my_traditional_task = PythonOperator(
            task_id=f"my_traditional_task{i}", python_callable=add_19, op_args=[i]
        )

        start >> add_19_task_flow(i) >> end

        start >> my_traditional_task >> end

static_loops()