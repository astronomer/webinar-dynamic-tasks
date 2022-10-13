from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.decorators import task
from datetime import datetime
from airflow.operators.empty import EmptyOperator

with DAG(
    dag_id="static_loops",
    start_date=datetime(2022, 10, 1),
    schedule=None,
    catchup=False
):

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    @task
    def add_19_task_flow(x):
        return x + 19

    def add_19(x):
        return x + 19

    for i in range(5):

        my_traditional_task = PythonOperator(
            task_id=f"my_traditional_task{i}",
            python_callable=add_19,
            op_args=[i]
        )

        start >> add_19_task_flow(i) >> end

        start >> my_traditional_task >> end


