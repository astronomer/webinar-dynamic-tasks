from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.decorators import task
from datetime import datetime

with DAG(
    dag_id="LIVE_dag_traditional_operators",
    start_date=datetime(2022, 10, 1),
    schedule=None,
    catchup=False
):

    # define helper functions
    def add_19(x):
        return x + 19

    def subtract_23(x):
        return x - 23

    # create a cross product
    cross_product_calculations = PythonOperator.partial(
        task_id="cross_product_calculations"
    ).expand(
        python_callable=[add_19, subtract_23],
        op_args=[[1],[2],[3]]
    )

    # map over sets of kwargs
    sets_of_kwargs_calculations = PythonOperator.partial(
        task_id="sets_of_kwargs_calculations"
    ).expand_kwargs(
        [
            {"python_callable": add_19, "op_args": [1]},
            {"python_callable": subtract_23, "op_args": [2]},
            {"python_callable": add_19, "op_args": [3]},
        ]
    )

    # Access Xcoms
    print_the_result_of_equation_3 = BashOperator(
        task_id="print_the_result_of_equation_3",
        bash_command="echo {{ ti.xcom_pull(task_ids=['sets_of_kwargs_calculations'])[2] }}"
    )

    sets_of_kwargs_calculations >> print_the_result_of_equation_3