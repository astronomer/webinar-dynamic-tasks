"""Toy example DAG showing dynamic task mapping with XComs.

These features are available in Airflow version 2.3+.
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.decorators import task
from datetime import datetime
from airflow import XComArg

with DAG(
    dag_id="syntax_mapping_over_xcoms",
    start_date=datetime(2022, 7, 1),
    schedule_interval=None,
    catchup=False
) as dag:

    # EXAMPLE 1: upstream and downstream task are defined using the TaskFlowAPI
    @task
    def one_two_three_TF():
        """Return the list [1, 2, 3]."""
        return [1, 2, 3]

    @task
    def plus_10_TF(x):
        """Add 10 to x."""
        return x+10

    plus_10_TF.partial().expand(x=one_two_three_TF())

    # EXAMPLE 2: upstream task is defined using the TaskFlowAPI,
    # downstream task is defined using a traditional operator
    @task
    def one_two_three_TF_2():
        """Return the list [[1], [2], [3]]."""
        return [[1], [2], [3]]

    def plus_10_traditional(x):
        """Add 10 to x."""
        return x+10

    plus_10_task = PythonOperator.partial(
        task_id="plus_10_task",
        python_callable=plus_10_traditional
    ).expand(
        op_args=one_two_three_TF_2()
    )

    # EXAMPLE 3: upstream task is defined using a traditional operator,
    # downstream task is defined using the TaskFlowAPI
    def one_two_three_classical():
        """Return the list [1, 2, 3]."""
        return [1, 2, 3]

    @task
    def plus_10_TF_2(x):
        """Add 10 to x."""
        return x+10

    one_two_three_task = PythonOperator(
        task_id="one_two_three_task",
        python_callable=one_two_three_classical
    )

    plus_10_TF_2.partial().expand(x=XComArg(one_two_three_task))

    # EXAMPLE 4: both upstream and downstream tasks are defined using
    # traditional operators
    def one_two_three_traditional():
        """Return the list [[1], [2], [3]]."""
        return [[1], [2], [3]]

    def plus_10_traditional(x):
        """Add 10 to x."""
        return x+10

    one_two_three_task_2 = PythonOperator(
        task_id="one_two_three_task_2",
        python_callable=one_two_three_traditional
    )

    plus_10_task_both_traditional = PythonOperator.partial(
        task_id="plus_10_task_both_traditional",
        python_callable=plus_10_traditional
    ).expand(
        op_args=XComArg(one_two_three_task_2)
    )

    # set dependencies
    one_two_three_task_2 >> plus_10_task_both_traditional

    # EXAMPLE 5: Mix zip and the cross-product behavior.
    # Available in Airflow version 2.3+.
    def add_num(x, y, word):
        """Return a string containing the sum of x + y and a word."""
        return f"{x+y}: {word}"

    mix_cross_and_zip = PythonOperator.partial(
        task_id="mix_cross_and_zip",
        python_callable=add_num
    ).expand(
        op_args=list(zip([1, 2, 3], [10, 20, 30])),
        op_kwargs=[{"word": "hi"}, {"word": "bye"}]
    )

    # results in 6 mapped instances printing:
    # "11: hi", "22: hi", "33: hi", "11: bye", "22: bye", "33: bye"
