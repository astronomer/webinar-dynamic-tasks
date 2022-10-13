"""Toy example DAG showing how the .map method works.

This example shows the syntax and workings of using the .map function on
the output of a task.
list_objects simulates getting outputs of a fixed format. map_objects is
a function transforming the output of list objects to return an
AirflowSkipException for all strings starting with "skip".
The mapped task mapped_printing_function maps on the transformed output,
resulting in the mapped task instances 0 and 2 to be skipped.
"""

from airflow import DAG
from airflow.decorators import task
from datetime import datetime
from airflow.exceptions import AirflowSkipException
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="4_skip_mapped_instances",
    start_date=datetime(2022, 10, 1),
    schedule=None,
    catchup=False
):

    # an upstream task returns a list of outputs in a fixed format
    @task
    def list_strings():
        return ["skip_hello", "hi", "skip_hallo", "hola", "hey"]

    # the function used to transform the upstream output before
    # a downstream task is dynamically mapped over it
    def skip_strings_starting_with_skip(string):
        if len(string) < 4:
            return string + "!"
        elif string[:4] == "skip":
            raise AirflowSkipException(
                f"Skipping {string}; as I was told!"
            )
        else:
            return string + "!"

    # transforming the output of the first task with the map function
    # for non-TaskFlow operators use
    # my_upstream_traditional_operator.output.map(mapping_function)
    transformed_list = list_strings().map(skip_strings_starting_with_skip)

    # the task using dynamic task mapping on the transformed list of strings
    @task(max_active_tis_per_dag=2)
    def mapped_printing_function(string):
        return "Say " + string

    mapped_task = mapped_printing_function.expand(
        string=transformed_list
    )

    # when skipping mapped instances mind downstream trigger rules!
    downstream_hello = BashOperator(
        task_id="downstream_hello",
        bash_command="echo hello",
        trigger_rule="all_done"
    )

    mapped_task >> downstream_hello