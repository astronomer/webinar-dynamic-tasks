"""
### Use the .zip method to combine XComArg/.output objects

This DAG shows a simple example of how you can use the .zip method of the XComArg object.
"""

from airflow.decorators import dag, task
from pendulum import datetime


@dag(start_date=datetime(2023, 5, 1), schedule=None, catchup=False)
def zip_toy_example():
    # upstream task 1 returning a list
    @task
    def get_123():
        return [1, 2, 3]

    # upstream task 2 returning a list
    @task
    def get_abcd():
        return ["a", "b", "c", "d"]

    zipped_input = get_123().zip(get_abcd(), fillvalue="MISSING")

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


zip_toy_example()
