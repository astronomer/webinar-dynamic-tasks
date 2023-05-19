"""
### Dynamically map tasks over multiple parameters

Simple DAG that shows how to use dynamic task mapping with multiple parameters.
"""

from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.decorators import dag, task
from pendulum import datetime


@dag(start_date=datetime(2023, 5, 1), schedule_interval=None, catchup=False)
def dynamic_task_mapping_multiple_parameters_2():
    # EXAMPLE 1: mapping over 2 kwargs - cross product
    # Available in Airflow version 2.3+.
    BashOperator.partial(task_id="cross_product_example").expand(
        bash_command=[
            "echo $WORD",  # prints the env variable WORD
            "echo `expr length $WORD`",  # prints the number of letters in WORD
            "echo ${WORD//e/X}",  # replaces each "e" in WORD with "X"
        ],
        env=[{"WORD": "hello"}, {"WORD": "tea"}, {"WORD": "goodbye"}],
    )
    # results in 3x3=9 mapped task instances printing:
    # hello, tea, goodbye, 5, 3, 7, hXllo, tXa, goodbyX

    # EXAMPLE 2: mapping over sets of kwarg.
    # Available in Airflow version 2.4+.
    @task
    def turn_into_XComArg():
        """Turn sets of keywordarguments into an XComArg."""
        return [
            {"bash_command": "echo $WORD", "env": {"WORD": "hello"}},
            {"bash_command": "echo `expr length $WORD`", "env": {"WORD": "tea"}},
            {"bash_command": "echo ${WORD//e/X}", "env": {"WORD": "goodbye"}},
        ]

    kwargs = turn_into_XComArg()

    BashOperator.partial(task_id="expand_kwargs_XComArg").expand_kwargs(kwargs)

    # results in 3 mapped instances printing:
    # hello, 3, goodbyX

    # EXAMPLE 3: mapping over a list containing a zip, TaskFlowAPI
    # Available in Airflow version 2.4+.
    zipped_arguments = list(zip([1, 2, 3], [10, 20, 30], [100, 200, 300]))
    # zipped_arguments contains: [(1,10,100), (2,20,200), (3,30,300)]
    # the zupped argumenta can also be created using the .zip() method of
    # the XComArg object.

    # creating the mapped task instances using the TaskFlowAPI
    @task
    def TaskFlow_add_numbers(zipped_x_y_z):
        """Add x, y and z.

        Since the zipped_arguments are passed in directly as a positional
        argument, the tuples have to be indexed into to add the numbers.
        """
        return zipped_x_y_z[0] + zipped_x_y_z[1] + zipped_x_y_z[2]

    TaskFlow_add_numbers.expand(zipped_x_y_z=zipped_arguments)

    # results in 3 mapped instances printing:
    # 111, 222, 333

    # EXAMPLE 4: mapping over a list containing a zip,
    # traditional PythonOperator
    # Available in Airflow version 2.4+.
    def add_numbers(x, y, z):
        """Add x, y and z.

        Since the op_args argument of the traditional PythonOperator expects
        a list, the contents of the zipped tuples are put into the function
        in an unpacked form.
        """
        return x + y + z

    PythonOperator.partial(
        task_id="standard_add_numbers", python_callable=add_numbers
    ).expand(op_args=zipped_arguments)

    # results in 3 mapped instances printing:
    # 111, 222, 333

    # EXAMPLE 5: Mix zip and the cross-product behavior.
    def add_num(x, y, word):
        """Return a string containing the sum of x + y and a word."""
        return f"{x+y}: {word}"

    PythonOperator.partial(task_id="mix_cross_and_zip", python_callable=add_num).expand(
        op_args=list(zip([1, 2, 3], [10, 20, 30])),
        op_kwargs=[{"word": "hi"}, {"word": "bye"}],
    )

    # results in 6 mapped instances printing:
    # "11: hi", "22: hi", "33: hi", "11: bye", "22: bye", "33: bye"


dynamic_task_mapping_multiple_parameters_2()
