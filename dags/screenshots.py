from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.decorators import task
from datetime import datetime
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.exceptions import AirflowSkipException

with DAG(
    dag_id="screenshots",
    start_date=datetime(2022, 10, 1),
    schedule=None,
    catchup=False
):

    start = EmptyOperator(task_id="start")
    midpoint = EmptyOperator(task_id="midpoint")
    end = EmptyOperator(task_id="end")

    def add_19(x):
        return x + 19

    extract_dynamic = PythonOperator.partial(
            task_id="extract_dynamic",
            python_callable=add_19
    ).expand(op_args=[[0],[1],[2]])


    @task
    def add_19_task_flow(x):
        return x + 19

    add_19_task_flow.partial().expand(x=[1,2,3])
    

    midpoint >> extract_dynamic >> end



    t1 = BashOperator.partial(
        task_id="t1"
    ).expand(
        bash_command=[
            "echo $WORD", 
            "echo `expr length $WORD`",
            "echo ${WORD//e/X}"
        ],
        env=[
            {"WORD": "hello"},
            {"WORD": "tea"},
            {"WORD": "goodbye"}
        ]
    )


    t2 = BashOperator.partial(task_id="t2").expand_kwargs(
        [
            {"bash_command": "echo $WORD","env" : {"WORD": "hello"}},
            {"bash_command": "echo `expr length $WORD`", "env" : {"WORD": "tea"}},
            {"bash_command": "echo ${WORD//e/X}", "env" : {"WORD": "goodbye"}}
        ]
    )

    @task
    def get_123():
        return [1,2,3]

    @task 
    def get_abc():
        return ["a", "b", "c"]

    @task
    def multiply(input):
        num = input[0]
        letter = input[1]
        return num * letter

    multiply.partial().expand(
        input=get_123().zip(get_abc())
    )

    @task
    def get_123_strings():
        return ["1", "2", "3"]

    def turn_str_to_int(string):
        if string == "2":
            raise AirflowSkipException("nope")
        return int(string)

    transformed_list = get_123_strings().map(turn_str_to_int)

    @task
    def add_23(integer):
        return integer + 23

    dependent_task = EmptyOperator(
        task_id="dependent_task"
    )

    add_23.partial().expand(integer=transformed_list) >> dependent_task


    