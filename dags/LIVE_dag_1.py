from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.decorators import task
from datetime import datetime

def add_19(x):
    return x + 19

def subtract_23(x):
    return x - 23

with DAG(
    dag_id="LIVE_dag_1",
    start_date=datetime(2022, 10, 1),
    schedule=None,
    catchup=False
):

    # Use .expand to map over several kwargs to create a cross-product
    cross_product = PythonOperator.partial(
        task_id="cross_product"
    ).expand(
        python_callable=[add_19, subtract_23],
        op_args=[[1],[2],[3]]
    )

    @task 
    def make_sentences(name,activity,day):
        return f"{name} will {activity} on {day}!"

    make_sentences.expand(
        x=["Lilou", "Woody", "Avery"], 
        y=["sit on the laptop", "play fetch"], 
        z=["Monday", "Tuesday"]
    )

    # Use .expand_kwargs to map over sets of keyword arguments
    sets_of_kwargs = PythonOperator.partial(
        task_id="sets_of_kwargs"
    ).expand_kwargs(
        [
            {"python_callable": add_19, "op_args": [1]},
            {"python_callable": subtract_23, "op_args": [2]},
            {"python_callable": add_19, "op_args": [3]},
        ]
    )

    @task 
    def make_sentences(name,activity,day):
        return f"{name} will {activity} on {day}!"

    sentences = make_sentences.expand_kwargs(
        [
            {"x": "Lilou", "y": "sit on the laptop", "z": "Monday"},
            {"x": "Woody", "y": "sit on the laptop", "z": "Tuesday"},
            {"x": "Avery", "y": "play fetch", "z": "Monday"},

        ]
    )

    # access specific XComs from dynamically mapped tasks
    print_the_result_of_equation_3 = BashOperator(
        task_id="print_the_result_of_equation_3",
        bash_command="echo {{ ti.xcom_pull(task_ids=['sets_of_kwargs'])[2] }}"
    )

    sets_of_kwargs >> print_the_result_of_equation_3

    
    @task
    def print_the_first_sentence_with_emphasis(sentence):
        return sentence + "!!!!!"

    print_the_first_sentence_with_emphasis(sentences[0])
