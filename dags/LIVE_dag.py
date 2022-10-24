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
    dag_id="LIVE_dag",
    start_date=datetime(2022, 10, 1),
    schedule=None,
    catchup=False
):

    ### 1) Use .expand to map over several kwargs to create a cross-product
    cross_product_calculations = PythonOperator.partial(
        task_id="cross_product_calculations"
    ).expand(
        python_callable=[add_19, subtract_23],
        op_args=[[1],[2],[3]]
    )

    @task 
    def cross_product_sentences(name,activity,day):
        return f"{name} will {activity} on {day}!"

    cross_product_sentences.expand(
        name=["Lilou", "Woody", "Avery"], 
        activity=["sit on the laptop", "play fetch"], 
        day=["Monday", "Tuesday"]
    )

    ### 2) Use .expand_kwargs to map over sets of keyword arguments
    @task 
    def sets_of_kwargs_sentences(name,activity,day):
        return f"{name} will {activity} on {day}!"

    sentences = sets_of_kwargs_sentences.expand_kwargs(
        [
            {"name": "Lilou", "activity": "sit on the laptop", "day": "Monday"},
            {"name": "Woody", "activity": "sit on the laptop", "day": "Tuesday"},
            {"name": "Avery", "activity": "play fetch", "day": "Monday"}

        ]
    )

    sets_of_kwargs_calculations = PythonOperator.partial(
        task_id="sets_of_kwargs_calculations"
    ).expand_kwargs(
        [
            {"python_callable": add_19, "op_args": [1]},
            {"python_callable": subtract_23, "op_args": [2]},
            {"python_callable": add_19, "op_args": [3]},
        ]
    )

    # You can map over the input of upstream tasks!

    pets = [("Avery", "dog"), ("Lilou", "cat"), ("Woody", "cat")]

    @task
    def return_kwargs(pets):
        list_of_dicts = []
        for pet in pets:
            pet_name = pet[0]
            animal = pet[1]

            list_of_dicts.append(
                {"name": pet_name, "activity": f"eat {animal}-food", "day": "every day"}
            )
            
        return list_of_dicts

    sets_of_kwargs_sentences.expand_kwargs(return_kwargs(pets))



    # access specific XComs from dynamically mapped tasks
    print_the_result_of_equation_3 = BashOperator(
        task_id="print_the_result_of_equation_3",
        bash_command="echo {{ ti.xcom_pull(task_ids=['sets_of_kwargs_calculations'])[2] }}"
    )

    sets_of_kwargs_calculations >> print_the_result_of_equation_3

    
    @task
    def print_the_first_sentence_with_emphasis(sentences):
        return sentences[0] + "!!!!!"

    print_the_first_sentence_with_emphasis(sentences)
