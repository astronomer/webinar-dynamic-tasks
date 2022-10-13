"""Example DAG showing the use of the .map method."""

from airflow import DAG
from datetime import datetime
from airflow.exceptions import AirflowSkipException
from airflow.providers.amazon.aws.operators.s3 import S3ListOperator
from airflow.providers.amazon.aws.operators.s3 import S3DeleteObjectsOperator

"""
This DAG shows an example implementation of dynamically mapping over an
S3DeleteObjectsOperator and skipping deletion of certain files based on their
filetype using a .map mapping. This type of mapping was added in Airflow 2.4.

The first task 'list_files_S3' lists all files in S3_BUCKET. Instead of passing
the input directly to the deletion task a mapping step is used in between,
using the map_files_for_deletion function. This function will not create an
Airflow task.
map_files_for_deletion maps all files of the type json, yml and txt to an
Airflow exception causing these files to be skipped in the deletion task.

The delete_files task is dynamically mapped over the transformed list of files.
"""

S3_BUCKET = "your S3 bucket"

with DAG(
    dag_id="use_case_example_dag_map_function",
    start_date=datetime(2022, 10, 1),
    schedule=None,
    catchup=False
):

    # the S3ListOperator will return all names of files in the S3 bucket and
    # can only filter by prefix, not by filetype
    list_files_S3 = S3ListOperator(
        task_id="list_files_S3",
        aws_conn_id="aws_conn",
        bucket=S3_BUCKET
    )

    # the mapping function transforms the list of all filenames into a list of
    # AirflowSkipExceptions and filenames causing all json, yml and txt files
    # to be skipped by the downstream task
    def map_files_for_deletion(filename):
        if filename.rsplit(".", 1)[-1] in ("json", "yml", "txt"):
            raise AirflowSkipException(f"Skip deletion to keep {filename}")
        return filename

    # using .map on the output object of the upstream operator
    # this does not create a task in the Airflow UI
    transformed_file_list = list_files_S3.output.map(map_files_for_deletion)

    # the S3DeleteObjectsOperator dynamically maps over the transformed list
    delete_files = S3DeleteObjectsOperator.partial(
        task_id="delete_files",
        aws_conn_id="aws_conn",
        bucket=S3_BUCKET
    ).expand(keys=transformed_file_list)
