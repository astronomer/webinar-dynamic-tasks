"""
### Dynamically check for file parity in two S3 buckets using .zip

This DAG shows an example implementation of checking for file parity between two
S3 buckets using the .zip() method of the XComArg/.output object.
"""

from airflow import AirflowException
from airflow.decorators import dag, task
from airflow.providers.amazon.aws.operators.s3 import S3ListOperator
from pendulum import datetime

# define the buckets and table to be compared
S3_BUCKET_1 = "live-bucket-1"
S3_BUCKET_2 = "live-bucket-2"


@dag(
    start_date=datetime(2023, 5, 1),
    schedule_interval=None,
    catchup=False,
)
def dynamic_s3_file_parity_zip():
    list_files_in_S3_one = S3ListOperator(
        task_id="list_files_in_S3_one", aws_conn_id="aws_conn", bucket=S3_BUCKET_1
    )

    list_files_in_S3_two = S3ListOperator(
        task_id="list_files_in_S3_two", aws_conn_id="aws_conn", bucket=S3_BUCKET_2
    )

    zipped_files = list_files_in_S3_one.output.zip(list_files_in_S3_two.output)

    @task
    def file_parity_check(filenames):
        filename_1 = filenames[0]
        filename_2 = filenames[1]
        if filename_1 == filename_2:
            return f"{filename_1} exists in both, {S3_BUCKET_1} and {S3_BUCKET_2}"
        else:
            raise AirflowException("Filenames don't match!")

    file_parity_check.expand(filenames=zipped_files)


dynamic_s3_file_parity_zip()