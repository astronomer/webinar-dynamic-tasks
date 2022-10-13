"""Example DAG showing the use of the .zip method of the XComArg object."""

from airflow import DAG, XComArg
from airflow.decorators import task
from datetime import datetime
from airflow.providers.amazon.aws.operators.s3 import S3ListOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
import re
import logging

"""
This DAG shows an example implementation of comparing data between two
S3 buckets and a table in Snowflake using the new .zip() method of the
XComArg object.

The DAG gathers the names of all .txt files in S3_BUCKET_1 and S3_BUCKET_2, as
well as information from a DATE and a CUSTOMER column in Snowflake. The
information from XCom is zipped using the new .zip() method. A comparing
function checks that the date in the name of the files from S3_BUCKET_1 and
the customer in the name of the files from S3_BUCKET_2 match up chronologically
with the information in the Snowflake table.
Any mismatch will cause a failure of the DAG.

This DAG needs both, a Snowflake and Amazon S3 connection. The format of the
.txt file names is: YYYY_MM_DD_CUSTOMERNAME.txt.
"""

# get the Airflow task logger
task_logger = logging.getLogger('airflow.task')

# define the buckets and table to be compared
S3_BUCKET_1 = "your S3 bucket 1"
S3_BUCKET_2 = "your S3 bucket 2"
SNOWFLAKE_DB = "your snowflake database"
SNOWFLAKE_SCHEMA = "your snowflake schema"
SNOWLAKE_TABLE = "your snowflake table"

with DAG(
    dag_id="use_case_example_dag_zip",
    start_date=datetime(2022, 9, 1),
    schedule_interval=None,
    catchup=False
) as dag:

    list_files_in_S3_one = S3ListOperator(
        task_id="list_files_in_S3_one",
        aws_conn_id="aws_conn",
        bucket=S3_BUCKET_1
    )

    list_files_in_S3_two = S3ListOperator(
        task_id="list_files_in_S3_two",
        aws_conn_id="aws_conn",
        bucket=S3_BUCKET_2
    )

    query_snowflake = SnowflakeOperator(
        task_id="query_snowflake",
        snowflake_conn_id="snowflake_conn",
        sql=f"""
            SELECT CAST(DATE AS TEXT) AS DATE, CUSTOMER AS CUSTOMER
            FROM {SNOWFLAKE_DB}.{SNOWFLAKE_SCHEMA}.{SNOWLAKE_TABLE}
            ORDER BY DATE ASC
        """
    )

    @task
    def compare_dates_logfiles(input_tuple):
        """Compare info from the S3 buckets and the Snowflake table.

        Compares the date from files in the S3_BUCKET_1 with the dates in
        the DATE column of the Snowflake table as well as the customer names
        from the in the files from S3_BUCKET_2 with the customer names
        in the CUSTOMER column of the Snowflake table.
        Throws an error if there is any mismatch.
        """
        date_file_1 = re.findall("(\d+_\d+_\d+)", input_tuple[0])[0]
        name_file_2 = re.findall("\d+_\d+_\d+_(.+).txt", input_tuple[1])[0]
        snowflake_entry = input_tuple[2]

        date_file_1_converted_format = date_file_1.replace("_", "-")

        if (
            snowflake_entry['DATE'] == date_file_1_converted_format
            and snowflake_entry['CUSTOMER'] == name_file_2
        ):
            task_logger.info(f"Matching entries on {date_file_1}")
        else:
            raise ValueError(
                f"""Mismatch Snowflake and S3 data!
                On {date_file_1} {S3_BUCKET_2}'s file is named {name_file_2},
                while the Snowflake entry is {snowflake_entry['CUSTOMER']}"""
            )

    compare_dates_logfiles.expand(
        # call .zip() on the XComArg object to zip together several
        # XComArgs
        input_tuple=XComArg(list_files_in_S3_one).zip(
            XComArg(list_files_in_S3_two),
            XComArg(query_snowflake),
            # fillvalue defines the entry in the zipped tuple in case one list
            # is shorter than the others
            fillvalue="MISSING ENTRY"
        )
    )
