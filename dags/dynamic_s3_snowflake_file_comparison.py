"""
### Use the .zip method to combine XComArg/.output objects to compare files in S3 and Snowflake

This DAG shows an example implementation of comparing data between two
S3 buckets and a table in Snowflake using the .zip() method of the
XComArg/.output object.

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

from airflow.decorators import dag, task
from pendulum import datetime
from airflow.providers.amazon.aws.operators.s3 import S3ListOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
import re
import logging

# get the Airflow task logger
task_logger = logging.getLogger("airflow.task")

# define the buckets and table to be compared
S3_BUCKET_1 = "zip-bucket-one"
S3_BUCKET_2 = "zip-bucket-two"
SNOWFLAKE_DB = "SANDBOX"
SNOWFLAKE_SCHEMA = "TAMARAFINGERLIN"
SNOWLAKE_TABLE = "ZIP_EXAMPLE"


@dag(
    start_date=datetime(2023, 5, 1),
    schedule_interval=None,
    catchup=False,
)
def dynamic_s3_snowflake_file_comparison():
    list_files_in_S3_one = S3ListOperator(
        task_id="list_files_in_S3_one", aws_conn_id="aws_conn", bucket=S3_BUCKET_1
    )

    list_files_in_S3_two = S3ListOperator(
        task_id="list_files_in_S3_two", aws_conn_id="aws_conn", bucket=S3_BUCKET_2
    )

    query_snowflake = SnowflakeOperator(
        task_id="query_snowflake",
        snowflake_conn_id="snowflake_conn",
        sql=f"""
            SELECT CAST(DATE AS TEXT) AS DATE, CUSTOMER AS CUSTOMER
            FROM {SNOWFLAKE_DB}.{SNOWFLAKE_SCHEMA}.{SNOWLAKE_TABLE}
            ORDER BY DATE ASC
        """,
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
            snowflake_entry["DATE"] == date_file_1_converted_format
            and snowflake_entry["CUSTOMER"] == name_file_2
        ):
            task_logger.info(f"Matching entries on {date_file_1}")
        else:
            raise ValueError(
                f"""Mismatch Snowflake and S3 data!
                On {date_file_1} {S3_BUCKET_2}'s file is named {name_file_2},
                while the Snowflake entry is {snowflake_entry['CUSTOMER']}"""
            )

    compare_dates_logfiles.expand(
        # call .zip() on the .output object to zip together several XComArgs
        input_tuple=list_files_in_S3_one.output.zip(
            list_files_in_S3_two.output,
            query_snowflake.output,
            # fillvalue defines the entry in the zipped tuple in case one list
            # is shorter than the others
            fillvalue="MISSING ENTRY",
        )
    )


dynamic_s3_snowflake_file_comparison()
