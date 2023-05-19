"""
### Dynamically map the S3CopyObjectOperator over source/destination pairs

Simple DAG that shows how .map can be used together with .expand_kwargs to dynamically
map over sets of parameters derived from an upstream task.
"""

from airflow.decorators import dag
from pendulum import datetime
from airflow.providers.amazon.aws.operators.s3 import (
    S3CopyObjectOperator,
    S3ListOperator,
)

S3_BUCKET_1 = "dyn-tasks-webinar-1"
S3_BUCKET_2 = "dyn-tasks-webinar-2"


@dag(
    start_date=datetime(2023, 5, 1),
    schedule=None,
    catchup=False,
)
def dynamic_s3_copy_source_dest_pairs():
    # upstream task resturning a list of keys in an S3 bucket
    list_files_s3_bucket = S3ListOperator(
        task_id="list_files_s3_bucket", aws_conn_id="aws_conn", bucket=S3_BUCKET_1
    )

    # .map function transforming the list to map over
    def create_pairs(key):
        source_dest_pair = {
            "source_bucket_key": f"s3://{S3_BUCKET_1}/{key}",
            "dest_bucket_key": f"s3://{S3_BUCKET_2}/Copy_from_{S3_BUCKET_1}_{key}",
        }
        return source_dest_pair

    # transformation magic
    source_dest_pairs = list_files_s3_bucket.output.map(create_pairs)

    # the dynamically mapped task using expand_kwargs
    S3CopyObjectOperator.partial(
        task_id="copy_files_S3", aws_conn_id="aws_conn"
    ).expand_kwargs(source_dest_pairs)


dynamic_s3_copy_source_dest_pairs()
