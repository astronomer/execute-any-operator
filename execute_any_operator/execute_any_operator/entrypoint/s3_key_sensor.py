
import click
from airflow.exceptions import AirflowSensorTimeout
from execute_any_operator.operators.execute_any import ExecuteAnyOperator
from execute_any_operator.utils.helpers import _remove_unused_kwargs


@click.command()
@click.option("--bucket-name", default=None, help="Name of the S3 bucket. Only needed when `bucket_key` is not provided as a full s3:// url.")
@click.option("--wildcard-match", default=False, help="whether the bucket_key should be interpreted as a Unix wildcard pattern.")
@click.option("--aws-conn-id", default='aws_default', help="A reference to the s3 connection.")
@click.option("--verify", default=None, help="Whether or not to verify SSL certificates for S3 connection. By default SSL certificates are verified.")
@click.option("--poke-interval", default=60, help="Time in seconds that the job should wait in between each tries.")
@click.option("--timeout", default=60 * 60 * 24 * 7, help="Time, in seconds before the task times out and fails.")
@click.option("--exponential-backoff", default=False, help="Allow progressive longer waits between pokes by using exponential backoff algorithm.")
@click.argument("bucket-key", required=True)
def s3_key_sensor(bucket_key, **kwargs):
    """Waits for a key (a file-like instance on S3) to be present in a S3 bucket.
    S3 being a key/value it does not support folders. The path is just a key
    a resource.
    """
    click.echo("Executing S3KeySensor")
    task = ExecuteAnyOperator(
        operator="airflow.providers.amazon.aws.sensors.s3_key:S3KeySensor",
        bucket_key=bucket_key,
        **_remove_unused_kwargs(kwargs)
    )
    try:
        task.execute()
        click.echo(f"File '{bucket_key}' exists")
    except AirflowSensorTimeout:
        click.echo(f"File '{bucket_key}' not found")
