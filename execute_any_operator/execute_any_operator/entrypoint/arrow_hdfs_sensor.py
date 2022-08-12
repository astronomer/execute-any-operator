import click
from execute_any_operator.operators.execute_any import ExecuteAnyOperator
from execute_any_operator.utils.helpers import _remove_unused_kwargs


@click.command()
@click.option(
    "--hdfs-conn-id",
    default="hdfs_default",
    help="The Airflow connection used for HDFS credentials.",
)
@click.option(
    "--ignored-ext", default=None, help="This is the list of ignored extensions."
)
@click.option("--ignore-copying", default=True, help="Shall we ignore?")
@click.option("--file-size", default=None, help="This is the size of the file.")
@click.option(
    "--poke-interval",
    default=60,
    help="Time in seconds that the job should wait in between each tries.",
)
@click.option(
    "--timeout",
    default=60 * 60 * 24 * 7,
    help="Time, in seconds before the task times out and fails.",
)
@click.option(
    "--exponential-backoff",
    default=False,
    help="Allow progressive longer waits between pokes by using exponential backoff algorithm.",
)
@click.argument("filepath", required=True)
def arrow_hdfs_sensor(filepath, **kwargs):
    """Apache PyArrow based HDFS sensor with Python3 Kerberos support."""
    click.echo("Executing HdfsSensor")
    task = ExecuteAnyOperator(
        operator="arrow_hdfs_sensor.sensor:ArrowHdfsSensor",
        filepath=filepath,
        **_remove_unused_kwargs(kwargs)
    )
    task.execute()
