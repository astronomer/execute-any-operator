
import click
from execute_any_operator.operators.execute_any import ExecuteAnyOperator
from execute_any_operator.utils.helpers import _remove_unused_kwargs


@click.command()
@click.option("--hdfs-conn-id", default='hdfs_default')
@click.option("--ignored-ext", default=None)
@click.option("--ignore-copying", default=True)
@click.option("--file-size", default=None)
@click.argument("filepath")
def arrow_hdfs_sensor(filepath, **kwargs):
    """Apache PyArrow based HDFS sensor with Python3 Kerberos support."""
    click.echo("Executing HdfsSensor")
    task = ExecuteAnyOperator(
        operator="ArrowHdfsSensor",
        filepath=filepath,
        **_remove_unused_kwargs(kwargs)
    )
    task.execute()
