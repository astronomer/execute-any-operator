import os

import click
from execute_any_operator.entrypoint.arrow_hdfs_sensor import arrow_hdfs_sensor
from execute_any_operator.entrypoint.bash_operator import bash_operator
from execute_any_operator.entrypoint.hive_operator import hive_operator
from execute_any_operator.entrypoint.kubernetes_pod_operator import \
    kubernetes_pod_operator
from execute_any_operator.entrypoint.python_operator import python_operator
from execute_any_operator.entrypoint.remote_bash_operator import \
    remote_bash_operator
from execute_any_operator.entrypoint.s3_key_sensor import s3_key_sensor
from execute_any_operator.entrypoint.simple_http_operator import \
    simple_http_operator
from execute_any_operator.utils.helpers import _multi_tuple_to_dict


@click.group()
@click.version_option()
@click.option(
    "--env-var",
    default=None,
    multiple=True,
    type=click.Tuple([str, str]),
    callback=_multi_tuple_to_dict,
    help="Environment variables for operators to use, can also accessed via Variable.get."
)
def cli(env_var):
    """Executes Airflow operator classes as Python objects without the need for running Airflow."""
    os.environ.update({k.upper(): v for k, v in env_var.items()})


cli.add_command(bash_operator)
cli.add_command(arrow_hdfs_sensor)
cli.add_command(hive_operator)
cli.add_command(kubernetes_pod_operator)
cli.add_command(python_operator)
cli.add_command(remote_bash_operator)
cli.add_command(s3_key_sensor)
cli.add_command(simple_http_operator)

if __name__ == "__main__":
    cli()
