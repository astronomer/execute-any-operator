
import click
from execute_any_operator.operators.execute_any import ExecuteAnyOperator
from execute_any_operator.utils.helpers import _remove_unused_kwargs

from remote_bash_operator.operator import Config, EnvironmentConfigInstance


@click.command()
@click.option(
    "-e",
    "--env",
    default=None,
    help="Environment variables for the bash script execution",
)
@click.option(
    "-o", "--output-encoding", default="utf-8", help="Output encoding of bash command"
)
@click.option(
    "-w", "--working-directory", "cwd", default=None, help="Working directory to execute the command in"
)
@click.option("--encode", default=True, help="")
@click.option("--extra_clusters", default=(), help="")
@click.option("--files", default=(), help="")
@click.option("--pom_xml_path", default="", help="")
@click.option("--log4j_path", default="", help="")
@click.option("--yarn_queue", default="", help="")
@click.option("--yarn_tags", default="", help="")
@click.argument("command", required=True)
@click.argument("cluster", required=True)
@click.argument("user", required=True)
@click.argument("job-name", required=True)
@click.argument("memory", type=int, required=True)
@click.argument("vcores", type=int, required=True)
def remote_bash_operator(command, cluster, user, job_name, memory, vcores, **kwargs):
    """Execute a Bash script, command or set of commands."""
    click.echo("Executing RemoteBashOperator")
    task = ExecuteAnyOperator(
        operator="remote_bash_operator.operator:RemoteBashOperator",
        command=command,
        cluster=cluster,
        user=user,
        job_name=job_name,
        memory=memory,
        vcores=vcores,
        config=Config(configs=[EnvironmentConfigInstance()]),
        **_remove_unused_kwargs(kwargs)
    )
    task.pre_execute()
    task.execute()
