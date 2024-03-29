import click
from execute_any_operator.operators.execute_any import ExecuteAnyOperator
from execute_any_operator.utils.helpers import (
    _multi_tuple_to_dict,
    _remove_unused_kwargs,
)


@click.command()
@click.option(
    "-e",
    "--env",
    default=None,
    multiple=True,
    type=click.Tuple([str, str]),
    callback=_multi_tuple_to_dict,
    help="""If env is not None, it must be a dict that defines the
environment variables for the new process; these are used instead
of inheriting the current process environment, which is the default
behavior.""",
)
@click.option(
    "-o", "--output-encoding", default=None, help="Output encoding of bash command."
)
@click.option(
    "-w",
    "--working-director",
    "cwd",
    default=None,
    help="""Working directory to execute the command in.
If None (default), the command is run in a temporary directory.""",
)
@click.argument("bash-command", required=True)
def bash_operator(bash_command, **kwargs):
    """Execute a Bash script, command or set of commands."""
    click.echo("Executing BashOperator")
    task = ExecuteAnyOperator(
        operator="airflow.operators.bash:BashOperator",
        bash_command=bash_command,
        **_remove_unused_kwargs(kwargs)
    )
    task.execute()
