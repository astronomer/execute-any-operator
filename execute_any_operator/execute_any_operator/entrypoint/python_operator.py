import click
from execute_any_operator.operators.execute_any import ExecuteAnyOperator
from execute_any_operator.utils.helpers import (
    _multi_tuple_to_dict,
    _remove_unused_kwargs,
    _str_to_callable,
)


@click.command()
@click.option(
    "-a",
    "--arguments",
    "op_args",
    default=None,
    multiple=True,
    help="A list of positional arguments that will get unpacked when calling your callable.",
)
@click.option(
    "-k",
    "--keyword-arguments",
    "op_kwargs",
    default=None,
    multiple=True,
    type=click.Tuple([str, str]),
    callback=_multi_tuple_to_dict,
    help="A dictionary of keyword arguments that will get unpacked in your function.",
)
@click.argument("python-callable", required=True, callback=_str_to_callable)
def python_operator(python_callable, **kwargs):
    """Executes a Python callable."""
    click.echo("Executing PythonOperator")
    task = ExecuteAnyOperator(
        operator="airflow.operators.python:PythonOperator",
        python_callable=python_callable,
        **_remove_unused_kwargs(kwargs)
    )
    task.execute()
