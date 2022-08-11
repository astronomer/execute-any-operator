
import click
from execute_any_operator.operators.execute_any import ExecuteAnyOperator
from execute_any_operator.utils.helpers import (_multi_tuple_to_dict,
                                                _remove_unused_kwargs)


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
@click.argument("python-callable", required=True)
def python_operator(python_callable, **kwargs):
    """Executes a Python callable."""
    click.echo("Executing PythonOperator")
    import importlib

    mod_name, func_name = python_callable.split(":")
    mod = importlib.import_module(mod_name)
    func = getattr(mod, func_name)
    task = ExecuteAnyOperator(
        operator="airflow.operators.python:PythonOperator",
        python_callable=func,
        **_remove_unused_kwargs(kwargs)
    )
    task.execute()
