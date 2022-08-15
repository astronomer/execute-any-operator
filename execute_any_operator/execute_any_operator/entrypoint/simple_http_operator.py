import click
from execute_any_operator.operators.execute_any import ExecuteAnyOperator
from execute_any_operator.utils.dict_xcom_backend import XComData
from execute_any_operator.utils.helpers import (
    _multi_tuple_to_dict,
    _remove_unused_kwargs,
    _str_to_callable,
)

# endpoint: Optional[str] = None,
# method: str = 'POST',
# data: Any = None,
# headers: Optional[Dict[str, str]] = None,
# response_check: Optional[Callable[..., bool]] = None,
# response_filter: Optional[Callable[..., Any]] = None,
# extra_options: Optional[Dict[str, Any]] = None,
# http_conn_id: str = 'http_default',
# log_response: bool = False,
# auth_type: Type[AuthBase] = HTTPBasicAuth,


@click.command()
@click.option("--endpoint", default=None, help="The relative part of the full url.")
@click.option("--method", default="POST", help="The HTTP method to use.")
@click.option(
    "--data",
    default=None,
    multiple=True,
    type=click.Tuple([str, str]),
    callback=_multi_tuple_to_dict,
    help="The data to pass. POST-data in POST/PUT and params in the URL for a GET request.",
)
@click.option(
    "--headers",
    default=None,
    multiple=True,
    type=click.Tuple([str, str]),
    callback=_multi_tuple_to_dict,
    help="The HTTP headers to be added to the request.",
)
@click.option(
    "--response-check",
    default=None,
    callback=_str_to_callable,
    help="""A check against the 'requests' response object.
The callable takes the response object as the first positional argument
and optionally any number of keyword arguments available in the context dictionary.
It should return True for 'pass' and False otherwise.""",
)
@click.option(
    "--response-filter",
    default=None,
    callback=_str_to_callable,
    help="""A function allowing you to manipulate the response
text. e.g response_filter=lambda response: json.loads(response.text).
The callable takes the response object as the first positional argument
and optionally any number of keyword arguments available in the context dictionary.""",
)
@click.option(
    "--extra-options", default=None, help="Extra options for the 'requests' library."
)
@click.option(
    "--http-conn-id",
    default="http_default",
    help="The http connection to run the operator against",
)
@click.option("--log-response", default=False)
# @click.option("--auth-type", default=None, help="The auth type for the service.")
def simple_http_operator(**kwargs):
    click.echo("Executing SimpleHttpOperator")
    task = ExecuteAnyOperator(
        operator="airflow.providers.http.operators.http:SimpleHttpOperator",
        **_remove_unused_kwargs(kwargs)
    )
    task.execute()
    print("======= XCOM DATA =======")
    print(XComData)
