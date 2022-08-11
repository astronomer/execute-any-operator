import click
from execute_any_operator.operators.execute_any import ExecuteAnyOperator
from execute_any_operator.utils.dict_xcom_backend import XComData
from execute_any_operator.utils.helpers import _remove_unused_kwargs

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
@click.option("--endpoint", default=None)
@click.option("--method", default='POST')
@click.option("--data", default=None)
@click.option("--headers", default=None)
@click.option("--response-check", default=None)
@click.option("--response-filter", default=None)
@click.option("--extra-options", default=None)
@click.option("--http-conn-id", default='http_default')
@click.option("--log-response", default=False)
# @click.option("--auth-type", default=HTTPBasicAuth)
def simple_http_operator(endpoint, **kwargs):
    click.echo("Executing SimpleHttpOperator")
    task = ExecuteAnyOperator(
        operator="airflow.providers.http.operators.http:SimpleHttpOperator",
        endpoint=endpoint,
        **_remove_unused_kwargs(kwargs)
    )
    task.execute()
    print("======= XCOM DATA =======")
    print(XComData)
