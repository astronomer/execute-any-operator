# Execute Any Operator CLI

The `execute-any-operator` command is used to arbitrarily run any supported Airflow operator without the need to run Airflow services.

> **Note**: The `requirements.txt` file is currently built to support extra operator libraries copied into the project directory (i.e. `remote_bash_operator` and `arrow_hdfs_sensor`). The lines in the `requirements.txt` for these two operators should be replaced with the proper library name that can be added with `pip install`.

## Requirements

1. Docker
2. Python 3

## Building the Docker Image

Building the Docker image is simple, just execute the following command in the root of the repository:

```bash
docker build -t execute-any-operator .
```

This will create a Docker image tagged with `execute-any-operator` which includes the CLI tool and other required code.

To change the image repository and tag add the following build arguments:

```bash
docker build -t execute-any-operator --build-arg BASE_IMAGE=<my-image-repo> --build-arg IMAGE_TAG=<my-image-tag>
```

## Executing Operators

To execute operators, we can run the docker image with the subcommand for the operator we want to execute. For example, this is how you would execute the `BashOperator`:

```bash
docker run --rm execute-any-operator bash-operator --env NAME World 'echo "Hello, ${NAME}!"'
```

## Providing Environment Variables

Some operators may require environment variables, or they may use `Variable.get` to get Airflow variables. Environment variables can also be used to provide Airflow connections. Here is an example of how to pass environment variables to an operator for an Airflow connection (i.e. `SimpleHttpOperator`):

```bash
docker run --rm execute-any-operator --env-var AIRFLOW_CONN_HTTP_DEFAULT http://example.com/https simple-http-operator --method GET
```

Another use case for environment variables is for operators that use `Variable.get`. Airflow typically will retrieve variables from the backend database. Since we are executing Airflow operators without a database, the `Variable` model has been patched to retrieve only from environment variables. For example, if I want to retrieve an Airflow variable called `my_var`, then I can provide an environment variable like this:

```bash
docker run --rm execute-any-operator --env-var my_var value <operator-subcommand>
```

## Adding New Operators

To add new operators to the CLI tool, all that's required is to create a new subcommand that initializes the `ExecuteAnyOperator` and calls `.execute()`. Another important piece to adding a new operator is to include all of the initialization parameters that you might need. Please refer to the example below:

### `ExampleOperator` Implementation

This example will walk through how to add new operators to the CLI tool. Consider the following example operator assumed to be in `airflow/operators/example.py`:

```python
# airflow/operators/example.py
from airflow.models.baseoperator import BaseOperator

class ExampleOperator(BaseOperator):
    """
    An example operator for explaining the execute-any-operator CLI.

    :param arg_one: Required first argument
    :type arg_one: str
    :param arg_two: Optional second argument
    :type arg_two: bool
    :param arg_three: Optional third argument
    :type arg_three: Dict[str, str]
    """
    def __init__(self, *, arg_one: str, arg_two: bool = False, arg_three: Dict[str, str] = None, **kwargs):
        super().__init__(**kwargs)
        self.arg_one = arg_one
        self.arg_two = arg_two
        self.arg_three = arg_three or {}

    def execute(self, context: Dict):
        print(self.arg_one)
        print(self.arg_two)
        print(self.arg_three)
```

Notice that there is one required argument (`arg_one`) and two optional arguments (`arg_two` and `arg_three`). This is important when defining the CLI entrypoint. Next we will create a new Python file in the entrypoint directory - `execute_any_operator/execute_any_operator/entrypoint/example_operator.py`. The content of the file will look like this:

```python
import click
from execute_any_operator.operators.execute_any import ExecuteAnyOperator
from execute_any_operator.utils.helpers import _multi_tuple_to_dict, _remove_unused_kwargs

@click.command()
@click.option("--arg-two", default=False, help="Optional second argument")
@click.option("--arg-three", default=None, multiple=True, type=click.Tuple([str, str]), callable=_multi_tuple_to_dict, help="Optional third argument")
@click.argument("arg-one", required=True)
def example_operator(arg_one, **kwargs):
    @click.echo("Executing ExampleOperator")
    task = ExecuteAnyOperator(
        operator="airflow.operators.example:ExampleOperator",
        arg_one=arg_one,
        **_remove_unused_kwargs(kwargs)
    )
    task.execute()
```

Let's take a look at this piece by piece. First, notice the imports:

```python
import click
from execute_any_operator.operators.execute_any import ExecuteAnyOperator
from execute_any_operator.utils.helpers import _multi_tuple_to_dict, _remove_unused_kwargs
```

Here we are importing the `click` library, which allows us to create CLI commands. Then we import the `ExecuteAnyOperator` which is used for executing the provided operator. And finally, we have some helper functions that are used to transform command arguments or values being passed to the operator.

The next important piece is the subcommand definition:

```python
@click.command()
@click.option("--arg-two", default=False, help="Optional second argument")
@click.option("--arg-three", default=None, multiple=True, type=click.Tuple([str, str]), callback=_multi_tuple_to_dict, help="Optional third argument")
@click.argument("arg-one", required=True)
def example_operator(arg_one, **kwargs):
    ...
```

First, we add the `@click.command()` decorator to tell the CLI tool that this is a command. Next, we add the command options and their defaults. Note that the options are named with `-` separating words instead of `_`. The click library will modify these at runtime and replace `-` with `_` when passing them to the subcommand method signature. Notice we also have a `@click.argument`, this is a required parameter that must be passed when calling the subcommand. Use `argument` when the operator has required parameters. Required parameters are provided to the method signature, and all options are being provided in the `**kwargs` parameter because click will pass them as keyword arguments.

Observe the `callback` that is being used in the `arg-three` option. `arg-three` is a dictionary parameter for the `ExampleOperator`. Because CLI tools only pass strings, we need to transform the values being passed with a callback. By setting `multiple=True` and `type=click.Tuple([str, str])`, we are telling the CLI library that the arguments can be provided multiple times and that there should be two strings provided. The `_multi_tuple_to_dict` callback will then transform the tuples to a dictionary, for example:

```bash
--arg-three key1 value1 --arg-three key2 value2  # arg_three = {"key1": "value1", "key2": "value2"}
```

Finally, initializing the `ExecuteAnyOperator` and calling execute will finish things off:

```python
task = ExecuteAnyOperator(
    operator="airflow.operators.example:ExampleOperator",
    arg_one=arg_one,
    **_remove_unused_kwargs(kwargs)
)
task.execute()
```

The first parameter for the `ExecuteAnyOperator` is the operator import path. In this example, the `ExampleOperator` exists at `airflow/operators/example.py`. So our operator parameter is `operator="airflow.operators.example:ExampleOperator"` which is a python dot notation path. Under the hood the `ExecuteAnyOperator` will use `importlib` to import the operator when initializing the task.

Next you will see the required parameter `arg_one` being provided. After that there is a function being called with the rest of the `kwargs`, `_remove_unused_kwargs`. This callable simply removes any `None` key, value pairs from `kwargs` in order to allow for the task to be initialized with the defaults of the operator definition.

Last but not least, `task.execute()` is called to execute the task. In order to register the new subcommand, we need to import the method in the entrypoint [__init__.py](execute_any_operator/execute_any_operator/entrypoint/__init__.py) and add it as a subcommand to the CLI group:

```python
from execute_any_operator.entrypoint.example_operator import example_operator
...
cli.add_command(example_operator)
```

## Retrieving XCom Data

Since this CLI tool is executing Airflow operators without running Airflow services, we are left with no place to store XCom data from the task execution. In order to resolve this issue, there is a custom XCom backend defined: [DictXComBackend](execute_any_operator/execute_any_operator/utils/dict_xcom_backend.py). This backend simply stores XCom data in a global dictionary. In order to retrieve XCom data, the `XComData` dictionary can be imported from the backend file.

```python
from execute_any_operator.utils.dict_xcom_backend import XComData
```

The pattern for data being stored in XCom is `XComData[dag_id][task_id][key]`. An example of printing XCom data can be found in the implementation of [SimpleHttpOperator](execute_any_operator/execute_any_operator/entrypoint/simple_http_operator.py).
