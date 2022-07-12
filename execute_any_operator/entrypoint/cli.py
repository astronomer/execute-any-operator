import click
from execute_any_operator.operators.execute_any import ExecuteAnyOperator


@click.group()
def cli():
    """Executes Airflow operator classes as Python objects without the need for running Airflow."""
    pass


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
    "-c", "--cwd", default=None, help="Working directory to execute the command in"
)
@click.argument("bash-command", required=True)
def bash_operator(env, output_encoding, cwd, bash_command):
    """Execute a Bash script, command or set of commands."""
    click.echo("Executing BashOperator")
    task = ExecuteAnyOperator(
        operator="BashOperator",
        bash_command=bash_command,
        env=env,
        output_encoding=output_encoding,
        cwd=cwd,
    )
    task.execute()


@click.command()
def hdfs_sensor():
    """Waits for a file or folder to land in HDFS."""
    click.echo("Executing HdfsSensor")


@click.command()
def hive_operator():
    """Executes hql code or hive script in a specific Hive database."""
    click.echo("Executing HiveOperator")


@click.command()
def kubernetes_pod_operator():
    """Execute a task in a Kubernetes Pod."""
    click.echo("Executing KubernetesPodOperator")
    task = ExecuteAnyOperator(
        operator="KubernetesPodOperator",
        namespace="default",
        image="alpine",
        cmds=[
            "sh",
            "-c",
            "mkdir -p /airflow/xcom/;echo '[1,2,3,4]' > /airflow/xcom/return.json",
        ],
        name="write-xcom",
        do_xcom_push=True,
        get_logs=True,
        in_cluster=False,
    )
    task.execute()


@click.command()
@click.option(
    "-a",
    "--op-args",
    default=None,
    multiple=True,
    help="A list of positional arguments that will get unpacked when calling your callable.",
)
@click.option(
    "-k",
    "--op-kwargs",
    default=None,
    multiple=True,
    help="A dictionary of keyword arguments that will get unpacked in your function.",
)
@click.argument("python-callable", required=True)
def python_operator(op_args, op_kwargs, python_callable):
    """Executes a Python callable."""
    click.echo("Executing PythonOperator")
    import importlib

    mod_name, func_name = python_callable.split(":")
    mod = importlib.import_module(mod_name)
    func = getattr(mod, func_name)
    op_kwargs = {k: v for k, v in map(lambda x: x.split("="), op_kwargs)}
    task = ExecuteAnyOperator(
        operator="PythonOperator",
        python_callable=func,
        op_args=op_args,
        op_kwargs=op_kwargs,
    )
    task.execute()


@click.command()
def s3_key_sensor():
    """Waits for a key (a file-like instance on S3) to be present in a S3 bucket.
    S3 being a key/value it does not support folders. The path is just a key
    a resource.
    """
    click.echo("Executing S3KeySensor")


cli.add_command(bash_operator)
cli.add_command(hdfs_sensor)
cli.add_command(hive_operator)
cli.add_command(kubernetes_pod_operator)
cli.add_command(python_operator)
cli.add_command(s3_key_sensor)

if __name__ == "__main__":
    cli()
