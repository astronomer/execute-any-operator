from typing import Any, Dict, List, Tuple

import click
from execute_any_operator.operators.execute_any import ExecuteAnyOperator


def _multi_tuple_to_dict(ctx, param, value: List[Tuple[str, Any]]) -> Dict[str, Any]:
    if value is not None:
        return {k: v for k, v in value}
    return {}


def _remove_unused_kwargs(kwargs) -> Dict[str, Any]:
    return {k: v for k, v in kwargs.items() if v is not None}


@click.group()
@click.version_option()
def cli():
    """Executes Airflow operator classes as Python objects without the need for running Airflow."""
    pass


@click.command()
@click.option(
    "-e",
    "--env",
    default=None,
    multiple=True,
    type=click.Tuple([str, str]),
    callback=_multi_tuple_to_dict,
    help="Environment variables for the bash script execution",
)
@click.option(
    "-o", "--output-encoding", default=None, help="Output encoding of bash command"
)
@click.option(
    "-w", "--working-director", "cwd", default=None, help="Working directory to execute the command in"
)
@click.argument("bash-command", required=True)
def bash_operator(bash_command, **kwargs):
    """Execute a Bash script, command or set of commands."""
    click.echo("Executing BashOperator")
    task = ExecuteAnyOperator(
        operator="BashOperator",
        bash_command=bash_command,
        **_remove_unused_kwargs(kwargs)
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
@click.option("-n", "--namespace", default=None, help="")
@click.option("-i", "--image", default=None, help="")
@click.option("-N", "--name", default=None, help="")
@click.option("-c", "--commands", "cmds", default=None, multiple=True, help="")
@click.option("-a", "--arguments", default=None, multiple=True, help="")
@click.option("-p", "--ports", default=None, help="")
@click.option("-m", "--volume-mounts", default=None, help="")
@click.option("-v", "--volumes", default=None, help="")
@click.option("-E", "--env-vars", default=None, help="")
@click.option("-e", "--env-from", default=None, help="")
@click.option("-s", "--secrets", default=None, help="")
@click.option("--in-cluster", default=False, help="")
@click.option("--cluster-context", default=None, help="")
@click.option("-l", "--labels", default=None, help="")
@click.option("--startup-timeout-seconds", default=120, help="")
@click.option("--image-pull-policy", default=None, help="")
@click.option("-A", "--annotations", default=None, help="")
@click.option("-r", "--resources", default=None, help="")
@click.option("--affinity", default=None, help="")
@click.option("-C", "--config-file", default=None, help="")
@click.option("--node-selectors", default=None, help="")
@click.option("--node-selector", default=None, help="")
@click.option("--image-pull-secrets", default=None, help="")
@click.option("--service-account-name", default=None, help="")
@click.option("--is-delete-operator-pod", default=True, help="")
@click.option("--tolerations", default=None, help="")
@click.option("--security-context", default=None, help="")
@click.option("--dnspolicy", default=None, help="")
@click.option("--log-events-on-failure", default=False, help="")
@click.option("--do-xcom-push", default=False, help="")
@click.option("--pod-template-file", default=None, help="")
@click.option("--priority-class-name", default=None, help="")
@click.option("--pod-runtime-info-envs", default=None, help="")
@click.option("--termination-grace-period", default=None, help="")
@click.option("--configmaps", default=None, help="")
def kubernetes_pod_operator(**kwargs):
    """Execute a task in a Kubernetes Pod."""
    click.echo("Executing KubernetesPodOperator")
    print(kwargs)
    task = ExecuteAnyOperator(
        operator="KubernetesPodOperator",
        **_remove_unused_kwargs(kwargs)
    )
    task.execute()


@click.command()
@click.option(
    "-a",
    "--arguments",
    "op-args",
    default=None,
    multiple=True,
    help="A list of positional arguments that will get unpacked when calling your callable.",
)
@click.option(
    "-k",
    "--keyword-arguments",
    "op-kwargs",
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
        operator="PythonOperator",
        python_callable=func,
        **_remove_unused_kwargs(kwargs)
    )
    task.execute()


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
@click.option("--config", default=None, help="")
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
@click.argument("job_name", required=True)
@click.argument("memory", required=True)
@click.argument("vcores", required=True)
def remote_bash_operator(command, cluster, user, job_name, memory, vcores, **kwargs):
    """Execute a Bash script, command or set of commands."""
    click.echo("Executing BashOperator")
    task = ExecuteAnyOperator(
        operator="BashOperator",
        # bash_command=bash_command,
        # env=env,
        # output_encoding=output_encoding,
        # cwd=cwd,
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
