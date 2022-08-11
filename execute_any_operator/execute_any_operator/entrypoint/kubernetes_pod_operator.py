
import click
from execute_any_operator.operators.execute_any import ExecuteAnyOperator
from execute_any_operator.utils.helpers import _remove_unused_kwargs


@click.command()
@click.option("-n", "--namespace", default=None, help="")
@click.option("-i", "--image", default=None, help="")
@click.option("-N", "--name", default=None, help="")
# @click.option("--random-name-suffix", default=True, help="")
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
    task = ExecuteAnyOperator(
        operator="KubernetesPodOperator",
        **_remove_unused_kwargs(kwargs)
    )
    task.execute()