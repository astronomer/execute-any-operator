
import click


@click.command()
def hive_operator():
    """Executes hql code or hive script in a specific Hive database."""
    click.echo("Executing HiveOperator")
