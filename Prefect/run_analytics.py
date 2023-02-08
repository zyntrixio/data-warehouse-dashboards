from prefect import flow, task
from prefect.blocks.system import Secret, String
from prefect_airbyte.connections import trigger_sync
from prefect_dask.task_runners import DaskTaskRunner
from prefect_dbt.cli.commands import trigger_dbt_cli_command
from prefect_dbt.cli.configs import SnowflakeTargetConfigs
from prefect_dbt.cli.credentials import DbtCliProfile
from prefect_snowflake.credentials import SnowflakeCredentials
from prefect_snowflake.database import SnowflakeConnector


@task
def get_dbt_cli_profile(env):
    dbt_connector = SnowflakeConnector(
        schema="BINK",
        database={"dev": "DEV", "prod": "BINK"}[env],
        warehouse="ENGINEERING",
        credentials=SnowflakeCredentials.load("snowflake-transform-user"),
    )
    dbt_cli_profile = DbtCliProfile(
        name="Bink",
        target="target",
        target_configs=SnowflakeTargetConfigs(connector=dbt_connector),
    )
    return dbt_cli_profile


def dbt_cli_task(dbt_cli_profile, command):
    return trigger_dbt_cli_command(
        command=command,
        overwrite_profiles=True,
        profiles_dir=f"/opt/github.com/binkhq/data-warehouse-dashboards/Prefect",
        project_dir=f"/opt/github.com/binkhq/data-warehouse-dashboards/Bink",
        dbt_cli_profile=dbt_cli_profile,
    )


@flow(name="Analytics_Flow")
def run(
    env: str,
    is_run_transformations: bool = True,
):
    dbt_cli_profile = get_dbt_cli_profile(env)
    dbt_cli_task(dbt_cli_profile, "dbt deps")
    if is_run_transformations:
        dbt_cli_task(dbt_cli_profile, "dbt run")
