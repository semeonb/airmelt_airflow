from cosmos import (
    DbtTaskGroup,
    ProjectConfig,
    ProfileConfig,
    ExecutionConfig,
    RenderConfig,
)
from cosmos.profiles import GoogleCloudOauthProfileMapping


class AirflowDbtTaskGroup(DbtTaskGroup):
    """
    The AirflowDbtTaskGroup is a subclass of the DbtTaskGroup that is used to run dbt models in Airflow.

    Parameters
    ----------
    group_id : str, optional
        Name of the task group, by default "dbt_task_group"
    dbt_project_path : PathLike, optional
        Path to the dbt project folder, for example {os.environ['AIRFLOW_HOME']}/dags/dbt/{profile_name}
    dbt_model_path : PathLike, optional
        Path to the dbt model folder, for example "{os.environ['AIRFLOW_HOME']}/dags/dbt/codere_core/models/{DBT_MODEL}"
    dbt_profile_name : str, optional
        Name of the dbt profile
    target_name : str, optional
        Name of the dbt target, usually the environment name (dev, prod, etc.)
    connection_id : str, optional
        Name of the Airflow connection that contains the GCP credentials
    project_id : str, optional
        Name of the GCP project
    dataset : str, optional
        Name of the BigQuery target dataset
    keyfile : str, optional
        Path to the GCP service account key file
    location : str, optional
        Location of the BigQuery dataset, default is "US"
    method : str, optional
        Method of authentication, default is "service-account", other options are "oauth" and "application-default"
    threads : int, optional
        Number of threads to use for dbt execution, default is 1
    dbt_executable_path : PathLike, optional
        Path to the dbt executable, for example {os.environ['AIRFLOW_HOME']}/dbt_venv/bin/dbt
    vars : str, optional
        DBT variables to pass to the dbt command in format '{"key":"value"}' default is None
    """

    def __init__(
        self,
        group_id="dbt_task_group",
        dbt_project_path=None,
        dbt_model_path=None,
        dbt_profile_name=None,
        target_name=None,
        connection_id=None,
        project_id=None,
        dataset=None,
        keyfile=None,
        location="US",
        method="service-account",
        threads=1,
        dbt_executable_path=None,
        *args,
        **kwargs,
    ):
        super().__init__(
            group_id,
            profile_config=ProfileConfig(
                profile_name=dbt_profile_name,
                target_name=target_name,
                profile_mapping=GoogleCloudOauthProfileMapping(
                    conn_id=connection_id,
                    profile_args={
                        "project": project_id,
                        "dataset": dataset,
                        "location": location,
                        "method": method,
                        "keyfile": keyfile,
                        "threads": threads,
                    },
                ),
            ),
            project_config=ProjectConfig(dbt_project_path),
            execution_config=ExecutionConfig(dbt_executable_path=dbt_executable_path),
            render_config=RenderConfig(select=["path:{}".format(dbt_model_path)]),
            *args,
            **kwargs,
        )
