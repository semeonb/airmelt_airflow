from os import PathLike
from typing import Any
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
    __________

    group_id: str name of the task group
    dbt_project_path: PathLike path to the dbt project folder, for example {os.environ['AIRFLOW_HOME']}/dags/dbt/{profile_name}
    dbt_model_path: PathLike path to the dbt model folder, for example "{os.environ['AIRFLOW_HOME']}/dags/dbt/codere_core/models/{DBT_MODEL}"
    dbt_profile_name: str name of the dbt profile
    target_name: str name of the dbt target, usually the environment name (dev, prod, etc.)
    connection_id: str name of the Airflow connection that contains the GCP credentials
    project_id: str name of the GCP project
    dataset: str name of the BigQuery target dataset
    keyfile: str path to the GCP service account key file
    location: str location of the BigQuery dataset, default is "US"
    method: str method of authentication, default is "service-account", other options are "oauth" and "application-default"
    """

    def __init__(
        self,
        group_id: str = "dbt_task_group",
        dbt_project_path: PathLike = None,
        dbt_model_path: PathLike = None,
        dbt_profile_name: str = None,
        target_name: str = None,
        connection_id: str = None,
        project_id: str = None,
        dataset: str = None,
        keyfile: str = None,
        location: str = "US",
        method: str = "service-account",
        *args: Any,
        **kwargs: Any
    ) -> None:
        super().__init__(group_id, *args, **kwargs)
        profile_mapping = GoogleCloudOauthProfileMapping(
            conn_id=connection_id,
            profile_args={
                "project": project_id,
                "dataset": dataset,
                "location": location,
                "method": method,
                "keyfile": keyfile,
            },
        )
        profile_config = ProfileConfig(
            profile_name=dbt_profile_name,
            target_name=target_name,
            profile_mapping=profile_mapping,
        )
        execution_config = ExecutionConfig(threads=1)
        self.project_config = ProjectConfig(dbt_project_path)
        self.profile_config = profile_config
        self.execution_config = execution_config
        self.render_config = RenderConfig(select=["path:{}".format(dbt_model_path)])


# DbtTaskGroup(
#         group_id="run_dbt",
#         project_config=ProjectConfig(get_dbt_project_path(DBT_PROFILE_NAME)),
#         profile_config=profile_config,
#         execution_config=execution_config,
#         render_config=RenderConfig(select=["path:{}".format(MODEL_PATH)]),
#         )
