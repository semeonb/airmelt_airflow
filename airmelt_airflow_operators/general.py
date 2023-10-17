import logging
import re
from typing import Any
import os
from datetime import datetime, timedelta
import json
from google.cloud import bigquery
from airflow.models import BaseOperator, Variable
from airflow.utils.decorators import apply_defaults


def extract_date(text, use_default=True, days_offset=0):
    """
    Define a regex pattern to match the date format "YYYY-MM-DD"
    text: the text to be searched
    days_offset: the number of days to offset from the default date
    """
    date_pattern = r"\d{4}-\d{2}-\d{2}"

    # Search for the pattern in the input text
    match = re.search(date_pattern, text)

    # If a match is found, return the matched date
    if match:
        date_parsed = datetime.strptime(match.group(), "%Y-%m-%d") - timedelta(
            days=days_offset
        )
        return date_parsed.date()
    else:
        if use_default:
            date_parsed = datetime.today() - timedelta(days=days_offset)
            return date_parsed.date()
        else:
            return None


def get_var(
    var_name: str,
    default: Any = None,
    description: str = None,
    deserialize_json: bool = False,
):
    """Use to retrieve variables that can be overridden at run time.

    Parameters
    ------
    var_name : str
        The variable name as defined in Airflow ``Variable``.
    default : str, optional
        The default value to be used at run time, if an appropriate default
        exists. This has no effect if either an Airflow variable **or** env var already
        exists.
    Default value to set Description of the Variable
    deserialize_json: Deserialize the value to a Python dict
    alt_str_sep : str, optional
        Variables can alternatively use ``alt_str_sep`` to separate namepace from variable
        name instead of ``.``. The ``.`` separator takes precedence. Defaults to `__` and
        does a simple string replace to substitute out the ``.`` in a given variable name
        (so don't include another ``.`` in the name!)
    """
    if var_name == "ENV_TYPE":
        return os.getenv("ENV_TYPE")
    if var_name in os.environ:
        default_value = os.environ.get(var_name, default=default)
    else:
        default_value = default
    if default is None:
        return Variable.get(
            var_name,
            deserialize_json=deserialize_json,
        )
    return Variable.setdefault(
        var_name,
        default=default_value,
        description=description,
        deserialize_json=deserialize_json,
    )


def _get_schema(schema_file) -> dict:
    """
    The _get_schema function takes the path to a JSON schema file as input and returns a json object that contains all columns needed for the BigQuery table.

    schema_file: the path of the JSON file containing the schema.
    """
    logger = logging.getLogger(__name__)
    try:
        with open(schema_file) as schema_data:
            obj = json.load(schema_data)
            for i in obj:
                if "description" not in i:
                    i["description"] = ""
            return obj
    except FileNotFoundError as e:
        logger.error(f"Error: {schema_file} not found")
        raise e
    except Exception as e:
        logger.error(f"Error reading {schema_file}")
        raise e
    finally:
        schema_data.close()


def generate_bq_schema(schema_dict: dict):
    # Generate a BigQuery schema from dictionary
    # Initialize an empty list to hold the generated schema fields
    schema = []

    # Loop through each object in the JSON schema
    for i in schema_dict:
        # Ensure 'description' attribute exists, set to empty string if missing
        if "description" not in i:
            i["description"] = ""

        # Ensure 'mode' attribute exists, set to 'NULLABLE' if missing
        if "mode" not in i:
            i["mode"] = "NULLABLE"

        # Check if the field type is not 'RECORD'
        if not i.get("type") == "RECORD":
            # Append a SchemaField for non-nested fields
            schema.append(
                bigquery.SchemaField(
                    name=i.get("name"),
                    field_type=i.get("type"),
                    mode=i.get("mode"),
                    description=i.get("description"),
                )
            )
        else:
            # Initialize an empty list for nested fields
            nested_schema = []

            # Loop through nested fields
            for k in i["fields"]:
                # Ensure 'description' attribute exists, set to empty string if missing
                if "description" not in k:
                    k["description"] = ""

                # Append a SchemaField for nested fields
                nested_schema.append(
                    bigquery.SchemaField(
                        name=k.get("name"),
                        field_type=k.get("type"),
                        mode=k.get("mode"),
                        description=k.get("description"),
                    )
                )

            # Append a SchemaField for the entire nested structure
            schema.append(
                bigquery.SchemaField(
                    name=i.get("name"),
                    field_type=i.get("type"),
                    mode=i.get("mode"),
                    description=i.get("description"),
                    fields=(nested_schema),
                )
            )

    # Return the generated schema
    return schema


def gen_bq_dataset_table(project_id, destination_table_id, partition: datetime = None):
    """
    The gen_bq_dataset_table function takes the project_id, destination_table_id and partition as input
    and returns the full table name in the format project_id.dataset_id.table_id$partition.
    """
    if partition:
        destination_table_id = destination_table_id + "$" + partition.strftime("%Y%m%d")
    return f"{project_id}.{destination_table_id}"


class SuccessOperator(BaseOperator):
    @apply_defaults
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def execute(self, context):
        pass


class DagStartOperator(BaseOperator):
    @apply_defaults
    def __init__(self, arguments_to_log={}, *args, **kwargs):
        self.arguments_to_log = arguments_to_log
        super().__init__(*args, **kwargs)

    def execute(self, context):
        logger = logging.getLogger(__name__)
        logger.info("DAG execution started")ß
        for key, value in self.arguments_to_log.items():
            logger.info(f"{key}: {value}")


class GSFile(object):
    def __init__(self, gs_path: str, sharded: bool = False, file_format="json") -> None:
        dt = datetime.utcnow().strftime("%Y%m%d%H%M%S")
        if sharded:
            filename = "{gs_path}/{dt}/file".format(gs_path=gs_path, dt=dt)
            self.name = filename + "{}"
            self.gs_source = [filename + "*"]
        else:
            filename = "{gs_path}/{dt}/file".format(gs_path=gs_path, dt=dt)
            self.name = filename
            self.gs_source = [filename + "." + file_format]
        self.full_name = self.name + "." + file_format
        self.path = "{gs_path}/{dt}".format(gs_path=gs_path, dt=dt)
