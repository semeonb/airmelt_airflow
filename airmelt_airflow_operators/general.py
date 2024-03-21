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
        logger.info("Printing arguments:")
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


class DatesListFromRange(BaseOperator):
    """
    Returns a list of dates between date_from and date_to

    ``date_from`` is a templated variable for this operator
    ``date_to`` is a templated variable for this operator
    ``cap`` is a templated variable for this operator

    Parameters
    ----------
    date_from: str, required
        The start date
    date_to: str, required
        The end date
    cap: int, optional
        The maximum number of dates to return
    """

    template_fields = ["date_from", "date_to", "cap"]

    def __init__(self, date_from, date_to, cap=None, *args, **kwargs):
        super(DatesListFromRange, self).__init__(*args, **kwargs)
        self.date_from = date_from
        self.date_to = date_to
        self.cap = cap

    def execute(self, context):
        date_range = []
        counter = 0
        # Trying to extract the full date from the string
        self.date_from = datetime.strptime(self.date_from, "%Y-%m-%d")
        self.date_to = datetime.strptime(self.date_to, "%Y-%m-%d")
        while self.date_from <= self.date_to:
            if self.cap and counter == self.cap:
                break
            date_range.append(self.date_from.strftime("%Y-%m-%d"))
            self.date_from += timedelta(days=1)
            counter += 1
        return date_range


class ExtendedLastExecutionDate(BaseOperator):
    """
    The class renders last execution date, optionally substracts lookback and returns the date

    ``prev_start_date_success`` is a templated variable for this operator
    ``lookback_days`` is a templated variable for this operator

    Parameters
    ----------
    prev_start_date_success: datetime, required
        The last successful start date
    lookback_days: int, optional
        The number of days to substract from the last successful start date
    """

    template_fields = ["prev_start_date_success", "lookback_days"]

    def __init__(self, prev_start_date_success, lookback_days=0, *args, **kwargs):
        super(ExtendedLastExecutionDate, self).__init__(*args, **kwargs)
        self.lookback_days = lookback_days
        self.prev_start_date_success = prev_start_date_success

    def execute(self, context):
        output_date: datetime = self.prev_start_date_success - timedelta(
            days=self.lookback_days
        )
        return output_date.strftime("%Y-%m-%d")


class DeleteFilesFromTemp(BaseOperator):
    """
    Deletes files from Airflow /tmp folder given prefix or list of files
    """

    template_fields = ["prefix", "files_list"]

    def __init__(
        self,
        files_list: str = None,
        prefix: str = None,
        temp_folder="tmp",
        *args,
        **kwargs,
    ):
        super(DeleteFilesFromTemp, self).__init__(*args, **kwargs)
        self.files_list = files_list
        self.prefix = prefix
        self.temp_folder = temp_folder

    def execute(self, context):
        if self.files_list:
            for file in self.files_list:
                if os.path.exists(file):
                    os.remove("/{}/{}".format(self.temp_folder, file))
                    self.log.info("Succesfully deleted file {}".format(file))
        elif self.prefix:
            for file in os.listdir("/{}".format(self.temp_folder)):
                if file.startswith(self.prefix):
                    os.remove("/{}/{}".format(self.temp_folder, file))
                    self.log.info("Succesfully deleted file {}".format(file))
        else:
            self.log.info("No files to delete")
