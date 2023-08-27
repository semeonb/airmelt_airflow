import re
from typing import Any
import os
from datetime import datetime
from airflow.models import BaseOperator, Variable
from airflow.utils.decorators import apply_defaults


def extract_date(text):
    # Define a regex pattern to match the date format "YYYY-MM-DD"
    date_pattern = r"\d{4}-\d{2}-\d{2}"

    # Search for the pattern in the input text
    match = re.search(date_pattern, text)

    # If a match is found, return the matched date
    if match:
        return datetime.strptime(match.group(), "%Y-%m-%d")
    else:
        return None


def gen_file_name(filename, date=datetime.utcnow(), gen_ts_path=True):
    """
    Generate a file name with a timestamp by default.
    If ``gen_ts_path`` is set to False, one needs to be very careful about using this with sharded loads.
    """
    if gen_ts_path:
        return "{f1}/{dt}/{f2}".format(
            f1=filename,
            dt=date.strftime("%Y%m%d%H%M%S"),
            f2=filename,
        )
    else:
        return "{f1}/{f2}_{dt}".format(
            f1=filename,
            dt=date.strftime("%Y%m%d%H%M%S"),
            f2=filename,
        )


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


class SuccessOperator(BaseOperator):
    @apply_defaults
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def execute(self, context):
        pass
