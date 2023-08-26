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


def get_var(
    var_name: str,
    default: Any = None,
    description: str = None,
    deserialize_json: bool = False,
    alt_str_sep="__",
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
    if var_name in os.environ:
        default_value = os.environ.get(var_name, default=default)
    elif (alt_var_name := var_name.replace(".", alt_str_sep)) in os.environ:
        default_value = os.environ.get(alt_var_name, default=default)
    else:
        default_value = default
    if default is None:
        return Variable.get(
            var_name,
            default_var=default_value,
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
