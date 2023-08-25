import re
from datetime import date, timedelta, datetime
from airflow.models import BaseOperator
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
    
class SuccessOperator(BaseOperator):
    @apply_defaults
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def execute(self, context):
        pass
