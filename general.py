from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class SuccessOperator(BaseOperator):
    @apply_defaults
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def execute(self, context):
        pass
