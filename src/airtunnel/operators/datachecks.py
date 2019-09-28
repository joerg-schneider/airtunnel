from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


@apply_defaults
class DataCheckMetricsOperator(BaseOperator):
    pass
