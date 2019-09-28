from airflow.operators.sensors import BaseSensorOperator
from airflow.utils.decorators import apply_defaults


@apply_defaults
class AwaitLoadStatusSensor(BaseSensorOperator):
    """ Checks airtunnel's data asset load status metadata for a given condition."""

    pass


@apply_defaults
class AwaitAssetAncestorsUpdatedSensor(BaseSensorOperator):
    # parm: ignore assets(list)

    """ Using lineage for a given data asset, probes until all ancestors have been updated.
        (i.e. load status timestamp of all ancestors is more recent)
    """
    pass
