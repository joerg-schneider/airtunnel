from typing import Optional, Iterable

from airflow.operators.sensors import BaseSensorOperator
from airflow.utils.decorators import apply_defaults

import airtunnel.operators
from airtunnel import BaseDataAsset


@apply_defaults
class AwaitLoadStatusSensor(BaseSensorOperator):
    """ Checks airtunnel's data asset load status metadata for a given condition."""

    ui_color = airtunnel.operators.Colours.ingestion

    @apply_defaults
    def __init__(
        self,
        asset: BaseDataAsset,
        poke_interval: int = 30,
        timeout: int = 60 * 15,
        **kwargs,
    ):
        if "task_id" not in kwargs:
            kwargs["task_id"] = asset.name + "_" + "await_load_status"

        super().__init__(poke_interval=poke_interval, timeout=timeout, **kwargs)

    def poke(self, context):
        pass


@apply_defaults
class AwaitAssetAncestorsUpdatedSensor(BaseSensorOperator):

    """ Using lineage for a given data asset, probes until all ancestors have been updated.
        (i.e. load status timestamp of all ancestors is more recent)
    """

    ui_color = airtunnel.operators.Colours.ingestion

    @apply_defaults
    def __init__(
        self,
        asset: BaseDataAsset,
        ignore_assets: Optional[Iterable[BaseDataAsset]],
        poke_interval: int = 30,
        timeout: int = 60 * 15,
        **kwargs,
    ):
        if "task_id" not in kwargs:
            kwargs["task_id"] = asset.name + "_" + "await_ancestors_updated"

        super().__init__(poke_interval=poke_interval, timeout=timeout, **kwargs)

    def poke(self, context):
        pass
