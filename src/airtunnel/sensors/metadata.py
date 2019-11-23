""" Module for Airtunnel's metadata sensors. """

from datetime import timedelta, datetime
from typing import Optional, Iterable

from airflow.operators.sensors import BaseSensorOperator
from airflow.utils.decorators import apply_defaults

import airtunnel.operators
from airtunnel import BaseDataAsset
from airtunnel.metadata.adapter import get_configured_meta_adapter


@apply_defaults
class AwaitLoadStatusSensor(BaseSensorOperator):
    """ Airtunnel's AwaitLoadStatusSensor – checks the data asset load status metadata for a given condition."""

    ui_color = airtunnel.operators.Colours.ingestion

    @apply_defaults
    def __init__(
        self,
        asset: BaseDataAsset,
        poke_interval: int = 30,
        timeout: int = 60 * 15,
        refreshed_within: timedelta = None,
        refreshed_after: datetime = None,
        **kwargs,
    ):
        if "task_id" not in kwargs:
            kwargs["task_id"] = asset.name + "_" + "await_load_status"

        self._asset = asset

        if refreshed_after is None and refreshed_within is None:
            raise ValueError(
                "One of 'refreshed_within' or 'refreshed_after' should be supplied!"
            )

        if refreshed_within is None:
            self._refreshed_within = datetime.now() - timedelta(days=365 * 10)
        else:
            self._refreshed_within = datetime.now() - refreshed_within

        if refreshed_after is None:
            self._refreshed_after = datetime.now() - timedelta(days=365 * 10)
        else:
            self._refreshed_after = refreshed_after

        self._compare_date = None
        self._meta_adapter = None

        super().__init__(poke_interval=poke_interval, timeout=timeout, **kwargs)

    def poke(self, context):
        """ Perform the poke operation for this sensor from Airflow. """
        if self._meta_adapter is None:
            self._meta_adapter = get_configured_meta_adapter()

        if self._compare_date is None:
            # we pick the more recent date as a comparison:
            if self._refreshed_within > self._refreshed_after:
                self._compare_date = self._refreshed_within
            else:
                self._compare_date = self._refreshed_after

            self.log.info(f"Poking for a load status after: {self._compare_date}")

        current_load_status = self._meta_adapter.read_load_status(for_asset=self._asset)

        if (
            current_load_status is not None
            and current_load_status.load_time > self._compare_date
        ):
            return True

        self.log.info(
            f"Current load status of {current_load_status.load_time} is not recent enough."
        )
        return False


@apply_defaults
class AwaitAssetAncestorsUpdatedSensor(BaseSensorOperator):
    """ Airtunnel's AwaitAssetAncestorsUpdatedSensor – using lineage for a given data asset, probes until all
        ancestors have been updated. (i.e. load status timestamp of all ancestors is "recent enough")
    """

    ui_color = airtunnel.operators.Colours.ingestion

    @apply_defaults
    def __init__(
        self,
        asset: BaseDataAsset,
        ignore_ancestors: Optional[Iterable[BaseDataAsset]] = (),
        ancestors_refreshed_within: timedelta = None,
        include_upstream_levels: Iterable[int] = (0,),
        poke_interval: int = 30,
        timeout: int = 60 * 15,
        **kwargs,
    ):
        if "task_id" not in kwargs:
            kwargs["task_id"] = asset.name + "_" + "await_ancestors_updated"

        self._asset = asset
        self._ignore_ancestors = ignore_ancestors
        self._meta_adapter = None
        self._include_upstream_levels = include_upstream_levels
        self._ancestors_refreshed_within = ancestors_refreshed_within
        self._refreshed_since = None

        super().__init__(poke_interval=poke_interval, timeout=timeout, **kwargs)

    def poke(self, context):
        """ Perform the poke operation for this sensor from Airflow. """
        if self._meta_adapter is None:
            self._meta_adapter = get_configured_meta_adapter()

        if self._refreshed_since is None:
            if self._ancestors_refreshed_within is None:
                self._refreshed_since = datetime.now() - timedelta(days=365 * 10)
            else:
                self._refreshed_since = (
                    datetime.now() - self._ancestors_refreshed_within
                )

            self.log.info(
                f"Including ancestors refreshed since: {self._refreshed_since}"
            )

        lineage = self._meta_adapter.read_lineage(for_target=self._asset)
        for ancestor, upstream_level in lineage:
            if upstream_level in self._include_upstream_levels:
                if ancestor.data_target not in self._ignore_ancestors:
                    self.log.info(
                        f"Checking load status of ancestor: {ancestor.data_target.name}"
                    )
                    load_status_ancestor = self._meta_adapter.read_load_status(
                        for_asset=ancestor.data_target
                    )

                    if load_status_ancestor.load_time > self._refreshed_since:
                        self.log.info(f"Load time is in the specified time-range.")
                    else:
                        self.log.info(f"Load time is NOT in the specified time-range.")
                        return False

        return True
