""" Module for Airtunnel's ingestion sensors. """
import os

from airflow.models import TaskInstance
from airflow.operators.sensors import BaseSensorOperator
from airflow.utils.decorators import apply_defaults

import airtunnel.data_store
import airtunnel.operators
from airtunnel.data_asset import BaseDataAsset

K_DISCOVERED_FILES = "discovered_input_files"


@apply_defaults
class SourceFileIsReadySensor(BaseSensorOperator):
    """
    Airtunnel's SourceFileIsReadySensor – for a given Airtunnel data asset of type ``ingested``, this sensor will
    leverage the declared input files glob pattern to sense for new input data.

    If files have been discovered, the sensor will capture their names and modification dates. Then it will
    check that for ``no_of_required_static_pokes`` times, file names and modification dates have not changed, before
    returning successfully.

    This guards against incomplete larger input files that are still being written into the landing area while this
    sensor finds them.
    """

    ui_color = airtunnel.operators.Colours.ingestion

    @apply_defaults
    def __init__(
        self,
        asset: BaseDataAsset,
        no_of_required_static_pokes: int = 2,
        poke_interval: int = 30,
        timeout: int = 60 * 15,
        **kwargs,
    ):

        if "task_id" not in kwargs:
            kwargs["task_id"] = asset.name + "_" + "source_is_ready"

        super().__init__(poke_interval=poke_interval, timeout=timeout, **kwargs)

        self._asset = asset
        self._no_of_required_static_pokes = no_of_required_static_pokes
        self._discovered_input_files = None
        self._search_glob = os.path.join(
            self._asset.landing_path, self._asset.declarations.ingest_file_glob
        )
        self._data_store_adapter = airtunnel.data_store.get_configured_data_store_adapter()

    def poke(self, context):
        """ Perform the poke operation for this sensor from Airflow. """
        if (
            self._discovered_input_files is not None
            and self._no_of_required_static_pokes <= 1
        ):
            # we have found files that remained static for enough iterations.
            # -->> push the found files which will expose them as an XCom payload
            # context.xcom.push self._discovered_input_files
            ti: TaskInstance = context["task_instance"]
            ti.xcom_push(
                key=self._asset.discovered_files_xcom_key,
                value=list(self._discovered_input_files.keys()),
            )
            return True

        elif self._discovered_input_files is not None:
            # we have not found files before

            # scan for matching files again:
            matching_files = self._matching_files()
            # get modification timestamps on all files:
            matching_files_w_time = self._data_store_adapter.modification_times(
                matching_files
            )
            # check if same as in previous probe:
            if matching_files_w_time == self._discovered_input_files:
                # decrement the remaining number of checks for the files to remain static:
                self._no_of_required_static_pokes = (
                    self._no_of_required_static_pokes - 1
                )
                self.log.info(
                    "Previously discovered files have not changed - "
                    f"poke another {self._no_of_required_static_pokes} times"
                )
            else:
                # files have changed since the last check - store the new list of relevant files
                self.log.info(
                    "Previously discovered files have changed - keep poking ..."
                )
                self._discovered_input_files = matching_files_w_time

        else:
            matching_files = self._matching_files()
            if len(matching_files) > 0:
                # capture found files and their modification timestamps:
                self.log.info(f"Found {len(matching_files)} source files to ingest")
                self._discovered_input_files = self._data_store_adapter.modification_times(
                    matching_files
                )
            else:
                self.log.info(f"No matching files at {self._search_glob}")

        # we need to poke for another iteration
        return False

    def _matching_files(self):
        return self._data_store_adapter.glob(pattern=self._search_glob)
