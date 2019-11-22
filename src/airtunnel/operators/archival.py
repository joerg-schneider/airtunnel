""" Airtunnel operators for archival tasks. """
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

import airtunnel
import airtunnel.data_store
import airtunnel.operators
from airtunnel.data_asset import BaseDataAsset


@apply_defaults
class IngestArchiveOperator(BaseOperator):
    """ Simply move from ingest/landing to ingest/archive """

    ui_color = airtunnel.operators.Colours.archival

    @apply_defaults
    def __init__(self, asset: BaseDataAsset, *args, **kwargs):
        self._asset = asset
        self._data_store_adapter = airtunnel.data_store.get_configured_adapter()

        if "task_id" not in kwargs:
            kwargs["task_id"] = asset.name + "_" + "ingest_archival"

        super().__init__(*args, **kwargs)

    def execute(self, context):
        """ Executes the IngestArchiveOperator. """
        self._data_store_adapter.makedirs(
            path=self._asset.ingest_archive_path, exist_ok=True
        )
        self._data_store_adapter.move(
            source=self._asset.staging_pickedup_path(context),
            destination=self._asset.ingest_archive_path,
            recursive=True,
        )

        self.log.info(
            f"Moved landing data to archive path: {self._asset.ingest_archive_path}"
        )


@apply_defaults
class DataAssetArchiveOperator(BaseOperator):
    """ Prepares a new archive for a data asset and returns the name of the folder."""

    ui_color = airtunnel.operators.Colours.archival

    @apply_defaults
    def __init__(self, asset: BaseDataAsset, *args, **kwargs):
        self._asset = asset
        self._data_store_adapter = airtunnel.data_store.get_configured_adapter()

        if "task_id" not in kwargs:
            kwargs["task_id"] = asset.name + "_" + "ready_archival"

        super().__init__(*args, **kwargs)

    def execute(self, context):
        """ Executes the DataAssetArchiveOperator. """
        if not self._data_store_adapter.exists(self._asset.ready_path):
            self.log.info(
                f"Nothing to archive, {self._asset.ready_path} does not exist!"
            )
            return

        self._data_store_adapter.copy(
            source=self._asset.ready_path,
            destination=self._asset.ready_archive_path(context),
            recursive=True,
        )
        self.log.info(
            f"Copied archived version to path: {self._asset.ready_archive_path(context)}"
        )
