import os
import shutil

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

import airtunnel
from airtunnel.data_asset import BaseDataAsset


@apply_defaults
class IngestArchiveOperator(BaseOperator):
    """ Simply move from ingest/landing to ingest/archive """
    ui_color = airtunnel.operators.Colours.archival
    @apply_defaults
    def __init__(self, asset: BaseDataAsset, *args, **kwargs):
        self._asset = asset

        if "task_id" not in kwargs:
            kwargs["task_id"] = asset.name + "_" + "ingest_archival"

        super().__init__(*args, **kwargs)

    def execute(self, context):
        os.makedirs(self._asset.ingest_archive_path(context), exist_ok=True)
        shutil.move(
            self._asset.staging_pickedup_path(context),
            self._asset.ingest_archive_path(context),
        )
        self.log.info(
            f"Moved landing data to archive path: {self._asset.ingest_archive_path }"
        )


@apply_defaults
class DataAssetArchiveOperator(BaseOperator):
    """ Prepares a new archive for a data asset and returns the name of the folder."""
    ui_color = airtunnel.operators.Colours.archival
    @apply_defaults
    def __init__(self, asset: BaseDataAsset, *args, **kwargs):
        self._asset = asset

        if "task_id" not in kwargs:
            kwargs["task_id"] = asset.name + "_" + "ready_archival"

        super().__init__(*args, **kwargs)

    def execute(self, context):
        if not os.path.exists(self._asset.ready_path):
            self.log.info(
                f"Nothing to archive, {self._asset.ready_path} does not exist!"
            )
            return

        shutil.copytree(self._asset.ready_path, self._asset.ready_archive_path(context))
        self.log.info(
            f"Copied archived version to path: {self._asset.ready_archive_path(context)}"
        )
