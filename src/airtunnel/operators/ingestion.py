""" Airtunnel operators for ingestion tasks. """
from datetime import datetime
from typing import List

from airflow.models import BaseOperator, TaskInstance
from airflow.utils.decorators import apply_defaults

import airtunnel.data_store
import airtunnel.metadata.adapter
import airtunnel.operators
from airtunnel.data_asset import BaseDataAsset
from airtunnel.metadata.adapter import BaseMetaAdapter
from airtunnel.metadata.entities import IngestedFileMetadata


@apply_defaults
class IngestOperator(BaseOperator):
    """ Airtunnel's ingestion operator. First inspects source files (size, create & modification dates)
     and logs metadata using Airtunnel's MetaAdapter. Then moves ("ingests") these files to the staging/pickedup
     directory for the data asset at hand.

     If the staging/pickedup directory of the data store is not empty (indicating a previous ingestion job has failed),
     this operator will also fail and report the problem, to avoid data loss.

     The list of files to inspect & ingest is retrieved using Airflow XCOM data,
     which a ``airtunnel.sensors.ingestion.SourceFileIsReadySensor`` is expected to have provided beforehand.

    """

    ui_color = airtunnel.operators.Colours.ingestion

    @apply_defaults
    def __init__(
        self,
        asset: BaseDataAsset,
        metadata_adapter: BaseMetaAdapter = None,
        *args,
        **kwargs,
    ):
        self._asset = asset
        self._meta_adapter = (
            metadata_adapter
            if metadata_adapter is not None
            else airtunnel.metadata.adapter.get_configured_adapter()
        )
        self._data_store_adapter = airtunnel.data_store.get_configured_adapter()

        self.picked_up_dir = None

        if "task_id" not in kwargs:
            kwargs["task_id"] = asset.name + "_" + "ingest"

        super().__init__(*args, **kwargs)

    def execute(self, context):
        """ Execute the operator from Airflow. """
        ti: TaskInstance = context["task_instance"]
        self.picked_up_dir = self._asset.staging_pickedup_path(context)

        discovered_files = ti.xcom_pull(
            dag_id=self.dag_id, key=self._asset.discovered_files_xcom_key
        )

        if len(discovered_files) == 0:
            raise FileNotFoundError("No files to ingest!")

        inspected_files = self.inspect_discovered_files(
            data_store_adapter=self._data_store_adapter,
            for_asset=self._asset,
            dag_id=self.dag_id,
            task_id=self.task_id,
            dag_exec_date=ti.execution_date,
            files=discovered_files,
        )

        # ensure pickedup directory exists (if i.e. very first run)
        self._data_store_adapter.makedirs(self.picked_up_dir, exist_ok=True)

        # ensure asset staging pickedup directory is empty
        if not self._check_pickedup_dir_is_empty():
            raise ValueError(
                f"The pickup directory {self.picked_up_dir} is not empty which should not occur."
                f"Please check for potentially stale files of previous loads and clean up manually."
            )

        self.log.info(f"Landing directory is:{self._asset.landing_path}")
        self.log.info(f"Asset staging picked-up directory is: {self.picked_up_dir}")

        # move previously discovered files to the pickedup directory for this asset:
        for f in inspected_files:
            # beware: naive implementation here:
            target_path = f.filepath.replace(
                self._asset.landing_path, self.picked_up_dir
            )
            self.log.info(f"Moving file {f.filepath} to {target_path}.")
            self._data_store_adapter.move(source=f.filepath, destination=target_path)

        self._meta_adapter.write_inspected_files(discovered_files=inspected_files)

    def _check_pickedup_dir_is_empty(self):
        """ We check if the staging/pickedup directory is empty for the data asset at hand, to avoid data loss."""
        return len(self._data_store_adapter.listdir(path=self.picked_up_dir)) == 0

    @staticmethod
    def inspect_discovered_files(
        data_store_adapter: airtunnel.data_store.BaseDataStoreAdapter,
        for_asset: BaseDataAsset,
        dag_id: str,
        task_id: str,
        dag_exec_date: datetime,
        files: List[str],
    ) -> List[IngestedFileMetadata]:
        """
        Inspect all previously discovered files paths using the ``inspect()`` method of the Airtunnel data store
        adapter.

        :param data_store_adapter: the Airtunnel data store adapter to use,
         i.e. ``airtunnel.data_store.LocalDataStoreAdapter``
        :param for_asset: data asset for which the data is inspected
        :param dag_id: Airflow DAG ID to associate the metadata with
        :param task_id: Airflow task ID to associate the metadata with
        :param dag_exec_date: Airflow DAG execution date time to associate the metadata with
        :param files: list of file paths to inspect
        :return: a list of ingested file metadata entities
        """

        return [
            IngestedFileMetadata(
                for_asset=for_asset,
                filepath=f,
                filesize=stats[2],
                file_create_time=stats[0],
                file_mod_time=stats[1],
                dag_id=dag_id,
                dag_exec_date=dag_exec_date,
                task_id=task_id,
            )
            for f, stats in data_store_adapter.inspect(files).items()
        ]
