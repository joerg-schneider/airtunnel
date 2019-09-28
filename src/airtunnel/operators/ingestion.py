import os
from datetime import datetime
from typing import List

from airflow.models import BaseOperator, TaskInstance
from airflow.utils.decorators import apply_defaults

import airtunnel.operators
from airtunnel.data_asset import BaseDataAsset
from airtunnel.metadata.adapter import BaseMetaAdapter, SQLMetaAdapter
from airtunnel.metadata.entities import IngestedFileMetadata


@apply_defaults
class IngestOperator(BaseOperator):
    ui_color = airtunnel.operators.Colours.ingestion

    # inspects source files and collects metadata such as file format type, columns, etc.
    # writes to metadata db

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
            metadata_adapter if metadata_adapter is not None else SQLMetaAdapter()
        )

        self.picked_up_dir = None

        if "task_id" not in kwargs:
            kwargs["task_id"] = asset.name + "_" + "ingest"

        super().__init__(*args, **kwargs)

    def execute(self, context):
        ti: TaskInstance = context["task_instance"]
        self.picked_up_dir = self._asset.staging_pickedup_path(context)

        discovered_files = ti.xcom_pull(dag_id=self.dag_id, key=self._asset.discovered_files_xcom_key)

        if len(discovered_files) == 0:
            raise FileNotFoundError("No files to ingest!")

        inspected_files = self.inspect_discovered_files(
            for_asset=self._asset,
            dag_id=self.dag_id,
            task_id=self.task_id,
            dag_exec_date=ti.execution_date,
            files=discovered_files,
        )

        # ensure pickedup directory exists (if i.e. very first run)
        os.makedirs(self.picked_up_dir, exist_ok=True)

        # ensure asset staging pickedup directory is empty
        if not self._check_pickedup_dir_is_empty():
            raise ValueError(
                f"The pickup directory {self.picked_up_dir} is not empty which should not occur."
                f"Please check for potentially stale files of previous loads and clean up manually."
            )

        self.log.info(f"Landing directory is:{self._asset.landing_path}")
        self.log.info(f"Asset staging picked-up directory is: {self.picked_up_dir}")

        # move previously discovered files to the pickedup directory for this asset:
        # todo: consider if copy is better than move in terms of re-runnability!
        for f in inspected_files:
            # beware: naive implementation here:
            target_path = f.fpath.replace(self._asset.landing_path, self.picked_up_dir)
            self.log.info(f"Moving file {f.fpath} to {target_path}.")
            os.rename(f.fpath, target_path)

        self._meta_adapter.write_inspected_files(discovered_files=inspected_files)

    def _check_pickedup_dir_is_empty(self):
        return len(os.listdir(self.picked_up_dir)) == 0

    @staticmethod
    def inspect_discovered_files(
        for_asset: BaseDataAsset,
        dag_id: str,
        task_id: str,
        dag_exec_date: datetime,
        files: List[str],
    ) -> List[IngestedFileMetadata]:
        ingested_files_meta = []

        for f in files:
            f_stats = os.stat(f)
            ingested_files_meta.append(
                IngestedFileMetadata(
                    for_asset=for_asset,
                    fpath=f,
                    fsize=f_stats.st_size,
                    fctime=datetime.fromtimestamp(f_stats.st_ctime),
                    fmtime=datetime.fromtimestamp(f_stats.st_mtime),
                    dag_id=dag_id,
                    dag_exec_date=dag_exec_date,
                    task_id=task_id,
                )
            )

        return ingested_files_meta
