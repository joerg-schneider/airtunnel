import re
from datetime import datetime, timedelta
from os import path
from typing import Iterable, Optional

from airtunnel import declaration_store
from airtunnel.data_asset import ShellDataAsset, BaseDataAsset
from airtunnel.operators.sql import sql_helpers


class LoadStatus:
    def __init__(
        self,
        for_asset: BaseDataAsset,
        load_time: Optional[datetime] = None,
        dag_id: Optional[str] = None,
        task_id: Optional[str] = None,
        dag_exec_date: Optional[datetime] = None,
    ):
        self.for_asset = for_asset
        if load_time is None:
            self._load_time = datetime.now()
        else:
            self._load_time = load_time

        self._dag_id = dag_id
        self._task_id = task_id
        self._dag_exec_date = dag_exec_date

    def is_within(self, frame: timedelta):
        return frame > (datetime.now() - self.load_time)

    @property
    def load_time(self) -> datetime:
        return self._load_time

    @property
    def dag_id(self) -> str:
        return self._dag_id

    @property
    def task_id(self) -> str:
        return self._task_id

    @property
    def dag_exec_date(self) -> datetime:
        return self._dag_exec_date

    def __repr__(self) -> str:
        return (
            f"{self.for_asset.name} was loaded at {self.load_time}, "
            f"from DAG {self.dag_id} ({self.dag_exec_date}) and task {self.task_id}"
        )


class IngestedFileMetadata:
    def __init__(
        self,
        for_asset: BaseDataAsset,
        filepath: str,
        filesize: int,
        file_mod_time: datetime,
        file_create_time: datetime,
        dag_id: str,
        dag_exec_date: datetime,
        task_id: str,
    ):
        self._for_asset = for_asset
        self._filepath = filepath
        self._filesize = filesize
        self._file_mod_time = file_mod_time
        self._file_create_time = file_create_time
        self._dag_id = dag_id
        self._task_id = task_id
        self._dag_exec_date = dag_exec_date

    @property
    def for_asset(self) -> BaseDataAsset:
        return self._for_asset

    @property
    def filepath(self) -> str:
        return self._filepath

    @property
    def file_mod_time(self) -> datetime:
        return self._file_mod_time

    @property
    def file_create_time(self) -> datetime:
        return self._file_create_time

    @property
    def filesize(self) -> int:
        return self._filesize

    @property
    def dag_id(self) -> str:
        return self._dag_id

    @property
    def task_id(self) -> str:
        return self._task_id

    @property
    def dag_exec_date(self) -> datetime:
        return self._dag_exec_date

    def __repr__(self) -> str:
        return (
            f"{self.for_asset.name} has source file: {path.basename(self.filepath)}, "
            f"of size: {self.filesize}, created at: {self.file_create_time}, collected from:"
            f" DAG: {self.dag_id} ({self.dag_exec_date}) and task id {self.task_id}"
        )


class Lineage:
    def __init__(
        self,
        data_sources: Iterable[BaseDataAsset],
        data_target: BaseDataAsset,
        dag_id: Optional[str] = None,
        dag_exec_date: Optional[datetime] = None,
        task_id: Optional[str] = None,
    ) -> None:
        self._data_sources = data_sources
        self._data_target = data_target
        self._dag_id = dag_id
        self._task_id = task_id
        self._dag_exec_date = dag_exec_date

    @property
    def dag_id(self) -> str:
        return self._dag_id

    @property
    def task_id(self) -> str:
        return self._task_id

    @property
    def dag_exec_date(self) -> datetime:
        return self._dag_exec_date

    @property
    def data_sources(self) -> Iterable[BaseDataAsset]:
        return self._data_sources

    @property
    def data_target(self) -> BaseDataAsset:
        return self._data_target

    @staticmethod
    def lineage_from_sql_statement(
        statement: str, known_data_assets: Iterable[str] = None
    ) -> "Lineage":
        """ Note: simplistic algorithm with room for improvement! (i.e. would not support WITH style CTEs) """

        # ensure we either have received a list of known data assets or can look them up from the declaration store
        if known_data_assets is None:
            known_data_assets = [
                d.asset_name for d in declaration_store.fetch_all_declarations()
            ]

        # parse the affected data assets using their known tokens, focusing on "whole word" tokens

        tokens = [
            t.strip().lower()
            for t in re.compile(r"\s|,|=|\(|\)").split(
                statement.replace("\n", " ").replace("\r", "")
            )
            if len(t) > 2
        ]

        target, sources = None, []

        for tok in tokens:
            if tok in known_data_assets:
                if target is None:
                    target = ShellDataAsset(tok)
                else:
                    sources.append(ShellDataAsset(tok))

        return Lineage(data_sources=sources, data_target=target)

    def __repr__(self) -> str:
        return (
            f"{','.join([s.name for s in self.data_sources])}) --> {self.data_target.name} "
            f" (DAG: {self.dag_id} [{self.dag_exec_date}], task: {self.task_id})"
            if self.dag_id is not None and self.task_id is not None
            else ""
        )

    @staticmethod
    def lineage_from_sql_script(
        script_file_relative_path: str = None,
    ) -> Iterable["Lineage"]:
        script = sql_helpers.load_sql_script(
            script_file_relative_path=script_file_relative_path
        )

        return [
            Lineage.lineage_from_sql_statement(statement=stmnt)
            for stmnt in sql_helpers.split_sql_script(full_script=script)
        ]

    def __eq__(self, other: "Lineage"):
        """ Equality of two Lineage objects. """
        if self.data_target.name != other.data_target.name:
            return False

        if len(list(self.data_sources)) != len(list(other.data_sources)):
            return False

        for src in self.data_sources:
            if src.name not in [src2.name for src2 in other.data_sources]:
                return False
        return True
