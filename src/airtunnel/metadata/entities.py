import re
from datetime import datetime, timedelta
from typing import Iterable, Optional

from airtunnel import declaration_store
from airtunnel.data_asset import ShellDataAsset, BaseDataAsset
from airtunnel.operators.sql import sql_helpers


class LoadStatus:
    def __init__(self, for_asset: BaseDataAsset, load_time: Optional[datetime] = None):
        self.for_asset = for_asset
        if load_time is None:
            self.load_time = datetime.now()
        else:
            self.load_time = load_time

    def is_within(self, frame: timedelta):
        return frame > (datetime.now() - self.load_time)


class IngestedFileMetadata:
    def __init__(
        self,
        for_asset: BaseDataAsset,
        fpath: str,
        fsize: int,
        fmtime: datetime,
        fctime: datetime,
        dag_id: str,
        dag_exec_date: datetime,
        task_id: str,
    ):
        self.for_asset = for_asset
        self.fpath = fpath
        self.fsize = fsize
        self.fmtime = fmtime
        self.fctime = fctime
        self.dag_id = dag_id
        self.task_id = task_id
        self.dag_exec_date = dag_exec_date


class Lineage:
    __slots__ = ["__data_sources", "__data_target"]

    def __init__(
        self, data_sources: Iterable[BaseDataAsset], data_target: BaseDataAsset
    ) -> None:
        self.__data_sources = data_sources
        self.__data_target = data_target

    @property
    def data_sources(self) -> Iterable[BaseDataAsset]:
        return self.__data_sources

    @property
    def data_target(self) -> BaseDataAsset:
        return self.__data_target

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
