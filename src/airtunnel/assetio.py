from abc import abstractmethod, ABC
from os import path
from typing import Union, Iterable

import pandas as pd

from airtunnel.data_asset import BaseDataAsset, PandasDataAsset


class BaseDataAssetIO(ABC):
    @staticmethod
    @abstractmethod
    def write_data_asset(
        asset: BaseDataAsset,
        data: Union[pd.DataFrame, "pyspark.sql.DataFrame"],
        **writer_kwargs,
    ) -> None:
        raise NotImplementedError

    @staticmethod
    @abstractmethod
    def read_data_asset(
        asset: BaseDataAsset, source_files: Iterable[str], **reader_kwargs
    ) -> Union[pd.DataFrame, "pyspark.sql.DataFrame"]:
        raise NotImplementedError


class PandasDataAssetIO(BaseDataAssetIO):
    @staticmethod
    def write_data_asset(
        asset: BaseDataAsset, data: pd.DataFrame, **writer_kwargs
    ) -> None:
        if asset.declarations.is_parquet_output:
            data.to_parquet(
                path.join(asset.staging_ready_path, asset.output_filename),
                compression=asset.declarations.out_comp_codec,
                **writer_kwargs,
            )
        elif asset.declarations.is_csv_output:
            data.to_csv(
                path_or_buf=path.join(asset.staging_ready_path, asset.output_filename),
                compression=asset.declarations.out_comp_codec,
                **writer_kwargs,
            )
        else:
            raise ValueError(f"Only output formats of csv/parquet are supported!")

    @staticmethod
    def read_data_asset(
        asset: PandasDataAsset, source_files: Iterable[str], **reader_kwargs
    ) -> pd.DataFrame:
        if asset.declarations.is_csv_input:
            data = [pd.read_csv(f, **reader_kwargs) for f in source_files]
        elif asset.declarations.is_xls_input:
            data = [pd.read_excel(f, **reader_kwargs) for f in source_files]
        elif asset.declarations.is_parquet_input:
            data = [pd.read_parquet(f, **reader_kwargs) for f in source_files]

        return pd.concat(data) if len(data) > 1 else data[0]
