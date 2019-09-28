from os import path

import pandas as pd

from airtunnel import PandasDataAsset


def rebuild_for_store(asset: PandasDataAsset, airflow_context):
    dfs = [pd.read_csv(f) for f in asset.pickedup_files(airflow_context)]

    programme: pd.DataFrame = pd.concat(dfs)
    programme = programme.drop_duplicates(subset=asset.declarations.key_columns)

    programme.to_parquet(
        path.join(asset.staging_ready_path, asset.output_filename),
        compression=asset.declarations.out_comp_codec,
    )
