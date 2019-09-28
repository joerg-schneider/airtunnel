from os import path

import pandas as pd

from airtunnel import PandasDataAsset


def rebuild_for_store(asset: PandasDataAsset, airflow_context):
    dfs = [pd.read_csv(f) for f in asset.pickedup_files(airflow_context)]

    student: pd.DataFrame = pd.concat(dfs)

    student = asset.rename_fields_as_declared(student)

    student.to_parquet(
        path.join(asset.staging_ready_path, asset.output_filename),
        compression=asset.declarations.out_comp_codec,
    )
