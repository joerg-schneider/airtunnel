from os import path

import pandas as pd

from airtunnel import PandasDataAsset


def rebuild_for_store(asset: PandasDataAsset, airflow_context):
    student = PandasDataAsset(name="student")
    programme = PandasDataAsset(name="programme")
    enrollment = PandasDataAsset(name="enrollment")

    student_df = student.retrieve_from_store(airflow_context, consuming_asset=asset)
    programme_df = programme.retrieve_from_store(airflow_context, consuming_asset=asset)
    enrollment_df = enrollment.retrieve_from_store(
        airflow_context, consuming_asset=asset
    )

    enrollment_summary: pd.DataFrame = enrollment_df.merge(
        right=student_df, on=student.declarations.key_columns
    ).merge(right=programme_df, on=programme.declarations.key_columns)

    enrollment_summary = enrollment_summary.loc[:,["student_major", "programme_name", "student_id"]].groupby(
        by=["student_major", "programme_name"]
    ).count()

    enrollment_summary.to_parquet(
        path.join(asset.staging_ready_path, asset.output_filename),
        compression=asset.declarations.out_comp_codec,
    )
