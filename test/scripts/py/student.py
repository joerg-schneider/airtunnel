from airtunnel import PandasDataAsset, PandasDataAssetIO


def rebuild_for_store(asset: PandasDataAsset, airflow_context):

    student_data = PandasDataAssetIO.read_data_asset(
        asset=asset, source_files=asset.pickedup_files(airflow_context)
    )

    student_data = asset.rename_fields_as_declared(student_data)

    PandasDataAssetIO.write_data_asset(asset=asset, data=student_data)
