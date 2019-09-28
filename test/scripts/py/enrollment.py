from airtunnel import PandasDataAsset, PandasDataAssetIO


def rebuild_for_store(asset: PandasDataAsset, airflow_context):
    enrollment_data = PandasDataAssetIO.read_data_asset(
        asset=asset, source_files=asset.pickedup_files(airflow_context)
    )

    PandasDataAssetIO.write_data_asset(asset=asset, data=enrollment_data)
