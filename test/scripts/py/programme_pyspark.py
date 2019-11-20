from airtunnel import PySparkDataAsset, PySparkDataAssetIO


def rebuild_for_store(asset: PySparkDataAsset, airflow_context):
    programme_data = PySparkDataAssetIO.read_data_asset(
        asset=asset, source_files=asset.pickedup_files(airflow_context)
    )
    programme_data = programme_data.drop_duplicates(
        subset=asset.declarations.key_columns
    )
    PySparkDataAssetIO.write_data_asset(asset=asset, data=programme_data)
