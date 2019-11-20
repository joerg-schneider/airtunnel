from airtunnel import PySparkDataAsset, PySparkDataAssetIO


def rebuild_for_store(asset: PySparkDataAsset, airflow_context):
    enrollment_data = PySparkDataAssetIO.read_data_asset(
        asset=asset, source_files=asset.pickedup_files(airflow_context)
    )

    PySparkDataAssetIO.write_data_asset(asset=asset, data=enrollment_data)
