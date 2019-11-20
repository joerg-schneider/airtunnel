from airtunnel import PySparkDataAsset, PySparkDataAssetIO


def rebuild_for_store(asset: PySparkDataAsset, airflow_context):

    student_data = PySparkDataAssetIO.read_data_asset(
        asset=asset, source_files=asset.pickedup_files(airflow_context)
    )

    student_data = asset.rename_fields_as_declared(student_data)

    PySparkDataAssetIO.write_data_asset(asset=asset, data=student_data)
