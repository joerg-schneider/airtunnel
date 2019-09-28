from airtunnel import PandasDataAsset, PandasDataAssetIO


def rebuild_for_store(asset: PandasDataAsset, airflow_context):
    programme_data = PandasDataAssetIO.read_data_asset(
        asset=asset, source_files=asset.pickedup_files(airflow_context)
    )
    programme_data = programme_data.drop_duplicates(
        subset=asset.declarations.key_columns
    )
    PandasDataAssetIO.write_data_asset(asset=asset, data=programme_data)
