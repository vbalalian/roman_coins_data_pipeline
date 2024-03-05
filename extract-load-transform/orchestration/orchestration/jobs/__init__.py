from dagster import AssetSelection, define_asset_job

airbyte_assets = AssetSelection.groups("roman_coins_api_minio")
loading_assets = AssetSelection.groups("load")

airbyte_sync_job = define_asset_job(
    name='airbyte_sync_job',
    selection=airbyte_assets
)

loading_job = define_asset_job(
    name="loading_job",
    selection=loading_assets
)