from dagster import AssetSelection, define_asset_job

airbyte_sync_job = define_asset_job(
    name='airbyte_sync_job',
    selection=AssetSelection.all()
)