from dagster import AssetSelection, define_asset_job

airbyte_assets = AssetSelection.groups("extract")
loading_assets = AssetSelection.groups("load")
transform_assets = AssetSelection.groups("transform")

extract_job = define_asset_job(
    name='extract_job',
    selection=airbyte_assets
)

loading_job = define_asset_job(
    name="loading_job",
    selection=loading_assets
)

transform_job = define_asset_job(
    name="transform_job",
    selection=transform_assets
)