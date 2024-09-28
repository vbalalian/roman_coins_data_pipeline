from dagster import AssetExecutionContext
from dagster_dbt import DbtCliResource, dbt_assets
from ..resources import dbt_manifest_path, dbt_resource

@dbt_assets(
        manifest=dbt_manifest_path    
)
def dbt_transform_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt_resource.cli(["build"], context=context).stream()