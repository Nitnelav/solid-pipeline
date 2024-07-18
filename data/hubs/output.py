import geopandas as gpd


def configure(context):
    context.config("hubs_output_path")
    context.stage("data.hubs.list")


def execute(context):
    df_hubs: gpd.GeoDataFrame = context.stage("data.hubs.list")

    df_hubs.to_file(context.config("hubs_output_path"), index=False)
    return df_hubs
