import geopandas as gpd
import pandas as pd
import pandera as pa

def configure(context):
    context.config("movements_output_path")
    context.stage("movements.categorize")


def execute(context):
    gdf_moves: gpd.GeoDataFrame = context.stage("movements.categorize")
    
    gdf_moves.to_file(context.config("movements_output_path"), index=False)
    return gdf_moves