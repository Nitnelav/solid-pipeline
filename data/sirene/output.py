import geopandas as gpd
import pandas as pd
import pandera as pa

def configure(context):
    context.config("sirene_output_path")
    context.stage("data.sirene.assign_hub")


def execute(context):
    gdf_sirene: gpd.GeoDataFrame = context.stage("data.sirene.assign_hub")  
    
    gdf_sirene['municipality_id'] = gdf_sirene['municipality_id'].astype('object')
    gdf_sirene.to_file(context.config("sirene_output_path"), index=False)
    return gdf_sirene