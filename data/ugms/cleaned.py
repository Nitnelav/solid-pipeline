import pandas as pd
import geopandas as gpd

def configure(context):
    context.stage("data.ugms.raw")

def execute(context):
    df_establishments, df_goods, df_vehicles = context.stage("data.ugms.raw")