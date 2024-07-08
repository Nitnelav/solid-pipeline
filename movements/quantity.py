import geopandas as gpd
import pandas as pd
import pandera as pa

from .utils import get_st45_list

def configure(context):
    context.stage("data.sirene.assign_hub")

def _get_movements(sirene_row):
    employees = sirene_row["employees"]
    st8 = sirene_row["st8"]
    movements = []
    for el in get_st45_list(st8):
        # st45 = el["st45"]
        f = el["function"]
        movements.append(f(employees))
    return int(sum(movements) / len(movements))

def execute(context):
    gdf_sirene = context.stage("data.sirene.assign_hub")
    pa.DataFrameSchema({
        "siren": pa.Column("int32"),
        "siret": pa.Column(int),
        "municipality_id": pa.Column(str),
        "employees": pa.Column(int),
        "ape": pa.Column(str),
        "law_status": pa.Column(str),
        "st8": pa.Column(int),
        "st20": pa.Column(int),
        "st45": pa.Column(str),
        "hub_id": pa.Column(int),
        "hub_distance": pa.Column(float),
        "geometry": pa.Column("geometry"),
    }).validate(gdf_sirene)

    gdf_sirene["movements"] = gdf_sirene.apply(_get_movements, axis=1)

    return gdf_sirene
