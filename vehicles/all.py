import pandera as pa
import numpy as np

def configure(context):
    context.stage("data.sirene.assign_hub")
    context.stage("data.ugms.vehicles_distributions")

def execute(context):

    gdf_sirene = context.stage("data.sirene.assign_hub")
    df_vehicles_more_than_0, df_vehicles_gaussian = context.stage("data.ugms.vehicles_distributions")

    gdf_sirene = pa.DataFrameSchema(
        {
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
        }
    ).validate(gdf_sirene)

    df_vehicles_more_than_0 = pa.DataFrameSchema(
        index=pa.Index(pa.Int, name="st20"),
        columns = {
            "has_bicycles": pa.Column(np.float32),
            "has_motorcycles": pa.Column(np.float32),
            "has_cars": pa.Column(np.float32),
            "has_vans_small": pa.Column(np.float32),
            "has_vans_big": pa.Column(np.float32),
            "has_trucks_7t5": pa.Column(np.float32),
            "has_trucks_12t": pa.Column(np.float32),
            "has_trucks_19t": pa.Column(np.float32),
            "has_trucks_32t": pa.Column(np.float32),
            "has_articuated_28t": pa.Column(np.float32),
            "has_articuated_40t": pa.Column(np.float32),
        }
    ).validate(df_vehicles_more_than_0)

    df_vehicles_gaussian = pa.DataFrameSchema(
        index=pa.Index(pa.Int, name="st20"),
        columns = {
        }
    ).validate(df_vehicles_gaussian)
    
    return