import pandas as pd
import pandera as pa
import numpy as np


def configure(context):
    context.stage("synthesis.sirene.sampled")
    context.stage("data.ugms.vehicles_distributions")


def _assign_nb_vehicles(context, siret):
    df_vehicles_more_than_0 = context.data("df_vehicles_more_than_0")
    df_vehicles_gaussian = context.data("df_vehicles_gaussian")
    columns = [
        col.replace("has_", "")
        for col in df_vehicles_more_than_0.columns
        if col.startswith("has_")
    ]
    st20 = siret["st20"]
    result = {"siret": siret["siret"]}
    for column in columns:
        has_probability = df_vehicles_more_than_0.loc[st20, "has_" + column]
        nb_column = "nb_" + column
        result[nb_column] = 0
        if has_probability > 0 and np.random.rand() < has_probability:
            mean, std = df_vehicles_gaussian.loc[(st20, nb_column)]
            nb_vehicles = np.rint(np.random.normal(mean, std))
            result[nb_column] = nb_vehicles

    context.progress.update()
    return result


def execute(context):
    gdf_sirene = context.stage("synthesis.sirene.sampled")
    df_vehicles_more_than_0, df_vehicles_gaussian = context.stage(
        "data.ugms.vehicles_distributions"
    )

    gdf_sirene = pa.DataFrameSchema(
        {
            "siren": pa.Column("int32"),
            "siret": pa.Column(int),
            "municipality_id": pa.Column("category"),
            "employees": pa.Column(int),
            "ape": pa.Column(str),
            "law_status": pa.Column(str),
            "st8": pa.Column(int),
            "st20": pa.Column(int),
            "st45": pa.Column(str),
            "geometry": pa.Column("geometry"),
        }
    ).validate(gdf_sirene)

    df_vehicles_more_than_0 = pa.DataFrameSchema(
        index=pa.Index(pa.Int32, name="st20"),
        columns={
            "has_bicycles": pa.Column(float),
            "has_motorcycles": pa.Column(float),
            "has_cars": pa.Column(float),
            "has_vans_small": pa.Column(float),
            "has_vans_big": pa.Column(float),
            "has_trucks_7t5": pa.Column(float),
            "has_trucks_12t": pa.Column(float),
            "has_trucks_19t": pa.Column(float),
            "has_trucks_32t": pa.Column(float),
            "has_articuated_28t": pa.Column(float),
            "has_articuated_40t": pa.Column(float),
        },
    ).validate(df_vehicles_more_than_0)

    df_vehicles_gaussian = pa.DataFrameSchema(
        index=pa.MultiIndex(
            [
                pa.Index(pa.Int32, name="st20"),
                pa.Index(str),
            ]
        ),
        columns={
            "mean": pa.Column(float),
            "std": pa.Column(float),
        },
    ).validate(df_vehicles_gaussian)

    sirene_nb_vehicles = []
    with context.progress(label="Assigning Nb Vehicles ...", total=len(gdf_sirene)):
        with context.parallel(
            {
                "df_vehicles_more_than_0": df_vehicles_more_than_0,
                "df_vehicles_gaussian": df_vehicles_gaussian,
            }
        ) as parallel:
            for nb_vehicles in parallel.imap(
                _assign_nb_vehicles, gdf_sirene.to_dict(orient="records")
            ):
                sirene_nb_vehicles.append(nb_vehicles)

    gdf_sirene = gdf_sirene.merge(
        pd.DataFrame.from_dict(sirene_nb_vehicles), on="siret"
    )
    return gdf_sirene
