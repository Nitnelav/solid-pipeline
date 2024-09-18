import pandas as pd
import pandera as pa


def configure(context):
    context.stage("vehicles.quantity")


def _generate_vehicles(context, siret):
    vehicles = []
    types = [col.replace("nb_", "") for col in siret.keys() if col.startswith("nb_")]

    for type in types:
        for _ in range(int(siret["nb_" + type])):
            vehicles.append(
                {
                    "siret": siret["siret"],
                    "type": type,
                    "start": siret["geometry"],
                }
            )
    context.progress.update()
    return vehicles


def execute(context):
    gdf_sirene = context.stage("vehicles.quantity")

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

    vehicles = []
    with context.progress(label="Generating Vehicles ...", total=len(gdf_sirene)):
        with context.parallel() as parallel:
            for new_vehicles in parallel.imap(
                _generate_vehicles, gdf_sirene.to_dict(orient="records")
            ):
                vehicles += new_vehicles

    df_vehicles = pd.DataFrame(vehicles)

    return df_vehicles
