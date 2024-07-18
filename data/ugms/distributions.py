import pandera as pa
import numpy as np


def configure(context):
    context.stage("data.ugms.cleaned")


def execute(context):
    df_establishments, df_goods, df_vehicles = context.stage("data.ugms.cleaned")

    # Validate the data
    df_establishments = pa.DataFrameSchema(
        {
            "establishment_id": pa.Column(str),
            "st8": pa.Column(np.int8),
            "st20": pa.Column(np.int8),
            "st45": pa.Column("category"),
            "ape": pa.Column("category"),
            "employees": pa.Column(np.int32),
            "nb_movements": pa.Column(np.float32),
            "nb_deliveries": pa.Column(np.float32),
            "nb_pickups": pa.Column(np.float32),
            "nb_pickups_and_deliveries": pa.Column(np.float32),
        }
    ).validate(df_establishments)

    df_goods = pa.DataFrameSchema(
        {
            "establishment_id": pa.Column(str),
            "move_type": pa.Column("category", nullable=True),
            "good_type": pa.Column("category", nullable=True),
            "packaging": pa.Column("category", nullable=True),
            "nb_units": pa.Column(np.int32, nullable=True),
            "weight_kg": pa.Column(np.int32, nullable=True),
            "volume_m3": pa.Column(np.int32, nullable=True),
        }
    ).validate(df_goods)

    df_vehicles = pa.DataFrameSchema(
        {
            "establishment_id": pa.Column(str),
            "nb_bicycles": pa.Column(np.int32),
            "nb_motorcycles": pa.Column(np.int32),
            "nb_cars": pa.Column(np.int32),
            "nb_vans_small": pa.Column(np.int32),
            "nb_vans_big": pa.Column(np.int32),
            "nb_trucks_7t5": pa.Column(np.int32),
            "nb_trucks_12t": pa.Column(np.int32),
            "nb_trucks_19t": pa.Column(np.int32),
            "nb_trucks_32t": pa.Column(np.int32),
            "nb_articuated_28t": pa.Column(np.int32),
            "nb_articuated_40t": pa.Column(np.int32),
        }
    ).validate(df_vehicles)

    df_vehicles = df_vehicles.merge(
        df_establishments[["st20", "establishment_id", "establishment_weight"]],
        on="establishment_id",
    )
    nb_columns = [col for col in df_vehicles.columns if col.startswith("nb_")]
    df_vehicles.loc[:, nb_columns] = df_vehicles.loc[:, nb_columns].multiply(
        df_vehicles["establishment_weight"].astype(int), axis=0
    )
    df_vehicles.drop(columns=["establishment_id", "establishment_weight"], inplace=True)
    df_vehicles = df_vehicles.groupby("st20").sum()
    df_vehicles.rename(columns={col: col[3:] for col in nb_columns}, inplace=True)

    df_goods = df_goods.merge(
        df_establishments[["st8", "establishment_id", "establishment_weight"]],
        on="establishment_id",
    )
    df_goods.drop(columns=["establishment_id"], inplace=True)

    return df_establishments, df_goods, df_vehicles
