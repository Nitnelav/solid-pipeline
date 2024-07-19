import pandas as pd
import pandera as pa
import numpy as np


def configure(context):
    context.stage("data.ugms.cleaned")


def _more_than_0_probability(group):
    nb_columns = [col for col in group.columns if col.startswith("nb_")]
    weight_sum = group["establishment_weight"].sum()
    group[nb_columns] = (group[nb_columns] > 0).multiply(
        group["establishment_weight"] / weight_sum, axis=0
    )
    group.drop(columns=["st20", "establishment_weight"], inplace=True)
    return group.sum()


def _get_gaussian(group):
    nb_columns = [c for c in group.columns if c.startswith("nb_")]
    result = {col: (0.0, 0.0) for col in nb_columns}
    for col in nb_columns:
        col_group = group[[col, "establishment_weight"]][group[col] > 0]
        if col_group.empty:
            continue
        weight_sum = col_group["establishment_weight"].sum()

        mean = (col_group[col] * col_group["establishment_weight"]).sum() / weight_sum
        std = np.sqrt(
            (col_group["establishment_weight"] * (col_group[col] - mean) ** 2).sum() / weight_sum
        )
        result[col] = (mean, std if not np.isnan(std) else 0.0)

    return pd.DataFrame.from_dict(result, orient="index", columns=["mean", "std"])


def execute(context):
    df_establishments, _, df_vehicles = context.stage("data.ugms.cleaned")

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
            "establishment_weight": pa.Column(np.float32),
        }
    ).validate(df_establishments)

    df_vehicles = pa.DataFrameSchema(
        {
            "establishment_id": pa.Column(str),
            "has_vehicles": pa.Column(bool),
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

    df_vehicles.drop(columns=["establishment_id", "has_vehicles"], inplace=True)

    df_vehicles_more_than_0 = df_vehicles.groupby("st20").apply(
        _more_than_0_probability
    )
    df_vehicles_more_than_0.rename(
        columns={
            col: col.replace("nb_", "has_")
            for col in nb_columns
            if col.startswith("nb_")
        },
        inplace=True,
    )

    df_vehicles_gaussian = df_vehicles.groupby("st20").apply(_get_gaussian)

    return df_vehicles_more_than_0, df_vehicles_gaussian
