import pandas as pd
import pandera as pa
import numpy as np


def configure(context):
    context.stage("data.ugms.cleaned")



def _get_goods_weight_kg(group):
    kg_per_unit = 0.0
    # m3_per_unit = 0.0
    kg_per_m3 = 0.0

    group_kg_per_unit = group[group["kg_per_unit"] > 0][
        ["kg_per_unit", "establishment_weight"]
    ]
    if not group_kg_per_unit.empty:
        group_kg_per_unit["kg_per_unit"] *= group_kg_per_unit["establishment_weight"]
        kg_per_unit = (
            group_kg_per_unit["kg_per_unit"].sum()
            / group_kg_per_unit["establishment_weight"].sum()
        )

    # unused...

    # group_m3_per_unit = group[group["m3_per_unit"] > 0][
    #     ["m3_per_unit", "establishment_weight"]
    # ]
    # if not group_m3_per_unit.empty:
    #     group_m3_per_unit["m3_per_unit"] *= group_m3_per_unit["establishment_weight"]
    #     m3_per_unit = (
    #         group_m3_per_unit["m3_per_unit"].sum()
    #         / group_m3_per_unit["establishment_weight"].sum()
    #     )

    group_kg_per_m3 = group[group["kg_per_m3"] > 0][
        ["kg_per_m3", "establishment_weight"]
    ]
    if not group_kg_per_m3.empty:
        group_kg_per_m3["kg_per_m3"] *= group_kg_per_m3["establishment_weight"]
        kg_per_m3 = (
            group_kg_per_m3["kg_per_m3"].sum()
            / group_kg_per_m3["establishment_weight"].sum()
        )

    group = group.drop(columns=["kg_per_unit", "kg_per_m3"]) # , "m3_per_unit"
    group.loc[(group["weight_kg"] == 0) & (group["volume_m3"] > 0), "weight_kg"] =  group["volume_m3"] * kg_per_m3
    group.loc[(group["weight_kg"] == 0) & (group["nb_units"] > 0), "weight_kg"] =  group["nb_units"] * kg_per_unit

    return group


def execute(context):
    df_establishments, df_goods, _ = context.stage("data.ugms.cleaned")

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

    df_goods = pa.DataFrameSchema(
        {
            "establishment_id": pa.Column(str),
            "move_type": pa.Column("category", nullable=True),
            "good_type": pa.Column("category", nullable=True),
            "packaging": pa.Column("category", nullable=True),
            "nb_units": pa.Column(np.int32, nullable=True),
            "weight_kg": pa.Column(np.float64, nullable=True),
            "volume_m3": pa.Column(np.float64, nullable=True),
        }
    ).validate(df_goods)

    df_goods = df_goods.merge(
        df_establishments[["st8", "establishment_id", "establishment_weight"]],
        on="establishment_id",
    )

    df_goods["kg_per_unit"] = 0.0
    df_goods.loc[
        (df_goods["nb_units"] > 0) & (df_goods["weight_kg"] > 0), "kg_per_unit"
    ] = df_goods["weight_kg"] / df_goods["nb_units"]

    # unused...

    # df_goods["m3_per_unit"] = 0.0
    # df_goods.loc[
    #     (df_goods["nb_units"] > 0) & (df_goods["volume_m3"] > 0), "m3_per_unit"
    # ] = df_goods["volume_m3"] / df_goods["nb_units"]

    df_goods["kg_per_m3"] = 0.0
    df_goods.loc[
        (df_goods["volume_m3"] > 0) & (df_goods["weight_kg"] > 0), "kg_per_m3"
    ] = df_goods["weight_kg"] / df_goods["volume_m3"]

    df_goods = df_goods.groupby("good_type", observed=True).apply(
        _get_goods_weight_kg
    ).reset_index(drop=True)

    
    df_good_type_distributions = df_goods.groupby(["st8", "good_type"], observed=True)["establishment_weight"].sum()

    def _get_goods_weight_gaussian(group):
        weight_sum = group["establishment_weight"].sum()

        mean = (group["weight_kg"] * group["establishment_weight"]).sum() / weight_sum
        std = np.sqrt(
            (group["establishment_weight"] * (group["weight_kg"] - mean) ** 2).sum() / weight_sum
        )
        return pd.Series({ "mean": mean, "std": std if not np.isnan(std) else 0.0})

    df_good_weight_distributions = df_goods.groupby(["st8", "good_type"], observed=True).apply(
        _get_goods_weight_gaussian
    )
    return df_good_type_distributions, df_good_weight_distributions
