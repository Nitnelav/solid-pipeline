import pandas as pd
import pandera as pa
import numpy as np


def configure(context):
    context.stage("data.ugms.cleaned")


def execute(context):
    df_establishments, df_goods, _ = context.stage("data.ugms.cleaned")

    # Validate the data
    df_establishments = pa.DataFrameSchema(
        {
            "establishment_id": pa.Column(str),
            "st8": pa.Column(np.int32),
            "st20": pa.Column(np.int32),
            "st45": pa.Column("category"),
            "ape": pa.Column("category"),
            "employees": pa.Column(np.int32),
            "nb_movements": pa.Column(float),
            "nb_deliveries": pa.Column(float),
            "nb_pickups": pa.Column(float),
            "nb_pickups_and_deliveries": pa.Column(float),
            "suburb_type": pa.Column("category"),
            "establishment_weight": pa.Column(float),
        }
    ).validate(df_establishments)

    df_goods = pa.DataFrameSchema(
        {
            "establishment_id": pa.Column(str),
            "move_type": pa.Column("category"),
            "good_type": pa.Column("category"),
            "move_mode": pa.Column("category"),
            "weight_kg": pa.Column(float),
            "operation_weight": pa.Column(float),
        }
    ).validate(df_goods)

    df_goods = df_goods.merge(
        df_establishments[["establishment_id", "st8"]], on="establishment_id"
    )

    df_establishment_line_count = (
        df_goods.groupby(["st8", "establishment_id"], as_index=False)
        .size()
        .groupby("st8")
        .value_counts(subset=["size"], normalize=True)
        .reset_index(name="percentage")
    )

    def _get_goods_weight_gaussian(group):
        
        weight_sum = group["operation_weight"].sum()

        mean = (group["weight_kg"] * group["operation_weight"]).sum() / weight_sum
        std = np.sqrt(
            (group["operation_weight"] * (group["weight_kg"] - mean) ** 2).sum() / weight_sum
        )
        std = std if not np.isnan(std) else 0.0

        return pd.Series({"mean": mean, "std": std, "weight_coeff": weight_sum})

    df_good_weight_distributions = df_goods.groupby(
        ["st8", "move_type", "good_type", "move_mode"], observed=True
    ).apply(_get_goods_weight_gaussian).reset_index()
    
    return df_establishment_line_count, df_good_weight_distributions
