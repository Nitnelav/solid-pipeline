import pandera as pa
import pandas as pd
import numpy as np

from ..sirene.cleaned.utils import get_st20
from .utils import GOODS_SHORT_NAME_MAP


def configure(context):
    context.stage("data.ugms.raw")


def _get_goods_weight_kg(group):
    kg_per_unit = 0.0
    kg_per_m3 = 0.0

    group_kg_per_unit = group[group["kg_per_unit"] > 0][
        ["kg_per_unit", "operation_weight"]
    ]
    if not group_kg_per_unit.empty:
        group_kg_per_unit["kg_per_unit"] *= group_kg_per_unit["operation_weight"]
        kg_per_unit = (
            group_kg_per_unit["kg_per_unit"].sum()
            / group_kg_per_unit["operation_weight"].sum()
        )

    group_kg_per_m3 = group.loc[
        group["kg_per_m3"] > 0, ["kg_per_m3", "operation_weight"]
    ]

    if not group_kg_per_m3.empty:
        group_kg_per_m3["kg_per_m3"] *= group_kg_per_m3["operation_weight"]
        kg_per_m3 = (
            group_kg_per_m3["kg_per_m3"].sum()
            / group_kg_per_m3["operation_weight"].sum()
        )

    mask = (group["weight_kg"].isna()) & (group["volume_m3"] > 0)
    group.loc[mask, "weight_kg"] = group.loc[mask, "volume_m3"] * kg_per_m3
    mask = (group["weight_kg"].isna()) & (group["nb_units"] > 0)
    group.loc[mask, "weight_kg"] = group.loc[mask, "nb_units"] * kg_per_unit

    return group


def execute(context):
    df_establishments, df_operations, df_goods, df_vehicles, df_relations = context.stage(
        "data.ugms.raw"
    )

    # Validate the data
    df_establishments = pa.DataFrameSchema(
        {
            "establishment_id": pa.Column(str),
            "st8": pa.Column(np.int32),
            "st45": pa.Column(str),
            "ape": pa.Column(str),
            "employees": pa.Column(np.int32),
            "nb_movements": pa.Column(float),
            "nb_deliveries": pa.Column(float),
            "nb_pickups": pa.Column(float),
            "nb_pickups_and_deliveries": pa.Column(float),
            "surface": pa.Column(float),
            "suburb_type": pa.Column("category"),
            "establishment_weight": pa.Column(float),
        }
    ).validate(df_establishments)

    df_operations = pa.DataFrameSchema(
        {
            "establishment_id": pa.Column(str),
            "operation_id": pa.Column(np.int32),
            "move_type": pa.Column(str),
            "frequency": pa.Column(float),
            "nb_movements": pa.Column(float, nullable=True),
            "move_mode": pa.Column(str),
            "nb_units": pa.Column(float, nullable=True),
            "weight_kg": pa.Column(float, nullable=True),
            "volume_m3": pa.Column(float, nullable=True),
            "operation_weight": pa.Column(float),
        }
    ).validate(df_operations)

    df_goods = pa.DataFrameSchema(
        {
            "establishment_id": pa.Column(str),
            "operation_id": pa.Column(np.int32),
            "move_type": pa.Column(str),
            "good_type": pa.Column(str),
            "good_type_other": pa.Column(str),
            "packaging": pa.Column(str),
            "nb_units": pa.Column(float),
            "weight_kg": pa.Column(float),
            "volume_m3": pa.Column(float),
        }
    ).validate(df_goods)

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

    df_relations = pa.DataFrameSchema(
        {
            "establishment_id": pa.Column(str),
            "receive_center_nb": pa.Column(np.int32),
            "receive_center_part": pa.Column(float),
            "receive_center_part_cost": pa.Column(float),
            "receive_wholesaler_nb": pa.Column(np.int32),
            "receive_wholesaler_part": pa.Column(float),
            "receive_wholesaler_part_cost": pa.Column(float),
            "receive_supplier_nb": pa.Column(np.int32),
            "receive_supplier_part": pa.Column(float),
            "receive_supplier_part_cost": pa.Column(float),
            "receive_client_nb": pa.Column(np.int32),
            "receive_client_part": pa.Column(float),
            "receive_client_part_cost": pa.Column(float),
            "receive_other_nb": pa.Column(np.int32),
            "receive_other_part": pa.Column(float),
            "receive_other_part_cost": pa.Column(float),
            "send_center_nb": pa.Column(np.int32),
            "send_center_part": pa.Column(float),
            "send_center_part_cost": pa.Column(float),
            "send_wholesaler_nb": pa.Column(np.int32),
            "send_wholesaler_part": pa.Column(float),
            "send_wholesaler_part_cost": pa.Column(float),
            "send_supplier_nb": pa.Column(np.int32),
            "send_supplier_part": pa.Column(float),
            "send_supplier_part_cost": pa.Column(float),
            "send_client_nb": pa.Column(np.int32),
            "send_client_part": pa.Column(float),
            "send_client_part_cost": pa.Column(float),
            "send_other_nb": pa.Column(np.int32),
            "send_other_part": pa.Column(float),
            "send_other_part_cost": pa.Column(float),
        }
    ).validate(df_relations)

    df_establishments["st20"] = df_establishments.apply(
        lambda x: get_st20(x["st8"], x["employees"]), axis=1
    ).astype(int)
    df_establishments["st45"] = df_establishments["st45"].astype("category")
    df_establishments["ape"] = df_establishments["ape"].astype("category")

    df_operations["move_type"] = (
        df_operations["move_type"]
        .map({"r": "delivery", "e": "pickup", "c": "both"})
    )

    df_operations = df_operations.loc[
        df_operations["move_mode"] != "nan"
    ]  # 5097 before, 4948 after

    df_goods.loc[:, "move_type"] = (
        df_goods["move_type"]
        .map({"r": "delivery", "e": "pickup", "999999999": np.nan})
    )

    # Fetch missing move_type from operations
    df_goods = df_goods.merge(
        df_operations[["operation_id", "move_type", "move_mode", "frequency"]],
        on="operation_id",
        suffixes=("", "_op"),
    )
    mask = df_goods["move_type"].isna()
    df_goods.loc[mask, "move_type"] = df_goods.loc[mask, "move_type_op"]

    df_goods = df_goods.drop(columns=["move_type_op"])
    df_goods = df_goods.rename(columns={"move_mode_op": "move_mode", "frequency_op": "frequency"})

    df_goods.loc[:, "good_type"] = (
        df_goods["good_type"].replace("999999999", np.nan).astype("category")
    )
    df_goods = df_goods.loc[~df_goods["good_type"].isna()]  # 65 rows removed
    df_goods.loc[:, "good_type"] = df_goods["good_type"].map(GOODS_SHORT_NAME_MAP)

    df_goods.loc[:, "packaging"] = (
        df_goods["packaging"].replace("999999999", np.nan).astype("category")
    )
    df_goods.loc[:, "nb_units"] = df_goods["nb_units"].replace(999999999, np.nan)
    df_goods.loc[:, "weight_kg"] = df_goods["weight_kg"].replace(999999999, np.nan)
    df_goods.loc[:, "volume_m3"] = df_goods["volume_m3"].replace(999999999, np.nan)

    df_goods = df_goods.merge(
        df_operations[["operation_id", "nb_units", "weight_kg", "volume_m3"]],
        on="operation_id",
        suffixes=("", "_op"),
    )
    mask = (
        (df_goods["nb_units"] > 0)
        | (df_goods["weight_kg"] > 0)
        | (df_goods["volume_m3"] > 0)
    )
    df_goods.loc[~mask, "nb_units"] = df_goods.loc[~mask, "nb_units"].fillna(
        df_goods.loc[~mask, "nb_units_op"]
    )
    df_goods.loc[~mask, "weight_kg"] = df_goods.loc[~mask, "weight_kg"].fillna(
        df_goods.loc[~mask, "weight_kg_op"]
    )
    df_goods.loc[~mask, "volume_m3"] = df_goods.loc[~mask, "volume_m3"].fillna(
        df_goods.loc[~mask, "volume_m3_op"]
    )
    df_goods = df_goods.drop(columns=["nb_units_op", "weight_kg_op", "volume_m3_op"])

    mask = (
        (df_goods["nb_units"] > 0)
        | (df_goods["weight_kg"] > 0)
        | (df_goods["volume_m3"] > 0)
    )
    df_goods = df_goods.loc[mask]  # 273 rows removed

    # assign weight_kg based on observations on nb_units and volume_m3 for same good_type
    df_goods = df_goods.merge(
        df_establishments[["establishment_id", "st8"]],
        on="establishment_id",
    )
    df_goods = df_goods.merge(
        df_operations[["operation_id", "operation_weight"]],
        on="operation_id",
    )

    df_goods = df_goods.loc[df_goods["operation_weight"] > 0]

    df_goods.loc[:, "kg_per_unit"] = 0.0
    mask = (df_goods["nb_units"] > 0) & (df_goods["weight_kg"] > 0)
    df_goods.loc[mask, "kg_per_unit"] = (
        df_goods.loc[mask, "weight_kg"] / df_goods.loc[mask, "nb_units"]
    )

    df_goods.loc[:, "kg_per_m3"] = 0.0
    mask = (df_goods["volume_m3"] > 0) & (df_goods["weight_kg"] > 0)
    df_goods.loc[mask, "kg_per_m3"] = (
        df_goods.loc[mask, "volume_m3"] / df_goods.loc[mask, "nb_units"]
    )

    df_goods = (
        df_goods.groupby("good_type", observed=True)
        .apply(_get_goods_weight_kg)
        .reset_index(drop=True)
    )

    df_goods = df_goods.drop(columns=["good_type_other", "kg_per_m3", "kg_per_unit"])

    # scale weight_kg with nb of operations per week
    df_goods.loc[:, "weight_kg"] = df_goods["weight_kg"] * df_goods["frequency"]
    df_goods = df_goods.drop(columns=["frequency"])
    
    # assign move_type for rows with both delivery and pickup
    mask = df_goods["move_type"] == "both"
    df_goods.loc[mask, "move_type"] = df_goods.loc[mask, "move_type"].apply(
        lambda x: np.random.choice(["delivery", "pickup"])
    )

    df_goods = (
        df_goods.groupby(
            ["establishment_id", "move_type", "good_type", "move_mode"], observed=True
        )[["weight_kg", "operation_weight"]]
        .sum()
        .reset_index()
    )
    df_goods = df_goods.astype({"move_type": "category", "good_type": "category", "move_mode": "category"})
 
    df_vehicles.loc[:, "nb_cars"] = df_vehicles["nb_cars"].replace(999999999, 0)
    df_vehicles.loc[:, "nb_vans_small"] = df_vehicles["nb_vans_small"].replace(
        999999999, 0
    )
    df_vehicles.loc[:, "nb_vans_big"] = df_vehicles["nb_vans_big"].replace(999999999, 0)
    df_vehicles.loc[:, "nb_trucks_7t5"] = df_vehicles["nb_trucks_7t5"].replace(
        999999999, 0
    )
    df_vehicles.loc[:, "nb_trucks_12t"] = df_vehicles["nb_trucks_12t"].replace(
        999999999, 0
    )
    df_vehicles.loc[:, "nb_trucks_19t"] = df_vehicles["nb_trucks_19t"].replace(
        999999999, 0
    )
    df_vehicles.loc[:, "nb_trucks_32t"] = df_vehicles["nb_trucks_32t"].replace(
        999999999, 0
    )
    df_vehicles.loc[:, "nb_articuated_28t"] = df_vehicles["nb_articuated_28t"].replace(
        999999999, 0
    )
    df_vehicles.loc[:, "nb_articuated_40t"] = df_vehicles["nb_articuated_40t"].replace(
        999999999, 0
    )

    relation_directions = ["receive", "send"]
    relation_types = ["center", "wholesaler", "supplier", "client", "other"]

    df_relations.loc[df_relations["send_other_part"] == 190, "send_other_part"] = 100.0
    df_relations.loc[df_relations["send_other_part_cost"] == 190, "send_other_part_cost"] = 100.0

    for direction in relation_directions:
        for type in relation_types:
            column_base = "%s_%s" % (direction, type)
            
            # nb is 999999999 but eihter part or part_cost is valid
            mask = (df_relations[column_base + "_nb"] == 999999999)
            mask &= (
                df_relations[column_base + "_part"].between(0, 100, inclusive="right") | 
                df_relations[column_base + "_part_cost"].between(0, 100, inclusive="right")
            )
            df_relations.loc[mask, column_base + "_nb"] = 1
            
            # nb is 999999999 but eihter part or part_cost is 0
            mask = (df_relations[column_base + "_nb"] == 999999999)
            mask &= ((df_relations[column_base + "_part"] == 0.0) | (df_relations[column_base + "_part"] > 999))
            mask &= ((df_relations[column_base + "_part_cost"] == 0.0) | (df_relations[column_base + "_part_cost"] > 999))
            df_relations.loc[mask, column_base + "_nb"] = 0

            # nb is > 0 but part is 999999999 but part_cost is valid
            mask = (df_relations[column_base + "_nb"] > 0)
            mask &= (df_relations[column_base + "_part"] > 100)
            mask &= (df_relations[column_base + "_part_cost"].between(0, 100, inclusive="right"))
            df_relations.loc[mask, column_base + "_part"] = df_relations.loc[mask, column_base + "_part_cost"]

            # part and part_cost is 999999999
            mask = (df_relations[column_base + "_nb"] > 0)
            mask &= (df_relations[column_base + "_part"] > 100)
            mask &= (df_relations[column_base + "_part_cost"] > 100)
            df_relations.loc[mask, column_base + "_part"] = 100

            df_relations.drop(columns=[column_base + "_part_cost"])

    # rearrange relations
    relations = []
    for row in df_relations.to_dict(orient="records"):
        establishment_id = row["establishment_id"]
        for direction in relation_directions:
            for type in relation_types:
                column_nb_name = "%s_%s_nb" % (direction, type)
                column_part_name = "%s_%s_part" % (direction, type)
                nb_contract = row[column_nb_name]
                if row[column_nb_name] > 0:
                    item = {
                        "establishment_id" : establishment_id,
                        "direction": direction,
                        "type": type,
                        "nb_contract": nb_contract,
                        "part_pct": row[column_part_name]
                    }
                    relations.append(item)
    
    df_relations = pd.DataFrame.from_dict(relations).astype({
        "establishment_id": str,
        "direction": "category",
        "type": "category",
        "nb_contract": np.int32,
        "part_pct": float,
    })

    return df_establishments, df_goods, df_vehicles, df_relations
