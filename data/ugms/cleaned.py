import pandera as pa
import numpy as np

from ..sirene.cleaned.utils import get_st20


def configure(context):
    context.stage("data.ugms.raw")


def execute(context):
    df_establishments, df_goods, df_vehicles = context.stage("data.ugms.raw")

    # Validate the data
    df_establishments = pa.DataFrameSchema(
        {
            "establishment_id": pa.Column(str),
            "st8": pa.Column(np.int8),
            "st45": pa.Column(str),
            "ape": pa.Column(str),
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
            "move_type": pa.Column(str),
            "good_type": pa.Column(str),
            "good_type_other": pa.Column(str),
            "packaging": pa.Column(str),
            "nb_units": pa.Column(np.int32),
            "weight_kg": pa.Column(np.int32),
            "volume_m3": pa.Column(np.int32),
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

    df_establishments["st20"] = df_establishments.apply(
        lambda x: get_st20(x["st8"], x["employees"]), axis=1
    ).astype(np.int8)
    df_establishments["st45"] = df_establishments["st45"].astype("category")
    df_establishments["ape"] = df_establishments["ape"].astype("category")

    df_goods["move_type"] = (
        df_goods["move_type"]
        .map({"r": "delivery", "e": "pickup", "999999999": np.nan})
        .astype("category")
    )

    df_goods["good_type"] = np.where(
        (df_goods["good_type"] == "autre")
        & (df_goods["good_type_other"] != "999999999"),
        df_goods["good_type"] + "_" + df_goods["good_type_other"],
        df_goods["good_type"],
    )

    df_goods["good_type"] = (
        df_goods["good_type"].replace("999999999", np.nan).astype("category")
    )
    df_goods["packaging"] = (
        df_goods["packaging"].replace("999999999", np.nan).astype("category")
    )

    df_goods["nb_units"].replace(999999999, 0, inplace=True)
    df_goods["weight_kg"].replace(999999999, 0, inplace=True)
    df_goods["volume_m3"].replace(999999999, 0, inplace=True)

    df_goods = df_goods.drop(columns=["good_type_other"])

    df_vehicles = df_vehicles[df_vehicles["has_vehicles"]]
    df_vehicles = df_vehicles.drop(columns=["has_vehicles"])

    df_vehicles["nb_cars"].replace(999999999, 0, inplace=True)
    df_vehicles["nb_vans_small"].replace(999999999, 0, inplace=True)
    df_vehicles["nb_vans_big"].replace(999999999, 0, inplace=True)
    df_vehicles["nb_trucks_7t5"].replace(999999999, 0, inplace=True)
    df_vehicles["nb_trucks_12t"].replace(999999999, 0, inplace=True)
    df_vehicles["nb_trucks_19t"].replace(999999999, 0, inplace=True)
    df_vehicles["nb_trucks_32t"].replace(999999999, 0, inplace=True)
    df_vehicles["nb_articuated_28t"].replace(999999999, 0, inplace=True)
    df_vehicles["nb_articuated_40t"].replace(999999999, 0, inplace=True)

    return df_establishments, df_goods, df_vehicles
