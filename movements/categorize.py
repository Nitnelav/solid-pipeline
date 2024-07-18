import numpy as np
import pandera as pa

from random import Random

from .utils import ST8_DISTRIBUTIONS, ST20_DISTRIBUTIONS, ALL_DISTRIBUTIONS


def configure(context):
    context.config("seed", 42)
    context.stage("movements.all")


def _caracterize(move, rand: Random):
    data = {}
    keys = list(ALL_DISTRIBUTIONS["route_type"].keys())
    values = list(ALL_DISTRIBUTIONS["route_type"].values())
    data["route_type"] = rand.choices(keys, values)[0]

    keys = list(ST8_DISTRIBUTIONS[move["st8"]]["move_type"].keys())
    values = list(ST8_DISTRIBUTIONS[move["st8"]]["move_type"].values())
    data["move_type"] = rand.choices(keys, values)[0]

    keys = list(ST8_DISTRIBUTIONS[move["st8"]]["move_mode"].keys())
    values = list(ST8_DISTRIBUTIONS[move["st8"]]["move_mode"].values())
    data["move_mode"] = rand.choices(keys, values)[0]

    keys = list(ST20_DISTRIBUTIONS[move["st20"]]["vehicle"].keys())
    values = list(ST20_DISTRIBUTIONS[move["st20"]]["vehicle"].values())
    data["vehicle"] = rand.choices(keys, values)[0]

    return data


def _get_distance(move):
    result = 0
    if move["route_type"] == "direct":
        result += 13.4637
    if move["move_mode"] == "CA":
        result += -1.454
    if move["vehicle"] == "PL":
        result += 3.6464

    result += ([1, 1, 1, 0, 0, 0, 0, 1][move["st8"] - 1]) * 1.9224

    result += move["hub_distance"] * 0.4507

    result = result * -np.log(np.random.rand())

    return result


def execute(context):
    gdf_movements = context.stage("movements.all")
    pa.DataFrameSchema(
        {
            "siret": pa.Column(int),
            "st8": pa.Column(int),
            "st20": pa.Column(int),
            "geometry": pa.Column("geometry"),
        }
    ).validate(gdf_movements)

    rand = Random(context.config("seed"))

    gdf_movements[["route_type", "move_type", "move_mode", "vehicle"]] = (
        gdf_movements.apply(
            lambda x: _caracterize(x, rand), axis=1, result_type="expand"
        )
    )

    gdf_movements["distance"] = gdf_movements.apply(_get_distance, axis=1)

    return gdf_movements
