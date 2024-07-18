import pandas as pd
import geopandas as gpd

"""
Loads the IRIS zoning system.
"""


def configure(context):
    context.stage("data.spatial.download.iris")
    context.stage("data.spatial.codes")


def execute(context):
    df_codes = context.stage("data.spatial.codes")

    iris_file = "%s/%s" % (
        context.path("data.spatial.download.iris"),
        context.stage("data.spatial.download.iris"),
    )
    df_iris = gpd.read_file(iris_file)[["CODE_IRIS", "INSEE_COM", "geometry"]].rename(
        columns={"CODE_IRIS": "iris_id", "INSEE_COM": "municipality_id"}
    )
    df_iris.crs = dict(init="EPSG:2154")

    df_iris["iris_id"] = df_iris["iris_id"].astype("category")
    df_iris["municipality_id"] = df_iris["municipality_id"].astype("category")

    # Merge with requested codes and verify integrity
    df_iris = pd.merge(df_iris, df_codes, on=["iris_id", "municipality_id"])

    requested_iris = set(df_codes["iris_id"].unique())
    merged_iris = set(df_iris["iris_id"].unique())

    if requested_iris != merged_iris:
        raise RuntimeError(
            "Some IRIS are missing: %s" % (requested_iris - merged_iris,)
        )

    return df_iris
