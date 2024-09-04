import pandas as pd

"""
This stage loads the geolocalization data for the French enterprise registry.
"""


def configure(context):
    context.stage("data.sirene.download.geoloc")
    context.stage("data.spatial.codes")


def execute(context):
    # Filter by department
    df_codes = context.stage("data.spatial.codes")
    requested_municipalities = set(df_codes["municipality_id"].unique())

    COLUMNS_DTYPES = {
        "siret": "int64",
        "x": "float",
        "y": "float",
        "plg_code_commune": "str",
    }

    geoloc_file = "%s/%s" % (
        context.path("data.sirene.download.geoloc"),
        context.stage("data.sirene.download.geoloc"),
    )

    df_siret_geoloc = pd.read_csv(
        geoloc_file,
        usecols=COLUMNS_DTYPES.keys(),
        sep=";",
        dtype=COLUMNS_DTYPES,
        na_filter=False,
        engine="pyarrow",
    )
    df_siret_geoloc.dropna(subset=["siret"], inplace=True)
    df_siret_geoloc = df_siret_geoloc[df_siret_geoloc["plg_code_commune"].isin(requested_municipalities)]

    return df_siret_geoloc
