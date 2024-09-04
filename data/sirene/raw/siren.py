import pandas as pd

"""
This stage loads the raw data from the French enterprise registry.
"""


def configure(context):
    context.stage("data.sirene.download.siren")
    context.stage("data.sirene.raw.siret")


def execute(context):
    relevant_siren = context.stage("data.sirene.raw.siret")["siren"].unique()
    df_siren = []

    siren_file = "%s/%s" % (
        context.path("data.sirene.download.siren"),
        context.stage("data.sirene.download.siren"),
    )

    COLUMNS_DTYPES = {
        "siren": "int32",
        "categorieJuridiqueUniteLegale": "str",
    }


    df_siren = pd.read_csv(
        siren_file,
        usecols=COLUMNS_DTYPES.keys(),
        dtype=COLUMNS_DTYPES,
        na_filter=False,
        engine="pyarrow",
    )
    df_siren = df_siren[df_siren["siren"].isin(relevant_siren)]

    return df_siren
