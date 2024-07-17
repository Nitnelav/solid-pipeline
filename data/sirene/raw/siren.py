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

    siren_file = "%s/%s" % (context.path("data.sirene.download.siren"), context.stage("data.sirene.download.siren"))

    COLUMNS_DTYPES = {
        "siren":"int32", 
        "categorieJuridiqueUniteLegale":"str", 
    }
    
    with context.progress(label = "Reading SIREN...") as progress:
        csv = pd.read_csv(siren_file,
              usecols = COLUMNS_DTYPES.keys(), dtype = COLUMNS_DTYPES,chunksize = 10240)

        for df_chunk in csv:
            progress.update(len(df_chunk))

            df_chunk = df_chunk[
                df_chunk["siren"].isin(relevant_siren)
            ]

            if len(df_chunk) > 0:
                df_siren.append(df_chunk)

    return pd.concat(df_siren)
