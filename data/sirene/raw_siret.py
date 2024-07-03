import os
import pandas as pd

"""
This stage loads the raw data from the French enterprise registry.
"""

def configure(context):
    context.stage("data.sirene.download_siret")
    context.stage("data.spatial.codes")

def execute(context):
    # Filter by departement
    df_codes = context.stage("data.spatial.codes")
    requested_departements = set(df_codes["departement_id"].unique())

    df_siret = []

    siret_file = "%s/%s" % (context.path("data.sirene.download_siret"), context.stage("data.sirene.download_siret"))

    COLUMNS_DTYPES = {
        "siren":"int32", 
        "siret":"int64", 
        "codeCommuneEtablissement":"str",
        "activitePrincipaleEtablissement":"str", 
        "trancheEffectifsEtablissement":"str",
        "etatAdministratifEtablissement":"str"
    }
    
    with context.progress(label = "Reading SIRET...") as progress:
        csv = pd.read_csv(siret_file,
                          usecols = COLUMNS_DTYPES.keys(), dtype = COLUMNS_DTYPES,chunksize = 10240)

        for df_chunk in csv:
            progress.update(len(df_chunk))

            f = df_chunk["codeCommuneEtablissement"].isna() # Just to get a mask

            for departement in requested_departements:
                f |= df_chunk["codeCommuneEtablissement"].str.startswith(departement)

            f &= ~df_chunk["codeCommuneEtablissement"].isna()
            df_chunk = df_chunk[f]

            if len(df_chunk) > 0:
                df_siret.append(df_chunk)

    return pd.concat(df_siret)
