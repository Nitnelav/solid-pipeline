import pandas as pd

"""
This stage loads the raw data from the French enterprise registry.
"""

def configure(context):
    context.stage("data.sirene.download.siret")
    context.stage("data.spatial.codes")

def execute(context):
    # Filter by department
    df_codes = context.stage("data.spatial.codes")
    requested_municipalities = set(df_codes["municipality_id"].unique())

    df_siret = []

    siret_file = "%s/%s" % (context.path("data.sirene.download.siret"), context.stage("data.sirene.download.siret"))

    COLUMNS_DTYPES = {
        "siren":"int32", 
        "siret":"int64", 
        "activitePrincipaleEtablissement":"str", 
        "trancheEffectifsEtablissement":"str",
        "etatAdministratifEtablissement":"str",
        "codeCommuneEtablissement":"str",
        "numeroVoieEtablissement":"str",
        "typeVoieEtablissement":"str",
        "libelleVoieEtablissement":"str",
    }
    
    with context.progress(label = "Reading SIRET...") as progress:
        csv = pd.read_csv(siret_file,
                          usecols = COLUMNS_DTYPES.keys(), dtype = COLUMNS_DTYPES,chunksize = 10240)

        for df_chunk in csv:
            progress.update(len(df_chunk))

            f = df_chunk["codeCommuneEtablissement"].isna() # Just to get a mask

            for municipality in requested_municipalities:
                f |= (df_chunk["codeCommuneEtablissement"].astype(str) == municipality)

            f &= ~df_chunk["codeCommuneEtablissement"].isna()
            df_chunk = df_chunk[f]

            if len(df_chunk) > 0:
                df_siret.append(df_chunk)

    return pd.concat(df_siret)
