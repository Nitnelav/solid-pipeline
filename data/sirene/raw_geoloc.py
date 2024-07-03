import os
import pandas as pd

"""
This stage loads the geolocalization data for the French enterprise registry.
"""

def configure(context):
    context.stage("data.sirene.download_geoloc")
    context.stage("data.spatial.codes")

def execute(context):
    # Filter by departement
    df_codes = context.stage("data.spatial.codes")
    requested_departements = set(df_codes["departement_id"].unique())
    
    COLUMNS_DTYPES = {
        "siret":"int64", 
        "x":"float", 
        "y":"float",
        "plg_code_commune":"str",
    }

    geoloc_file = "%s/%s" % (context.path("data.sirene.download_geoloc"), context.stage("data.sirene.download_geoloc"))
    df_siret_geoloc = pd.DataFrame(columns=["siret","x","y"])
    
    with context.progress(label = "Reading geolocalized SIRET ...") as progress:
         csv = pd.read_csv(geoloc_file, 
                          usecols = COLUMNS_DTYPES.keys(), sep=";",dtype = COLUMNS_DTYPES,chunksize = 10240)
    
         for df_chunk in csv:
            progress.update(len(df_chunk))
            
            f = df_chunk["siret"].isna() # Just to get a mask
            
            for departement in requested_departements:

                f |= df_chunk["plg_code_commune"].str.startswith(departement)

            df_siret_geoloc = pd.concat([df_siret_geoloc, df_chunk[f]],ignore_index=True)

    return df_siret_geoloc

