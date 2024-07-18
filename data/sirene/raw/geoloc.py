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
    df_siret_geoloc = None

    with context.progress(label="Reading geolocalized SIRET ...") as progress:
        csv = pd.read_csv(
            geoloc_file,
            usecols=COLUMNS_DTYPES.keys(),
            sep=";",
            dtype=COLUMNS_DTYPES,
            chunksize=10240,
        )

        for df_chunk in csv:
            progress.update(len(df_chunk))

            f = df_chunk["siret"].isna()  # Just to get a mask

            for municipality in requested_municipalities:
                f |= df_chunk["plg_code_commune"].astype(str) == municipality

            if df_siret_geoloc is None:
                df_siret_geoloc = df_chunk[f]
            else:
                df_siret_geoloc = pd.concat(
                    [df_siret_geoloc, df_chunk[f]], ignore_index=True
                )

    return df_siret_geoloc
