import pandera as pa


def configure(context):
    sirene_cleaning_method = context.config("sirene_cleaning_method", "eqasim")

    if sirene_cleaning_method == "eqasim":
        context.stage("data.sirene.cleaned.eqasim", alias="cleaned")
    elif sirene_cleaning_method == "adrien":
        context.stage("data.sirene.cleaned.adrien", alias="cleaned")
    elif sirene_cleaning_method == "horizon":
        context.stage("data.sirene.cleaned.horizon", alias="cleaned")
    else:
        raise RuntimeError(
            "Unknown Sirenen Cleaning Method: %s" % sirene_cleaning_method
        )


def execute(context):
    gdf_sirene = context.stage("cleaned")
    pa.DataFrameSchema(
        {
            "siren": pa.Column("int32"),
            "siret": pa.Column(int),
            "municipality_id": pa.Column(str),
            "suburb_type": pa.Column("category"),
            "employees": pa.Column(int),
            "ape": pa.Column(str),
            "law_status": pa.Column(str),
            "st8": pa.Column(int),
            "st20": pa.Column(int),
            "st45": pa.Column(str),
            "geometry": pa.Column("geometry"),
        }
    ).validate(gdf_sirene)
    return gdf_sirene
