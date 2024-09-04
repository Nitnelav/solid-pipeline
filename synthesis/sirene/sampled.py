import pandera as pa


def configure(context):
    context.config("sirene_sample_size", 1.0)
    context.stage("data.sirene.cleaned.selected")

def execute(context):
    gdf_sirene = context.stage("data.sirene.cleaned.selected")
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

    gdf_sirene = gdf_sirene.sample(frac=context.config("sirene_sample_size"))
    return gdf_sirene