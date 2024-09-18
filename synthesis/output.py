import pandera as pa

def configure(context):
    context.stage("synthesis.sirene.sampled")

def execute(context):
    gdf_sirene = context.stage("synthesis.sirene.sampled")

    schema = pa.DataFrameSchema(
        {
            "siren": pa.Column(pa.Int),
            "siret": pa.Column(pa.String),
        }
    )

    schema.validate(gdf_sirene)
    context.stage("synthesis.sirene.sampled", schema=schema)