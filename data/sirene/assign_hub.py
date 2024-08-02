import pandera as pa


def configure(context):
    context.stage("data.sirene.cleaned.selected")
    context.stage("data.hubs.list")


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

    gdf_hubs = context.stage("data.hubs.list")
    pa.DataFrameSchema(
        {
            "id": pa.Column(int),
            "geometry": pa.Column("geometry"),
        }
    ).validate(gdf_hubs)

    gdf_sirene["hub_id"] = gdf_sirene.apply(
        lambda x: gdf_hubs.distance(x.geometry).idxmin(), axis=1
    )
    gdf_sirene["hub_distance"] = gdf_sirene.apply(
        lambda x: x.geometry.distance(gdf_hubs.loc[x["hub_id"]].geometry), axis=1
    )

    return gdf_sirene
