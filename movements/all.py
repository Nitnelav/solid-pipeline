import geopandas as gpd
import pandera as pa


def configure(context):
    context.stage("movements.quantity")


def execute(context):
    gdf_sirene = context.stage("movements.quantity")
    pa.DataFrameSchema(
        {
            "siren": pa.Column("int32"),
            "siret": pa.Column(int),
            "municipality_id": pa.Column(str),
            "employees": pa.Column(int),
            "ape": pa.Column(str),
            "law_status": pa.Column(str),
            "st8": pa.Column(int),
            "st20": pa.Column(int),
            "st45": pa.Column(str),
            "hub_id": pa.Column(int),
            "hub_distance": pa.Column(float),
            "movements": pa.Column(int),
            "geometry": pa.Column("geometry"),
        }
    ).validate(gdf_sirene)

    movements = []
    for index, row in gdf_sirene.iterrows():
        data = row[
            ["siret", "st8", "st20", "hub_id", "hub_distance", "geometry"]
        ].to_dict()
        for _ in range(row["movements"]):
            movements.append(data)
    gdf_movements = gpd.GeoDataFrame.from_dict(
        movements, geometry="geometry", crs=gdf_sirene.crs
    )

    return gdf_movements
