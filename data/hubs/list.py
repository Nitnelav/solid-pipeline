import geopandas as gpd

from shapely.geometry import Point

def configure(context):
    pass

def execute(context):

    coordinates = [
        {"id": 0, "x": 356775.282160, "y": 6691067.446850},
        {"id": 1, "x": 353711.437570, "y": 6690454.858730},
    ]

    geoms = [{"id": c["id"], "geometry": Point(c["x"], c["y"])} for c in coordinates]

    return gpd.GeoDataFrame.from_dict(geoms, geometry="geometry", crs="EPSG:2154")