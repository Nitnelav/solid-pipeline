import pandera as pa
import pandas as pd
import geopandas as gpd


def configure(context):
    context.stage("synthesis.sirene.sampled")
    context.stage("goods.assign")


def _find_supplier(context, row):
    gdf_supply: gpd.GeoDataFrame = context.data("gdf_supply")
    gdf_supply["distance"] = 999999999999.0
    gdf_supply.loc[gdf_supply["good_type"] == row["good_type"], "distance"] = (
        gdf_supply.distance(row["geometry"])
    )
    # Select the 5 closest suppliers and pick one at random
    supplier = gdf_supply.nsmallest(5, "distance").sample(n=1).iloc[0]
    context.progress.update()
    return supplier["siret"]


def execute(context):
    gdf_sirene = context.stage("synthesis.sirene.sampled")
    df_sirene_goods = context.stage("goods.assign")

    gdf_sirene = pa.DataFrameSchema(
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
            "geometry": pa.Column("geometry"),
        }
    ).validate(gdf_sirene)

    df_sirene_goods = pa.DataFrameSchema(
        {
            "siret": pa.Column("int64"),
            "st8": pa.Column("int64"),
            "move_type": pa.Column("str"),
            "good_type": pa.Column("str"),
            "move_mode": pa.Column("str"),
            "weight_kg": pa.Column("float"),
        }
    ).validate(df_sirene_goods)

    gdf_goods = gpd.GeoDataFrame(df_sirene_goods.merge(gdf_sirene, on="siret"))

    suppliers = []
    gdf_demand = gdf_goods.loc[(gdf_goods["move_type"] == "delivery")].copy()
    gdf_supply = gdf_goods.loc[(gdf_goods["move_type"] == "pickup")].copy()

    with context.progress(label="Finding suppliers ...", total=len(gdf_demand)):
        with context.parallel(
            {
                "gdf_supply": gdf_supply,
            }
        ) as parallel:
            for supplier_siret in parallel.imap(
                _find_supplier, gdf_demand.to_dict(orient="records")
            ):
                suppliers.append(supplier_siret)

    gdf_demand["supplier_siret"] = pd.DataFrame(suppliers).values
    return gdf_demand
