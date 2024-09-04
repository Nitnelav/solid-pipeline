import pandas as pd
import pandera as pa
import numpy as np


def configure(context):
    context.stage("synthesis.sirene.sampled")
    context.stage("data.ugms.goods_distributions")

def _assign_goods(context, sirene):
    df_good_weight_distributions = context.data("df_good_weight_distributions")

    siret_id = sirene["siret"]
    st8 = sirene["st8"]

    df_st8 = df_good_weight_distributions.loc[df_good_weight_distributions["st8"] == st8]

    nb_goods_lines = sirene["nb_goods_lines"]
    nb_goods_lines = min(nb_goods_lines, len(df_st8))

    goods = []
    rows = df_st8.sample(n=nb_goods_lines, weights=df_st8["weight_coeff"])
    for row in rows.to_dict(orient="records"):
        good = {   
            "siret": siret_id,
            "st8": st8,
            "move_type": row["move_type"],
            "good_type": row["good_type"],
            "move_mode": row["move_mode"],
        }
        mean = row["mean"]
        std = row["std"]
        good["weight_kg"] = abs(np.random.normal(mean, std))

        goods.append(good)

    context.progress.update()
    return goods

def _assign_nb_lines(context, sirene):
    df_establishment_line_count = context.data("df_establishment_line_count")
    st8 = sirene["st8"]

    df = df_establishment_line_count[df_establishment_line_count["st8"] == st8]
    result = df.sample(n=1, weights=df["percentage"])
    nb_lines = result["size"].values[0]

    context.progress.update()
    return nb_lines

def execute(context):
    gdf_sirene = context.stage("synthesis.sirene.sampled")
    df_establishment_line_count, df_good_weight_distributions = context.stage(
        "data.ugms.goods_distributions"
    )

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
            # "hub_id": pa.Column(int),
            # "hub_distance": pa.Column(float),
            "geometry": pa.Column("geometry"),
        }
    ).validate(gdf_sirene)

    df_establishment_line_count = pa.DataFrameSchema(
        {
            "st8": pa.Column(np.int32),
            "size": pa.Column(int),
            "percentage": pa.Column(float),
        }
    ).validate(df_establishment_line_count)

    df_good_weight_distributions = pa.DataFrameSchema(
        # index=pa.MultiIndex(
        #     [
        #         pa.Index(np.int32, name="st8"),
        #         pa.Index(str, name="move_type"),
        #         pa.Index(str, name="good_type"),
        #         pa.Index(str, name="move_mode"),
        #     ]
        # ),
        columns={
            "st8": pa.Column(np.int32),
            "move_type": pa.Column(str),
            "good_type": pa.Column(str),
            "move_mode": pa.Column(str),
            "mean": pa.Column(float),
            "std": pa.Column(float),
            "weight_coeff": pa.Column(float),
        },
    ).validate(df_good_weight_distributions)

    sirene_nb_lines = []
    with context.progress(label="Assigning Nb Lines ...", total=len(gdf_sirene)):
        with context.parallel(
            {
                "df_establishment_line_count": df_establishment_line_count,
            }
        ) as parallel:
            for nb_lines in parallel.imap(_assign_nb_lines, gdf_sirene.to_dict(orient="records")):
                sirene_nb_lines.append(nb_lines)
    
    gdf_sirene["nb_goods_lines"] = pd.DataFrame(sirene_nb_lines).values

    sirene_goods = []
    with context.progress(label="Assigning Goods ...", total=len(gdf_sirene)):
        with context.parallel(
            {
                "df_good_weight_distributions": df_good_weight_distributions,
            }
        ) as parallel:
            for goods in parallel.imap(_assign_goods, gdf_sirene.to_dict(orient="records")):
                sirene_goods += goods

    df_sirene_goods = pd.DataFrame(sirene_goods)
    
    if True:
        df_sirene_goods.groupby(["good_type", "move_mode", "move_type"])["weight_kg"].describe().reset_index().to_csv("sirene_sampled_goods_IdF_2pct.csv", sep=";")

    return gdf_sirene, df_sirene_goods