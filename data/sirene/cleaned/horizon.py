import geopandas as gpd
import pandas as pd
import pandera as pa
import random

from ..categories import APE_ST_CODES

from .utils import get_st20

"""
Clean the SIRENE enterprise census.
"""


def configure(context):
    context.stage("data.sirene.raw.geoloc")
    context.stage("data.sirene.raw.siren")
    context.stage("data.sirene.raw.siret")
    context.stage("data.spatial.codes")


def execute(context):
    df_sirene_establishments = context.stage("data.sirene.raw.siret")
    pa.DataFrameSchema(
        {
            "siren": pa.Column("int32"),
            "siret": pa.Column("int64"),
            "activitePrincipaleEtablissement": pa.Column("str", nullable=True),
            "trancheEffectifsEtablissement": pa.Column("str", nullable=True),
            "etatAdministratifEtablissement": pa.Column("str"),
            "codeCommuneEtablissement": pa.Column("str"),
            "numeroVoieEtablissement": pa.Column("str", nullable=True),
            "typeVoieEtablissement": pa.Column("str", nullable=True),
            "libelleVoieEtablissement": pa.Column("str", nullable=True),
        }
    ).validate(df_sirene_establishments)

    df_sirene_headquarters = context.stage("data.sirene.raw.siren")
    pa.DataFrameSchema(
        {
            "siren": pa.Column("int32"),
            "categorieJuridiqueUniteLegale": pa.Column("str"),
        }
    ).validate(df_sirene_headquarters)

    df_siret_geoloc = context.stage("data.sirene.raw.geoloc")
    pa.DataFrameSchema(
        {"siret": pa.Column("int64"), "x": pa.Column("float"), "y": pa.Column("float")}
    ).validate(df_siret_geoloc)

    df_codes = context.stage("data.spatial.codes")
    pa.DataFrameSchema(
        {
            "iris_id": pa.Column("str"),
            "municipality_id": pa.Column("str"),
            "department_id": pa.Column("str"),
            "region_id": pa.Column("int32"),
        }
    ).validate(df_codes)

    # Filter out establishments without a corresponding headquarter
    df_sirene = df_sirene_establishments[
        df_sirene_establishments["siren"].isin(df_sirene_headquarters["siren"])
    ].copy()

    # Remove inactive enterprises
    df_sirene = df_sirene[df_sirene["etatAdministratifEtablissement"] == "A"].copy()

    # Remove enterprises with 0 or invalid number of employees
    df_sirene = df_sirene[
        ~df_sirene["trancheEffectifsEtablissement"].isna()
        & ~df_sirene["trancheEffectifsEtablissement"].isin(["NN", "00"])
    ]

    # Set the number of employees
    employee_ranges = {
        "01": (1, 2),
        "02": (3, 5),
        "03": (6, 9),
        "11": (10, 19),
        "12": (20, 49),
        "21": (50, 99),
        "22": (100, 199),
        "31": (200, 249),
        "32": (250, 499),
        "41": (500, 999),
        "42": (1000, 1999),
        "51": (2000, 4999),
        "52": (5000, 9999),
        "53": (10000, 10000),
    }

    df_sirene["minimum_employees"] = 0
    df_sirene["maximum_employees"] = 0
    df_sirene["employees"] = 0

    for key, value in employee_ranges.items():
        min_ = int(value[0])
        max_ = value[1]
        df_sirene.loc[
            df_sirene["trancheEffectifsEtablissement"] == key, "minimum_employees"
        ] = min_
        df_sirene.loc[
            df_sirene["trancheEffectifsEtablissement"] == key, "maximum_employees"
        ] = max_
        # randommly assign a number of employees in the range
        df_sirene.loc[
            df_sirene["trancheEffectifsEtablissement"] == key, "employees"
        ] = random.randint(min_, max_)

    assert len(df_sirene[df_sirene["employees"] == 0]) == 0

    # Add activity classification
    df_sirene["ape"] = df_sirene["activitePrincipaleEtablissement"]

    # assign ST45 and ST8
    df_sirene["st8"] = 0
    df_sirene["st45"] = "0"

    for ape, st in APE_ST_CODES.items():
        df_sirene.loc[df_sirene["ape"] == ape, "st8"] = int(st["ST8"])
        df_sirene.loc[df_sirene["ape"] == ape, "st45"] = str(st["ST45"])

    # Check communes
    df_sirene["municipality_id"] = df_sirene["codeCommuneEtablissement"].astype(
        "category"
    )

    requested_municipalities = set(df_codes["municipality_id"].unique())
    excess_municipalities = (
        set(df_sirene["municipality_id"].unique()) - requested_municipalities
    )

    if len(excess_municipalities) > 0:
        print("Found excess municipalities in SIRENE data: ", excess_municipalities)

    if len(excess_municipalities) > 5:
        raise RuntimeError("Found more than 5 excess municipalities in SIRENE data")

    # Add law status
    initial_count = len(df_sirene)

    df_sirene = pd.merge(df_sirene, df_sirene_headquarters, on="siren")

    df_sirene["law_status"] = df_sirene["categorieJuridiqueUniteLegale"]
    df_sirene = df_sirene.drop(columns=["categorieJuridiqueUniteLegale"])

    final_count = len(df_sirene)
    assert initial_count == final_count

    unwanted_law_status = list(map(str, [
        1400, 1500, 1700, 1900, # (FR) Entrepreneur individuel
        2110, 2120, # (FR) Indivision personne morale|physique 
        # Companies that are just here to share a real-estate property (SCI etc.)
        6521, 6532, 6533, 6534, 6535, 6536, 6537, 6537, 6539, 6540, 6541, 6542, 6543, 6544, 6551, 6554, 6558,
        # (FR) Associations, syndicats, etc.
        9150, 9210, 9220, 9221, 9222, 9223, 9224, 9230, 9240, 9260, 9300
    ]))

    df_sirene = df_sirene[~df_sirene["law_status"].isin(unwanted_law_status)]

    # merging geographical SIREN file (containing only SIRET and location) with full SIREN file (all variables and processed)
    df_sirene = df_sirene.join(
        df_siret_geoloc.set_index("siret"), on="siret", how="left"
    )
    df_sirene.dropna(subset=["x", "y"], inplace=True)

    # convert to geopandas dataframe with Lambert 93, EPSG:2154 french official projection
    df_sirene = gpd.GeoDataFrame(
        df_sirene,
        geometry=gpd.points_from_xy(df_sirene.x, df_sirene.y),
        crs="EPSG:2154",
    )

    # Count the number of rows with the same geometry
    df_sirene["geometry_count"] = df_sirene.groupby("geometry")["geometry"].transform(
        "count"
    )

    # Remove rows with count greater than 15
    df_sirene = df_sirene[df_sirene["geometry_count"] <= 15]

    df_sirene["st20"] = df_sirene.apply(
        lambda x: get_st20(x["st8"], x["employees"]), axis=1
    )

    # cleanup columns
    df_sirene = df_sirene[
        [
            "siren",
            "siret",
            "municipality_id",
            "employees",
            "ape",
            "law_status",
            "st8",
            "st20",
            "st45",
            "geometry",
        ]
    ]

    return df_sirene
