import geopandas as gpd
import pandas as pd
import pandera as pa

from ..categories import APE_ST_CODES
from .employees import TEE_ST_EMPLOYEES

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
    pa.DataFrameSchema({
        "siren": pa.Column("int32"), 
        "siret": pa.Column("int64"), 
        "activitePrincipaleEtablissement": pa.Column("str", nullable=True), 
        "trancheEffectifsEtablissement": pa.Column("str", nullable=True),
        "etatAdministratifEtablissement": pa.Column("str"),
        "codeCommuneEtablissement": pa.Column("str"),
        "numeroVoieEtablissement": pa.Column("str", nullable=True),
        "typeVoieEtablissement": pa.Column("str", nullable=True),
        "libelleVoieEtablissement": pa.Column("str", nullable=True)
    }).validate(df_sirene_establishments)

    df_sirene_headquarters = context.stage("data.sirene.raw.siren")
    pa.DataFrameSchema({
        "siren": pa.Column("int32"), 
        "categorieJuridiqueUniteLegale": pa.Column("str"), 
    }).validate(df_sirene_headquarters)

    df_siret_geoloc = context.stage("data.sirene.raw.geoloc")
    pa.DataFrameSchema({
        "siret": pa.Column("int64"),
        "x": pa.Column("float"),
        "y": pa.Column("float")
    }).validate(df_siret_geoloc)

    df_codes = context.stage("data.spatial.codes")
    pa.DataFrameSchema({
        'iris_id': pa.Column("str"),
        'municipality_id': pa.Column("str"),
        'department_id': pa.Column("str"),
        'region_id': pa.Column("int32")
    }).validate(df_codes)

    # Filter out establishments without a corresponding headquarter
    df_sirene = df_sirene_establishments[df_sirene_establishments["siren"].isin(df_sirene_headquarters["siren"])].copy()

    # Remove inactive enterprises
    df_sirene = df_sirene[
        df_sirene["etatAdministratifEtablissement"] == "A"
    ].copy()

    # Remove enterprises with 0 or invalid number of employees
    df_sirene = df_sirene[
        ~df_sirene["trancheEffectifsEtablissement"].isna()
        # & ~df_sirene["trancheEffectifsEtablissement"].isin(["NN", "00"])
    ]

    # Add activity classification
    df_sirene["ape"] = df_sirene["activitePrincipaleEtablissement"]

    # assign ST45 and ST8
    df_sirene["st8"] = 0
    df_sirene["st45"] = "0"

    for ape, st in APE_ST_CODES.items():
        df_sirene.loc[df_sirene["ape"] == ape, "st8"] = int(st['ST8'])
        df_sirene.loc[df_sirene["ape"] == ape, "st45"] = str(st['ST45'])

    # TODO : add a check to see if all APE codes are in the dictionary
    df_sirene = df_sirene[df_sirene["st8"] != 0]

    # Check communes
    df_sirene["municipality_id"] = df_sirene["codeCommuneEtablissement"].astype("category")
    
    requested_municipalities = set(df_codes["municipality_id"].unique())
    excess_municipalities = set(df_sirene["municipality_id"].unique()) - requested_municipalities

    if len(excess_municipalities) > 0:
        print("Found excess municipalities in SIRENE data: ", excess_municipalities)

    if len(excess_municipalities) > 5:
        raise RuntimeError("Found more than 5 excess municipalities in SIRENE data")
    
    # Set the number of employees
    df_sirene["employees"] = 0
 
    for row in TEE_ST_EMPLOYEES:
        tee = str(row["TEE"]).zfill(2)
        st8 = int(row["ST8"])
        employees = int(row["EMPLOYEES"])
        df_sirene.loc[
            (df_sirene["trancheEffectifsEtablissement"] == tee) &
            (df_sirene["st8"] == st8), "employees"
        ] = employees

    # Add law status
    initial_count = len(df_sirene)

    df_sirene = pd.merge(df_sirene, df_sirene_headquarters, on = "siren")

    df_sirene["law_status"] = df_sirene["categorieJuridiqueUniteLegale"]
    df_sirene = df_sirene.drop(columns =  ["categorieJuridiqueUniteLegale"])

    final_count = len(df_sirene)
    assert initial_count == final_count

    # Filter on law_status
    unwanted_law_status = {
        'all': list(map(str, [
            2110, 2120, # (FR) Indivision personne morale|physique 
            5195, 5470, 5525, 5530, 5531, 5532, 5620, # various rural companies
            # Companies that are just here to share a real-estate property (SCI etc.)
            6521, 6534, 6535, 6536, 6538, 6539, 6542, 6543, 6558, 6595
        ])),
        'more_than_0': list(map(str, [
            2310, 2320, 2385, 2900, # (FR) societes en participation
            3110, # (FR) representation d'un etat ou organisme etranger en france
            5191, # (FR) caisse de caution mutuelle
            6210, 6220, # (FR) GIE et GEIE
            6599, # (FR) Autres societes civiles
            9210, 9220, 9221, 9222, 9230, 9300 # (FR) Associations, syndicats, etc.
        ])),
        'more_than_1': list(map(str, [
            1000, # (FR) Entrepreneur individuel
            2110, 2120, # (FR) Indivision personne morale|physique 
            # Companies that are just here to share a real-estate property (SCI etc.)
            6540, 6541, 6560, 6589, 6596,
            9110 # (FR) syndicat de copropriete
        ]))
    }

    to_remove = df_sirene["law_status"].isin(unwanted_law_status["all"])
    to_remove |= df_sirene["law_status"].isin(unwanted_law_status["more_than_0"]) & (df_sirene["employees"] > 0)
    to_remove |= df_sirene["law_status"].isin(unwanted_law_status["more_than_1"]) & (df_sirene["employees"] > 1)
    df_sirene = df_sirene[~to_remove]

    # merging geographical SIREN file (containing only SIRET and location) with full SIREN file (all variables and processed)
    df_sirene = df_sirene.join(df_siret_geoloc.set_index('siret'), on='siret', how="left")
    df_sirene.dropna(subset=['x', 'y'],inplace=True)

    # convert to geopandas dataframe with Lambert 93, EPSG:2154 french official projection
    df_sirene = gpd.GeoDataFrame(df_sirene, geometry=gpd.points_from_xy(df_sirene.x, df_sirene.y),crs="EPSG:2154")

    # Count the number of rows with the same geometry
    df_sirene["geometry_count"] = df_sirene.groupby("geometry")["geometry"].transform("count")
    
    # TODO : filter if more than 30 companies at the same location
    df_sirene = df_sirene.groupby("geometry").apply(lambda x: x.nlargest(30, "employees")).reset_index(drop=True)

    df_sirene['st20'] = df_sirene.apply(lambda x: get_st20(x['st8'], x['employees']), axis=1)

    # cleanup columns
    df_sirene = df_sirene[[
        "siren", "siret", "municipality_id", "employees", "ape", "law_status", 'st8', 'st20', 'st45', "geometry"
    ]]
    return df_sirene