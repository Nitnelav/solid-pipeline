import pandas as pd
import pandera as pa

"""
Clean the SIRENE enterprise census.
"""
 
def configure(context):
    context.stage("data.sirene.raw.siren", ephemeral = True)
    context.stage("data.sirene.raw.siret", ephemeral = True)
    context.stage("data.spatial.codes")

def execute(context):

    df_sirene_establishments = context.stage("data.sirene.raw.siret")
    pa.DataFrameSchema({
        "siren": pa.Column("int32"), 
        "siret": pa.Column("int64"), 
        "codeCommuneEtablissement": pa.Column("str"),
        "activitePrincipaleEtablissement": pa.Column("str", nullable=True), 
        "trancheEffectifsEtablissement": pa.Column("str", nullable=True),
        "etatAdministratifEtablissement": pa.Column("str")
    }).validate(df_sirene_establishments)

    df_sirene_headquarters = context.stage("data.sirene.raw.siren")
    pa.DataFrameSchema({
        "siren": pa.Column("int32"), 
        "categorieJuridiqueUniteLegale": pa.Column("str"), 
    }).validate(df_sirene_headquarters)

    df_codes = context.stage("data.spatial.codes")
    pa.DataFrameSchema({
        'iris_id': pa.Column("str"),
        'municipality_id': pa.Column("str"),
        'department_id': pa.Column("str"),
        'region_id': pa.Column("int32")
    }).validate(df_codes)

    # Filter out establishments without a corresponding headquarter
    df_sirene = df_sirene_establishments[df_sirene_establishments["siren"].isin(df_sirene_headquarters["siren"])].copy()

    return df_sirene