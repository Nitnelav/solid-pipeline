import pandas as pd
import numpy as np

"""
Clean the SIRENE enterprise census.
"""
 
def configure(context):
    context.stage("data.sirene.raw_siren", ephemeral = True)
    context.stage("data.sirene.raw_siret", ephemeral = True)
    context.stage("data.spatial.codes")

def execute(context):
    df_sirene_establishments = context.stage("data.sirene.raw_siret")
    df_sirene_headquarters = context.stage("data.sirene.raw_siren")

    # Filter out establishments without a corresponding headquarter
    df_sirene = df_sirene_establishments[df_sirene_establishments["siren"].isin(df_sirene_headquarters["siren"])].copy()

    return df_sirene
