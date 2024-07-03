import pandas as pd
import numpy as np

"""
Clean the SIRENE enterprise census.
"""
 
def configure(context):
    context.stage("data.sirene.raw.siren", ephemeral = True)
    context.stage("data.sirene.raw.siret", ephemeral = True)
    context.stage("data.spatial.codes")

def execute(context):
    df_sirene_establishments = context.stage("data.sirene.raw.siret")
    df_sirene_headquarters = context.stage("data.sirene.raw.siren")

    df_codes = context.stage("data.spatial.codes")

    # Filter out establishments without a corresponding headquarter
    df_sirene = df_sirene_establishments[df_sirene_establishments["siren"].isin(df_sirene_headquarters["siren"])].copy()

    # Remove inactive enterprises
    df_sirene = df_sirene[
        df_sirene["etatAdministratifEtablissement"] == "A"
    ].copy()

    # Define work place weights by person under salary ....
    df_sirene["minimum_employees"] = 1 # Includes "NN", "00", and NaN
    df_sirene["maximum_employees"] = 1 # Includes "NN", "00", and NaN
    df_sirene["employees"] = 1 # Includes "NN", "00", and NaN

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
        "53": (10000, 10000)
    }

    for key, value in employee_ranges.items():
        min_ = int(value[0])
        max_ = int(value[1])
        df_sirene.loc[df_sirene["trancheEffectifsEtablissement"] == key, "minimum_employees"] = min_
        df_sirene.loc[df_sirene["trancheEffectifsEtablissement"] == key, "maximum_employees"] = max_
        # further down the line, eqasim will use the minimum number of employees as weights so setting employees to minimum
        df_sirene.loc[df_sirene["trancheEffectifsEtablissement"] == key, "employees"] = min_
        
    # Add activity classification
    df_sirene["ape"] = df_sirene["activitePrincipaleEtablissement"]

    # Check communes
    df_sirene["municipality_id"] = df_sirene["codeCommuneEtablissement"].astype("category")
    
    requested_municipalities = set(df_codes["municipality_id"].unique())
    excess_municipalities = set(df_sirene["municipality_id"].unique()) - requested_municipalities

    if len(excess_municipalities) > 0:
        print("Found excess municipalities in SIRENE data: ", excess_municipalities)

    if len(excess_municipalities) > 5:
        raise RuntimeError("Found more than 5 excess municipalities in SIRENE data")

    df_sirene = df_sirene[["siren", "municipality_id", "employees", "minimum_employees", "maximum_employees", "ape", "siret"]]
        
    # Add law status
    initial_count = len(df_sirene)

    df_sirene = pd.merge(df_sirene, df_sirene_headquarters, on = "siren")

    df_sirene["law_status"] = df_sirene["categorieJuridiqueUniteLegale"]
    df_sirene = df_sirene.drop(columns =  ["categorieJuridiqueUniteLegale"])

    final_count = len(df_sirene)
    assert initial_count == final_count

    return df_sirene
