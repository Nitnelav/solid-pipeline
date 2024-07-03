import pandas as pd
import pandera as pa

def configure(context):
    sirene_cleaning_method = context.config("sirene_cleaning_method", "eqasim")

    if sirene_cleaning_method == "eqasim":
        context.stage("data.sirene.cleaned.eqasim", alias = "cleaned")
    elif sirene_cleaning_method == "adrien":
        context.stage("data.sirene.cleaned.adrien", alias = "cleaned")
    elif sirene_cleaning_method == "horizon":
        context.stage("data.sirene.cleaned.horizon", alias = "cleaned")
    else:
        raise RuntimeError("Unknown Sirenen Cleaning Method: %s" % sirene_cleaning_method)

def execute(context):
    df_cleaned = context.stage("cleaned")
    schema = pa.DataFrameSchema({
        "siren": pa.Column("str"),
        "municipality_id": pa.Column("str"),
        "employees": pa.Column("int"),
        "minimum_employees": pa.Column("int"),
        "maximum_employees": pa.Column("int"),
        "ape": pa.Column("str"),
        "siret": pa.Column("str"),
        "law_status": pa.Column("str"),
    })

    schema.validate(df_cleaned)
    return df_cleaned
