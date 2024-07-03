import pandas as pd
import numpy as np
import pandera as pa

def configure(context):
    context.stage("data.sirene.cleaned.eqasim")
    context.stage("data.sirene.cleaned.horizon")
    context.stage("data.sirene.cleaned.adrien")

def execute(context):
    df_cleaned_eqasim = context.stage("data.sirene.cleaned.eqasim")
    df_cleaned_horizon = context.stage("data.sirene.cleaned.horizon")
    df_cleaned_adrien = context.stage("data.sirene.cleaned.adrien")

    schema = pa.DataFrameSchema({
        "siren": pa.Column("int32"),
        "municipality_id": pa.Column("str"),
        "employees": pa.Column("int"),
        "minimum_employees": pa.Column("int"),
        "maximum_employees": pa.Column("int"),
        "ape": pa.Column("str"),
        "siret": pa.Column("int64"),
        "law_status": pa.Column("str"),
    })
    schema.validate(df_cleaned_eqasim)
    schema.validate(df_cleaned_horizon)
    schema.validate(df_cleaned_adrien)

    print("Number of companies with 'esaqim method' : %s" % len(df_cleaned_eqasim))
    print("Number of remaining establishments : %s" % len(df_cleaned_horizon))
    print("Number of remaining establishments : %s" % len(df_cleaned_adrien))
