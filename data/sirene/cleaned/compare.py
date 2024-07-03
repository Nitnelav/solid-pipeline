import pandas as pd
import numpy as np

def configure(context):
    context.stage("data.sirene.cleaned.eqasim")
    context.stage("data.sirene.cleaned.adrien")
    context.stage("data.sirene.cleaned.horizon")

def execute(context):
    df_cleaned_eqasim = context.stage("data.sirene.cleaned.eqasim")
    df_cleaned_adrien = context.stage("data.sirene.cleaned.adrien")
    df_cleaned_horizon = context.stage("data.sirene.cleaned.horizon")