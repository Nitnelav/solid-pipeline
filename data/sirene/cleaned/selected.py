import pandas as pd
import numpy as np

def configure(context):
    sirene_cleaning_method = context.config("sirene_cleaning_method", "eqasim")

    if sirene_cleaning_method == "eqasim":
        context.stage("data.sirene.cleaned.eqasim", alias = "cleaned")
    elif sirene_cleaning_method == "adrien":
        context.stage("data.sirene.cleaned.adrien", alias = "cleaned")
    else:
        raise RuntimeError("Unknown Sirenen Cleaning Method: %s" % sirene_cleaning_method)

def execute(context):
    return context.stage("cleaned")
