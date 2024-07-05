import pandas as pd
import pandera as pa

def configure(context):
    context.config("sirene_output_csv_path")
    context.stage("data.sirene.cleaned.selected")


def execute(context):
    df_cleaned = context.stage("data.sirene.cleaned.selected")  
    
    df_cleaned.to_csv(context.config("sirene_output_csv_path"), index=False, sep=";")
    return df_cleaned