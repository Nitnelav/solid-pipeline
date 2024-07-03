import numpy as np
import pandas as pd
import geopandas as gpd
import os

"""
Provides the municipality zoning system.
"""

def configure(context):
    context.stage("data.spatial.municipalities")

def execute(context):
    df_departments = context.stage("data.spatial.municipalities").dissolve(
        by = "department_id").drop(columns = ["municipality_id", "has_iris"]).reset_index()

    return df_departments
