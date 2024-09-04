import pandas as pd

"""
This stages loads a file containing all spatial codes in France and how
they can be translated into each other. These are mainly IRIS, commune,
departement and région.
"""


def configure(context):
    context.stage("data.spatial.download.codes")

    context.config("regions", [])
    context.config("departments", [])
    context.config("municipalities", [])


def execute(context):
    # Load IRIS registry

    codes_file = "%s/%s" % (
        context.path("data.spatial.download.codes"),
        context.stage("data.spatial.download.codes"),
    )
    df_codes = pd.read_excel(codes_file, skiprows=5, sheet_name="Emboitements_IRIS")[
        ["CODE_IRIS", "DEPCOM", "DEP", "REG"]
    ].rename(
        columns={
            "CODE_IRIS": "iris_id",
            "DEPCOM": "municipality_id",
            "DEP": "department_id",
            "REG": "region_id",
        }
    )

    df_codes["iris_id"] = df_codes["iris_id"].astype("category")
    df_codes["municipality_id"] = df_codes["municipality_id"].astype("category")
    df_codes["department_id"] = df_codes["department_id"].astype("category")
    df_codes["region_id"] = df_codes["region_id"].astype(int)

    # Filter zones
    requested_regions = list(map(int, context.config("regions")))
    requested_departments = list(map(str, context.config("departments")))
    requested_municipalities = list(map(str, context.config("municipalities")))

    if len(requested_regions) > 0:
        df_codes = df_codes[df_codes["region_id"].isin(requested_regions)]

    if len(requested_departments) > 0:
        df_codes = df_codes[df_codes["department_id"].isin(requested_departments)]

    if len(requested_municipalities) > 0:
        df_codes = df_codes[df_codes["municipality_id"].isin(requested_municipalities)]

    df_codes["iris_id"] = df_codes["iris_id"].cat.remove_unused_categories()
    df_codes["municipality_id"] = df_codes[
        "municipality_id"
    ].cat.remove_unused_categories()
    df_codes["department_id"] = df_codes["department_id"].cat.remove_unused_categories()

    return df_codes
