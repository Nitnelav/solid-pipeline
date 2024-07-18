import pandas as pd

ESTABLISHEMENT_COLUMNS = {
    "Idetab": "establishment_id",
    "ST45-OK": "st45",
    "ST8-OK": "st8",
    "Apet_ok": "ape",
    "Effec_total": "employees",
    "MVTS-OK": "nb_movements",
    "REC-OK": "nb_deliveries",
    "EXP-OK": "nb_pickups",
    "CONJ-OK": "nb_pickups_and_deliveries",
}

GOODS_COLUMNS = {
    "Idétab": "establishment_id",
    "Type_operation": "move_type",
    "Nature_marchandise": "good_type",
    "Nature_marchandise_autre": "good_type_other",
    "Conditionnement": "packaging",
    "Nb_unités": "nb_units",
    "Pds_kg": "weight_kg",
    "Vol_m3": "volume_m3",
}

VEHICLES_COLUMNS = {
    "Idetab": "establishment_id",
    "Véhicules": "has_vehicles",
    "Vélos_triport_total": "nb_bicycles",
    "Motos_total": "nb_motorcycles",
    "Voiture_total": "nb_cars",
    "Fourgo_total": "nb_small_vans",
    "Camio_total": "nb_big_vans",
    "Port_7t5_total": "nb_trucks_7t5",
    "Port_12t_total": "nb_trucks_12t",
    "Port_19t_total": "nb_trucks_19t",
    "Port_32t_total": "nb_trucks_32t",
    "Art_28t_total": "nb_articuated_28t",
    "Art_40t_total": "nb_articuated_40t",
}


def configure(context):
    context.config("ugms_file_path")


def execute(context):
    file_path = context.config("ugms_file_path")
    sheets = ["1188 ETAB-MVT-ULTIME-OK", "etab_marchandises", "Etab_Véhicules"]
    ugms_xls: dict[str, pd.DataFrame] = pd.read_excel(file_path, sheet_name=sheets)
    df_establishments, df_goods, df_vehicles = ugms_xls.values()

    df_establishments.rename(columns=ESTABLISHEMENT_COLUMNS, inplace=True)
    df_establishments = df_establishments[ESTABLISHEMENT_COLUMNS.values()]

    df_goods.rename(columns=GOODS_COLUMNS, inplace=True)
    df_goods = df_goods[GOODS_COLUMNS.values()]

    df_vehicles.rename(columns=VEHICLES_COLUMNS, inplace=True)
    df_vehicles = df_vehicles[VEHICLES_COLUMNS.values()]

    return df_establishments, df_goods, df_vehicles
