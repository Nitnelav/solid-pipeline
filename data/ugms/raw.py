import pandas as pd
import numpy as np

ESTABLISHEMENT_COLUMNS = {
    "Idetab": ("establishment_id", str),
    "ST8-OK": ("st8", np.int8),
    "ST45-OK": ("st45", str),
    "Apet_ok": ("ape", str),
    "Effec_total": ("employees", np.int32),
    "MVTS-OK": ("nb_movements", np.float32),
    "REC-OK": ("nb_deliveries", np.float32),
    "EXP-OK": ("nb_pickups", np.float32),
    "CONJ-OK": ("nb_pickups_and_deliveries", np.float32),
    "COEF-REDRETAB": ("establishment_weight", np.float32),
}

GOODS_COLUMNS = {
    "Idétab": ("establishment_id", str),
    "Type_operation": ("move_type", str),
    "Nature_marchandise": ("good_type", str),
    "Nature_marchandise_autre": ("good_type_other", str),
    "Conditionnement": ("packaging", str),
    "Nb_unités": ("nb_units", np.int32),
    "Pds_kg": ("weight_kg", np.float64),
    "Vol_m3": ("volume_m3", np.float64),
}

VEHICLES_COLUMNS = {
    "Idetab": ("establishment_id", str),
    "Véhicules": ("has_vehicles", bool),
    "Vélos_triport_total": ("nb_bicycles", np.int32),
    "Motos_total": ("nb_motorcycles", np.int32),
    "Voiture_total": ("nb_cars", np.int32),
    "Fourgo_total": ("nb_vans_small", np.int32),
    "Camio_total": ("nb_vans_big", np.int32),
    "Port_7t5_total": ("nb_trucks_7t5", np.int32),
    "Port_12t_total": ("nb_trucks_12t", np.int32),
    "Port_19t_total": ("nb_trucks_19t", np.int32),
    "Port_32t_total": ("nb_trucks_32t", np.int32),
    "Art_28t_total": ("nb_articuated_28t", np.int32),
    "Art_40t_total": ("nb_articuated_40t", np.int32),
}


def configure(context):
    context.config("ugms_file_path")


def execute(context):
    file_path = context.config("ugms_file_path")
    sheets = ["1188 ETAB-MVT-ULTIME-OK", "etab_marchandises", "Etab_Véhicules"]
    ugms_xls: dict[str, pd.DataFrame] = pd.read_excel(file_path, sheet_name=sheets)
    df_establishments, df_goods, df_vehicles = ugms_xls.values()

    column_names = {k: v[0] for k, v in ESTABLISHEMENT_COLUMNS.items()}
    column_types = {v[0]: v[1]  for _, v in ESTABLISHEMENT_COLUMNS.items()}

    df_establishments.rename(columns=column_names, inplace=True)
    df_establishments = df_establishments[column_names.values()]
    df_establishments["nb_deliveries"] = df_establishments["nb_deliveries"].fillna(0.0)
    df_establishments["nb_pickups"] = df_establishments["nb_pickups"].fillna(0.0)
    df_establishments["nb_pickups_and_deliveries"] = df_establishments["nb_pickups_and_deliveries"].fillna(0.0)
    df_establishments = df_establishments.astype(column_types)
    
    column_names = {k: v[0] for k, v in GOODS_COLUMNS.items()}
    column_types = {v[0]: v[1]  for _, v in GOODS_COLUMNS.items()}

    df_goods.rename(columns=column_names, inplace=True)
    df_goods = df_goods[column_names.values()]
    df_goods["weight_kg"] = df_goods["weight_kg"].fillna(0)
    df_goods["volume_m3"] = df_goods["volume_m3"].fillna(0)
    df_goods = df_goods.astype(column_types)

    column_names = {k: v[0] for k, v in VEHICLES_COLUMNS.items()}
    column_types = {v[0]: v[1]  for _, v in VEHICLES_COLUMNS.items()}

    df_vehicles.rename(columns=column_names, inplace=True)
    df_vehicles = df_vehicles[column_names.values()]
    df_vehicles["has_vehicles"] = df_vehicles["has_vehicles"].map({"oui": True, "non": False})
    df_vehicles = df_vehicles.astype(column_types)

    return df_establishments, df_goods, df_vehicles
