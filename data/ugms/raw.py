import pandas as pd

ESTABLISHEMENTS_COLUMNS = {
    "Idetab": ("establishment_id", str),
    "ST8-OK": ("st8", int),
    "ST45-OK": ("st45", str),
    "Apet_ok": ("ape", str),
    "Effec_total": ("employees", int),
    "MVTS-OK": ("nb_movements", float),
    "REC-OK": ("nb_deliveries", float),
    "EXP-OK": ("nb_pickups", float),
    "CONJ-OK": ("nb_pickups_and_deliveries", float),
    "Couronne": ("suburb_type", "category"),
    "COEF-REDRETAB": ("establishment_weight", float),
}

OPERATIONS_COLUMNS = {
    "Idétab": ("establishment_id", str),
    "Idopé": ("operation_id", int),
    "Type_opération": ("move_type", str),
    "Fréquence_hebdo": ("frequency", float),
    "NbMVT": ("nb_movements", float),
    "MG-3pos": ("move_mode", str),
    "Nb_total_marchandises": ("nb_units", float),
    "Poids_total_kg": ("weight_kg", float),
    "Volume_total_m3": ("volume_m3", float),
    "REDR-OPE-RETENU": ("operation_weight", float),
}

GOODS_COLUMNS = {
    "Idétab": ("establishment_id", str),
    "Idopé": ("operation_id", int),
    "Type_operation": ("move_type", str),
    "Nature_marchandise": ("good_type", str),
    "Nature_marchandise_autre": ("good_type_other", str),
    "Conditionnement": ("packaging", str),
    "Nb_unités": ("nb_units", float),
    "Pds_kg": ("weight_kg", float),
    "Vol_m3": ("volume_m3", float),
}

VEHICLES_COLUMNS = {
    "Idetab": ("establishment_id", str),
    "Véhicules": ("has_vehicles", bool),
    "Vélos_triport_total": ("nb_bicycles", int),
    "Motos_total": ("nb_motorcycles", int),
    "Voiture_total": ("nb_cars", int),
    "Fourgo_total": ("nb_vans_small", int),
    "Camio_total": ("nb_vans_big", int),
    "Port_7t5_total": ("nb_trucks_7t5", int),
    "Port_12t_total": ("nb_trucks_12t", int),
    "Port_19t_total": ("nb_trucks_19t", int),
    "Port_32t_total": ("nb_trucks_32t", int),
    "Art_28t_total": ("nb_articuated_28t", int),
    "Art_40t_total": ("nb_articuated_40t", int),
}


def configure(context):
    context.config("ugms_file_path")


def execute(context):
    file_path = context.config("ugms_file_path")
    sheets = ["1188 ETAB-MVT-ULTIME-OK", "Etab-OPE OK", "etab_marchandises", "Etab_Véhicules"]
    ugms_xls: dict[str, pd.DataFrame] = pd.read_excel(file_path, sheet_name=sheets)
    df_establishments, df_operations, df_goods, df_vehicles = ugms_xls.values()

    column_names = {k: v[0] for k, v in ESTABLISHEMENTS_COLUMNS.items()}
    column_types = {v[0]: v[1]  for _, v in ESTABLISHEMENTS_COLUMNS.items()}

    df_establishments.rename(columns=column_names, inplace=True)
    df_establishments = df_establishments[column_names.values()]
    df_establishments["nb_deliveries"] = df_establishments["nb_deliveries"].fillna(0.0)
    df_establishments["nb_pickups"] = df_establishments["nb_pickups"].fillna(0.0)
    df_establishments["nb_pickups_and_deliveries"] = df_establishments["nb_pickups_and_deliveries"].fillna(0.0)
    df_establishments = df_establishments.astype(column_types)
    
    column_names = {k: v[0] for k, v in OPERATIONS_COLUMNS.items()}
    column_types = {v[0]: v[1]  for _, v in OPERATIONS_COLUMNS.items()}

    df_operations.rename(columns=column_names, inplace=True)
    df_operations = df_operations[column_names.values()]
    df_operations["frequency"] = df_operations["frequency"].fillna(0)
    df_operations = df_operations[~df_operations["operation_id"].isna()]
    df_operations = df_operations.astype(column_types)

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

    return df_establishments, df_operations, df_goods, df_vehicles
