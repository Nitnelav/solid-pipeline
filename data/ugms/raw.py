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
    "Shon_ok": ("surface", float),
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

RELATIONS_COLUMNS = {
    "Idetab": ("establishment_id", str),

    "Appro_centraha_nb": ("receive_center_nb", int),
    "Appro_centraha_partop": ("receive_center_part", float),
    "Appro_centraha_partha": ("receive_center_part_cost", float),
    "Appro_gross_nb": ("receive_wholesaler_nb", int),
    "Appro_gross_partop": ("receive_wholesaler_part", float),
    "Appro_gross_partha": ("receive_wholesaler_part_cost", float),
    "Appro_fourn_nb": ("receive_supplier_nb", int),
    "Appro_fourn_partop": ("receive_supplier_part", float),
    "Appro_fourn_partha": ("receive_supplier_part_cost", float),
    "Appro_clientind_nb": ("receive_client_nb", int),
    "Appro_clientind_partop": ("receive_client_part", float),
    "Appro_clientind_partha": ("receive_client_part_cost", float),
    "Appro_autre_nb": ("receive_other_nb", int),
    "Appro_autre_partop": ("receive_other_part", float),
    "Appro_autre_partha": ("receive_other_part_cost", float),

    "Expéd_centraha_nb": ("send_center_nb", int),
    "Expéd_centraha_partop": ("send_center_part", float),
    "Expéd_centraha_partha": ("send_center_part_cost", float),
    "Expéd_gross_nb": ("send_wholesaler_nb", int),
    "Expéd_gross_partop": ("send_wholesaler_part", float),
    "Expéd_gross_partha": ("send_wholesaler_part_cost", float),
    "Expéd_fourn_nb": ("send_supplier_nb", int),
    "Expéd_fourn_partop": ("send_supplier_part", float),
    "Expéd_fourn_partha": ("send_supplier_part_cost", float),
    "Expéd_clientind_nb": ("send_client_nb", int),
    "Expéd_clientind_partop": ("send_client_part", float),
    "Expéd_clientind_partha": ("send_client_part_cost", float),
    "Expéd_autre_nb": ("send_other_nb", int),
    "Expéd_autre_partop": ("send_other_part", float),
    "Expéd_autre_partha": ("send_other_part_cost", float),
}


def configure(context):
    context.config("ugms_file_path")


def execute(context):

    # read xls file into pandas dataframes
    file_path = context.config("ugms_file_path")
    sheets = ["1188 ETAB-MVT-ULTIME-OK", "Etab-OPE OK", "etab_marchandises", "Etab_Véhicules", "Etab_Relations"]

    ugms_xls: dict[str, pd.DataFrame] = pd.read_excel(file_path, sheet_name=sheets)
    df_establishments, df_operations, df_goods, df_vehicles, df_relations = ugms_xls.values()

    # 1 - process establishments sheet
    column_names = {k: v[0] for k, v in ESTABLISHEMENTS_COLUMNS.items()}
    column_types = {v[0]: v[1]  for _, v in ESTABLISHEMENTS_COLUMNS.items()}
    # select columns
    df_establishments.rename(columns=column_names, inplace=True)
    df_establishments = df_establishments[column_names.values()]
    # process columns
    df_establishments["nb_deliveries"] = df_establishments["nb_deliveries"].fillna(0.0)
    df_establishments["nb_pickups"] = df_establishments["nb_pickups"].fillna(0.0)
    df_establishments["nb_pickups_and_deliveries"] = df_establishments["nb_pickups_and_deliveries"].fillna(0.0)
    df_establishments.loc[df_establishments["surface"] == "NSP", "surface"] = 0
    # set column types
    df_establishments = df_establishments.astype(column_types)
    
    # 2 - process operations sheet
    column_names = {k: v[0] for k, v in OPERATIONS_COLUMNS.items()}
    column_types = {v[0]: v[1]  for _, v in OPERATIONS_COLUMNS.items()}
    # select columns
    df_operations.rename(columns=column_names, inplace=True)
    df_operations = df_operations[column_names.values()]
    # process columns
    df_operations["frequency"] = df_operations["frequency"].fillna(0)
    df_operations = df_operations[~df_operations["operation_id"].isna()] # just one row removed 
    # set column types
    df_operations = df_operations.astype(column_types)

    # 3 - process goods sheet
    column_names = {k: v[0] for k, v in GOODS_COLUMNS.items()}
    column_types = {v[0]: v[1]  for _, v in GOODS_COLUMNS.items()}
    # select columns
    df_goods.rename(columns=column_names, inplace=True)
    df_goods = df_goods[column_names.values()]
    # process columns
    df_goods["weight_kg"] = df_goods["weight_kg"].fillna(0)
    df_goods["volume_m3"] = df_goods["volume_m3"].fillna(0)
    # set column types
    df_goods = df_goods.astype(column_types)

    # 4 - process vehicles sheet
    column_names = {k: v[0] for k, v in VEHICLES_COLUMNS.items()}
    column_types = {v[0]: v[1]  for _, v in VEHICLES_COLUMNS.items()}
    # select columns
    df_vehicles.rename(columns=column_names, inplace=True)
    # process columns
    df_vehicles = df_vehicles[column_names.values()]
    df_vehicles["has_vehicles"] = df_vehicles["has_vehicles"].map({"oui": True, "non": False})
    # set column types
    df_vehicles = df_vehicles.astype(column_types)

    # 5 - process relations sheet
    column_names = {k: v[0] for k, v in RELATIONS_COLUMNS.items()}
    column_types = {v[0]: v[1]  for _, v in RELATIONS_COLUMNS.items()}
    # select columns
    df_relations.rename(columns=column_names, inplace=True)
    df_relations = df_relations[column_names.values()]
    # process columns
    df_relations = df_relations.fillna(0)
    # set column types
    df_relations = df_relations.astype(column_types)

    return df_establishments, df_operations, df_goods, df_vehicles, df_relations
