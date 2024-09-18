import pandera as pa


def configure(context):
    context.config("output_path")
    context.stage("data.ugms.cleaned")


def execute(context):
    output_path = context.config("output_path")
    df_establishments, df_goods, df_vehicles, df_relations = context.stage("data.ugms.cleaned")

    # Validate the data
    df_establishments = pa.DataFrameSchema(
        {
            "establishment_id": pa.Column(str),
            "st8": pa.Column(pa.Int32),
            "st20": pa.Column(pa.Int32),
            "st45": pa.Column(str),
            "ape": pa.Column(str),
            "employees": pa.Column(pa.Int32),
            "nb_movements": pa.Column(float),
            "nb_deliveries": pa.Column(float),
            "nb_pickups": pa.Column(float),
            "nb_pickups_and_deliveries": pa.Column(float),
            "suburb_type": pa.Column(str),
            "establishment_weight": pa.Column(float),
        }
    ).validate(df_establishments)

    df_goods = pa.DataFrameSchema(
        {
            "establishment_id": pa.Column(str),
            "move_type": pa.Column(str),
            "good_type": pa.Column(str),
            "move_mode": pa.Column(str),
            "weight_kg": pa.Column(float),
            "operation_weight": pa.Column(float),
        }
    ).validate(df_goods)

    df_vehicles = pa.DataFrameSchema(
        {
            "establishment_id": pa.Column(str),
            "has_vehicles": pa.Column(bool),
            "nb_bicycles": pa.Column(pa.Int32),
            "nb_motorcycles": pa.Column(pa.Int32),
            "nb_cars": pa.Column(pa.Int32),
            "nb_vans_small": pa.Column(pa.Int32),
            "nb_vans_big": pa.Column(pa.Int32),
            "nb_trucks_7t5": pa.Column(pa.Int32),
            "nb_trucks_12t": pa.Column(pa.Int32),
            "nb_trucks_19t": pa.Column(pa.Int32),
            "nb_trucks_32t": pa.Column(pa.Int32),
            "nb_articuated_28t": pa.Column(pa.Int32),
            "nb_articuated_40t": pa.Column(pa.Int32),
        }
    ).validate(df_vehicles)

    df_relations = pa.DataFrameSchema(
        {
            "establishment_id": pa.Column(str),
            "direction": pa.Column("category"),
            "type": pa.Column("category"),
            "nb_contract": pa.Column(pa.Int32),
            "part_pct": pa.Column(float),
        }
    ).validate(df_relations)

    df_goods.to_csv("%s/ugms_goods.csv" % output_path, index=False)
    df_establishments.to_csv("%s/ugms_establishments.csv" % output_path, index=False)
    df_vehicles.to_csv("%s/ugms_vehicles.csv" % output_path, index=False)
    df_relations.to_csv("%s/ugms_relations.csv" % output_path, index=False)
