import pandera as pa

def configure(context):
    context.stage("ssm.ssm")
    context.stage("synthesis.sirene.sampled")
    context.stage("vehicles.all")

def execute(context):
    gdf_supplied_demand = context.stage("ssm.ssm")
    gdf_sirene = context.stage("synthesis.sirene.sampled")
    df_vehicles = context.stage("vehicles.all")

    gdf_sirene = pa.DataFrameSchema(
        {
            "siren": pa.Column("int32"),
            "siret": pa.Column(int),
            "municipality_id": pa.Column(str),
            "employees": pa.Column(int),
            "ape": pa.Column(str),
            "law_status": pa.Column(str),
            "st8": pa.Column(pa.Int64),
            "st20": pa.Column(pa.Int64),
            "st45": pa.Column(str),
            "geometry": pa.Column("geometry"),
        }
    ).validate(gdf_sirene)

    gdf_supplied_demand = pa.DataFrameSchema(
        {
            "siret": pa.Column(pa.Int64),
            "supplier_siret": pa.Column(pa.Int64),
        }
    ).validate(gdf_supplied_demand)

    df_vehicles = pa.DataFrameSchema(
        {
            "siret": pa.Column("int64"),
            "type": pa.Column("str"),
            "start": pa.Column("object"),
        }
    ).validate(df_vehicles)

    supplier_sirets = gdf_supplied_demand["supplier_siret"].unique()

    problems = []
    for supplier_siret in supplier_sirets:
        supplier = gdf_sirene.loc[gdf_sirene["siret"] == supplier_siret]
        vehicles = df_vehicles.loc[df_vehicles["siret"] == supplier_siret]
        clients = gdf_supplied_demand.loc[gdf_supplied_demand["supplier_siret"] == supplier_siret]
        problems.append({
            "supplier": supplier,
            "vehicles": vehicles,
            "clients": clients
        })

    return problems
        
        