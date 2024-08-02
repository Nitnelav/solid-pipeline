import pandas as pd
import pandera as pa
import numpy as np
import numba
import itertools


def configure(context):
    context.stage("data.sirene.assign_hub")
    context.stage("data.ugms.cleaned")

    context.config("random_seed", 1234)
    context.config("processes", 16)

@numba.jit(nopython = True) # Already parallelized parallel = True)
def sample_indices(uniform, cdf, selected_indices):
    indices = np.arange(len(uniform))

    for i, u in enumerate(uniform):
        indices[i] = np.count_nonzero(cdf < u)

    return selected_indices[indices]

def statistical_matching(progress, df_source, source_identifier, weight, df_target, target_identifier, columns, random_seed = 0, minimum_observations = 0):
    random = np.random.RandomState(random_seed)

    # Reduce data frames
    df_source = df_source[[source_identifier, weight] + columns].copy()
    df_target = df_target[[target_identifier] + columns].copy()

    # Sort data frames
    df_source = df_source.sort_values(by = columns)
    df_target = df_target.sort_values(by = columns)

    # Find unique values for all columns
    unique_values = {}

    for column in columns:
        unique_values[column] = list(sorted(set(df_source[column].unique()) | set(df_target[column].unique())))

    # Generate filters for all columns and values
    source_filters, target_filters = {}, {}

    for column, column_unique_values in unique_values.items():
        source_filters[column] = [df_source[column].values == value for value in column_unique_values]
        target_filters[column] = [df_target[column].values == value for value in column_unique_values]

    # Define search order
    source_filters = [source_filters[column] for column in columns]
    target_filters = [target_filters[column] for column in columns]

    # Perform matching
    weights = df_source[weight].values
    assigned_indices = np.ones((len(df_target),), dtype = int) * -1
    unassigned_mask = np.ones((len(df_target),), dtype = bool)
    assigned_levels = np.ones((len(df_target),), dtype = int) * -1
    uniform = random.random_sample(size = (len(df_target),))

    column_indices = [np.arange(len(unique_values[column])) for column in columns]

    for level in range(1, len(column_indices) + 1)[::-1]:
        level_column_indices = column_indices[:level]

        if np.count_nonzero(unassigned_mask) > 0:
            for column_index in itertools.product(*level_column_indices):
                f_source = np.logical_and.reduce([source_filters[i][k] for i, k in enumerate(column_index)])
                f_target = np.logical_and.reduce([target_filters[i][k] for i, k in enumerate(column_index)] + [unassigned_mask])

                selected_indices = np.nonzero(f_source)[0]
                requested_samples = np.count_nonzero(f_target)

                if requested_samples == 0:
                    continue

                if len(selected_indices) < minimum_observations:
                    continue

                selected_weights = weights[f_source]
                cdf = np.cumsum(selected_weights)
                cdf /= cdf[-1]

                assigned_indices[f_target] = sample_indices(uniform[f_target], cdf, selected_indices)
                assigned_levels[f_target] = level
                unassigned_mask[f_target] = False

                progress.update(np.count_nonzero(f_target))

    # Randomly assign unmatched observations
    cdf = np.cumsum(weights)
    cdf /= cdf[-1]

    assigned_indices[unassigned_mask] = sample_indices(uniform[unassigned_mask], cdf, np.arange(len(weights)))
    assigned_levels[unassigned_mask] = 0

    progress.update(np.count_nonzero(unassigned_mask))

    if np.count_nonzero(unassigned_mask) > 0:
        raise RuntimeError("Some target observations could not be matched. Minimum observations configured too high?")

    assert np.count_nonzero(unassigned_mask) == 0
    assert np.count_nonzero(assigned_indices == -1) == 0

    # Write back indices
    df_target[source_identifier] = df_source[source_identifier].values[assigned_indices]
    df_target = df_target[[target_identifier, source_identifier]]

    return df_target, assigned_levels

def _run_parallel_statistical_matching(context, args):
    # Pass arguments
    df_target, random_seed = args

    # Pass data
    df_source = context.data("df_source")
    source_identifier = context.data("source_identifier")
    weight = context.data("weight")
    target_identifier = context.data("target_identifier")
    columns = context.data("columns")
    minimum_observations = context.data("minimum_observations")

    return statistical_matching(context.progress, df_source, source_identifier, weight, df_target, target_identifier, columns, random_seed, minimum_observations)

def parallel_statistical_matching(context, df_source, source_identifier, weight, df_target, target_identifier, columns, minimum_observations = 0):
    random_seed = context.config("random_seed")
    processes = context.config("processes")

    random = np.random.RandomState(random_seed)
    chunks = np.array_split(df_target, processes)

    with context.progress(label = "Statistical matching ...", total = len(df_target)):
        with context.parallel({
            "df_source": df_source, "source_identifier": source_identifier, "weight": weight,
            "target_identifier": target_identifier, "columns": columns,
            "minimum_observations": minimum_observations
        }) as parallel:
                random_seeds = random.randint(10000, size = len(chunks))
                results = parallel.map(_run_parallel_statistical_matching, zip(chunks, random_seeds))

                levels = np.hstack([r[1] for r in results])
                df_target = pd.concat([r[0] for r in results])

                return df_target, levels

def _create_goods(context, matching):
    siret = matching["siret"]
    establishment_id = matching["establishment_id"]

    df_goods = context.data("df_goods")
    # gdf_sirene = context.data("gdf_sirene")

    df_goods = df_goods[df_goods["establishment_id"] == establishment_id].copy()
    df_goods["siret"] = siret

    df_goods.rename(columns = {"establishment_id": "ugms_id"}, inplace = True)

    return df_goods.to_dict(orient="records")

def execute(context):
    gdf_sirene = context.stage("data.sirene.assign_hub")
    df_establishments, df_goods, df_vehicles = context.stage("data.ugms.cleaned")

    gdf_sirene = pa.DataFrameSchema(
        {
            "siren": pa.Column("int32"),
            "siret": pa.Column(int),
            "municipality_id": pa.Column(str),
            "suburb_type": pa.Column("category"),
            "employees": pa.Column(int),
            "ape": pa.Column(str),
            "law_status": pa.Column(str),
            "st8": pa.Column(int),
            "st20": pa.Column(int),
            "st45": pa.Column(str),
            "hub_id": pa.Column(int),
            "hub_distance": pa.Column(float),
            "geometry": pa.Column("geometry"),
        }
    ).validate(gdf_sirene)

    df_establishments = pa.DataFrameSchema(
        {
            "establishment_id": pa.Column(str),
            "st8": pa.Column(np.int32),
            "st20": pa.Column(np.int32),
            "st45": pa.Column("category"),
            "ape": pa.Column("category"),
            "employees": pa.Column(np.int32),
            "nb_movements": pa.Column(float),
            "nb_deliveries": pa.Column(float),
            "nb_pickups": pa.Column(float),
            "nb_pickups_and_deliveries": pa.Column(float),
            "suburb_type": pa.Column("category"),
            "establishment_weight": pa.Column(float),
        }
    ).validate(df_establishments)

    df_goods = pa.DataFrameSchema(
        {
            "establishment_id": pa.Column(str),
            "move_type": pa.Column("category"),
            "good_type": pa.Column("category"),
            "move_mode": pa.Column("category"),
            "weight_kg": pa.Column(float),
            "operation_weight": pa.Column(float),
        }
    ).validate(df_goods)
    
    df_vehicles = pa.DataFrameSchema(
        {
            "establishment_id": pa.Column(str),
            "has_vehicles": pa.Column(bool),
            "nb_bicycles": pa.Column(np.int32),
            "nb_motorcycles": pa.Column(np.int32),
            "nb_cars": pa.Column(np.int32),
            "nb_vans_small": pa.Column(np.int32),
            "nb_vans_big": pa.Column(np.int32),
            "nb_trucks_7t5": pa.Column(np.int32),
            "nb_trucks_12t": pa.Column(np.int32),
            "nb_trucks_19t": pa.Column(np.int32),
            "nb_trucks_32t": pa.Column(np.int32),
            "nb_articuated_28t": pa.Column(np.int32),
            "nb_articuated_40t": pa.Column(np.int32),
        }
    ).validate(df_vehicles)

    # Create a column with employee classes
    employee_bins = [0, 3, 6, 10, 20, 50, 100, np.inf]
    employee_labels = ["0-2", "3-5", "6-10", "11-20", "21-50", "51-100", "101+"]
    
    df_establishments["employee_class"] = pd.cut(df_establishments["employees"], bins=employee_bins, labels=employee_labels)
    gdf_sirene["employee_class"] = pd.cut(gdf_sirene["employees"], bins=employee_bins, labels=employee_labels)

    columns = ["st8", "employee_class", "suburb_type"]
    minimum_observations = 10

    df_matching, levels = parallel_statistical_matching(
        context,
        df_establishments, "establishment_id", "establishment_weight",
        gdf_sirene, "siret", 
        columns,
        minimum_observations
    )
    df_matching["matching_levels"] =  pd.DataFrame(levels).values

    sirene_goods = []
    with context.progress(label="Assigning Goods ...", total=len(gdf_sirene)):
        with context.parallel(
            {
                "df_goods": df_goods,
                "gdf_sirene": gdf_sirene,
            }
        ) as parallel:
            for goods in parallel.imap(_create_goods, df_matching.to_dict(orient="records")):
                sirene_goods += goods

    df_sirene_goods = pd.DataFrame(sirene_goods)

    return df_sirene_goods