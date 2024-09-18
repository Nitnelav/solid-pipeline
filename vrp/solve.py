import geopandas as gpd
import pandas as pd
import numpy as np

import vroom

def configure(context):
    context.config("osrm_server", "sma.univ-eiffel.fr:15000")
    context.stage("vrp.problems")


def _solve_problem(context, problem):

    vroom_instance = vroom.Input(
        amount_size = 1,
        servers={"auto": context.data("osrm_server")},
        router=vroom._vroom.ROUTER.OSRM
    )

    problem_vehicles = []
    problem_jobs = []

    gdf_supplier: gpd.GeoDataFrame = problem["supplier"]
    gdf_clients: gpd.GeoDataFrame = problem["clients"]
    df_vehicles: pd.DataFrame = problem["vehicles"]

    gdf_supplier = gdf_supplier.to_crs(epsg=4326)
    supplier = gdf_supplier.iloc[0]
    gdf_clients = gdf_clients.to_crs(epsg=4326)

    supplier_position = (supplier.geometry.x, supplier.geometry.y)
    for vehicle in df_vehicles.itertuples():
        problem_vehicles.append(vroom.Vehicle(
            id=int(vehicle.Index),
            profile="auto",
            start=supplier_position,
            capacity=[1],
        ))

    for client in gdf_clients.itertuples():
        problem_jobs.append(vroom.Job(
            id=int(client.Index),
            location=(client.geometry.x, client.geometry.y),
            delivery=[1],
        ))
    
    vroom_instance.add_vehicle(problem_vehicles)
    vroom_instance.add_job(problem_jobs)

    try:
        solution = vroom_instance.solve(exploration_level=5, nb_threads=8)
    except Exception as e:
        print("WARINIG: problem failed with error : " + e)
        solution = None

    context.progress.update()
    return solution

def execute(context):
    problems = context.stage("vrp.problems")

    df_routes = None

    # FIXME: This is a temporary fix to avoid empty vehicles
    problems = [problem for problem in problems if len(problem["vehicles"]) > 0]

    with context.progress(label="Solving TSP with Vroom ...", total=len(problems)) as p:
        with context.parallel(
            {
                "osrm_server": context.config("osrm_server"),
            }
        ) as parallel:
            for solution in parallel.map(_solve_problem, problems):
                if solution is None:
                    continue
                if df_routes is None:
                    df_routes = solution.routes
                else:
                    df_routes = pd.concat([df_routes, solution.routes])

        # class ctx:
        #     def data(name):
        #         if name == "osrm_server":
        #             return context.config("osrm_server")
        #     class progress:
        #         def update():
        #             p.update()

        # for problem in problems:
        #     solution = _solve_problem(ctx, problem)
        #     if solution is None:
        #         print("WARNING: one problem failed")
        #         continue
        #     if df_routes is None:
        #         df_routes = solution.routes
        #     else:
        #         df_routes = pd.concat([df_routes, solution.routes])

    return df_routes



