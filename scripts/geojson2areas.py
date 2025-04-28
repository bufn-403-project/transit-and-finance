import geopandas as gpd
import pandas as pd
import argparse

def main():
    parser = argparse.ArgumentParser(description="Calculate area and output csv for census block groups from isochrone geojson")
    parser.add_argument("geojson", help="Input GeoJSON file (e.g. isochrones.geojson)")
    parser.add_argument("-v", "--verbose", action="store_true", help="Enable verbose output")
    args = parser.parse_args()

    if args.verbose:
            print("Loading geojson.")

    gdf = gpd.read_file(args.geojson)

    if gdf.crs != "EPSG:4326":
        gdf = gdf.to_crs("EPSG:4326")
        if args.verbose:
            print("Reprojected to EPSG:4326")

    gdf = gdf.to_crs(epsg=3857) # de facto standard used in GPS and on web

    if args.verbose:
            print("Done loading geojson.")

    # Unique sets of geoids, point labels, profiles, and time limits
    geoids = set()
    point_labels = set()
    profiles = set()
    time_limits = set()

    # Calculate all geoid, point_label, profile, and time_limit types.
    if args.verbose:
            print("Finding all geoid, point_label, profile, and time_limit types.")
    for index, row in gdf.iterrows():
        geoids.add(row["geoid"])
        point_labels.add(row["point_label"])
        profiles.add(row["profile"])
        time_limits.add(row["time_limit"])

    if args.verbose:
        print(f'Geoids: {geoids}, point labels: {point_labels}, profiles: {profiles}, time limits: {time_limits}')

    if args.verbose:
        print("Calcuating area of geometries, sorting by geoid")

    # Sort the gdf rows into appropriate geoid lists, calculate area
    shapes_by_geoid = {}
    for geoid in geoids:
        shapes_by_geoid[geoid] = []

    for index, row in gdf.iterrows():
        shape_dict = {
          "profile": row["profile"],
          "time_limit": row["time_limit"],
          "point_label": row["point_label"],
          "area": row["geometry"].area
        }
        shapes_by_geoid[row["geoid"]].append(shape_dict)

    if args.verbose:
        print("Done calcuating area of geometries.")

    if args.verbose:
        print("Calculating average areas of isochrone types by geoid")

    # Calculate average area by geoid, profile, time_limit; add to rows
    rows = []
    for geoid in geoids:
        for profile in profiles:
            for time_limit in time_limits:
                areas = []
                for shape in shapes_by_geoid[geoid]:
                    if (shape["profile"] == profile and shape["time_limit"] == time_limit):
                        areas.append(shape["area"])
                average_area = sum(areas)
                if (average_area != 0):
                    rows.append([geoid, profile, time_limit, average_area])

    if args.verbose:
        print("Done calculating average areas and sorting geometries. Writing to disk.")

    columns = [
        "GEOID10",
        "profile",
        "time_limit",
        "average_area",
    ]

    # Save dataframe
    df = pd.DataFrame(rows, columns=columns)
    df.to_csv("blockgroup_areas.csv", index=False)

    if args.verbose:
        print("Output saved to blockgroup_areas.csv")

if __name__ == "__main__":
    main()
