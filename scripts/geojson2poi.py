import geopandas as gpd
import pandas as pd
import argparse
import json
from shapely.geometry import Point

def find_poi_in_shape(shape, geo_df):
    # Filter points inside the shape
    points_in_shape = geo_df[geo_df.geometry.within(shape)]

    # Calculate results
    total_pois = len(points_in_shape)
    average_stars = points_in_shape['stars'].mean() if total_pois > 0 else None
    business_ids = points_in_shape['business_id'].tolist()

    return total_pois, average_stars, business_ids

def calculate_poi_averages(num_poi, rating_poi):
    if (len(num_poi) != len(rating_poi)):
        raise Exception("num_poi and rating_poi must have same number of elements!")
    num_shapes = len(num_poi)
    sum_num_poi = 0
    for i in range(num_shapes):
        if num_poi[i] != None:
            sum_num_poi += num_poi[i]
    if num_shapes == 0 or sum_num_poi == 0:
        return 0,0

    average_num_poi = sum_num_poi / num_shapes
    
    average_rating_poi = 0
    for i in range(num_shapes):
        if num_poi[i] != None and rating_poi[i] != None:
            average_rating_poi += num_poi[i] * rating_poi[i]
    average_rating_poi /= sum_num_poi
    
    return average_num_poi, average_rating_poi

def main():
    parser = argparse.ArgumentParser(description="Calculate point of interests and output csv for census block groups from isochrone geojson")
    parser.add_argument("geojson", help="Input GeoJSON file (e.g. isochrones.geojson)")
    parser.add_argument("json", help="Input Yelp json file (e.g. yelp.json)")
    parser.add_argument("-v", "--verbose", action="store_true", help="Enable verbose output")
    args = parser.parse_args()

    if args.verbose:
            print("Loading geojson.")

    gdf = gpd.read_file(args.geojson)

    if gdf.crs != "EPSG:4326":
        gdf = gdf.to_crs("EPSG:4326")
        if args.verbose:
            print("Reprojected to EPSG:4326")

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
        print("Loading json")

    # Load the JSON file (newline-delimited JSON objects)
    yelp_df = pd.read_json(args.json, lines=True)

    # Drop missing values in 'categories' column
    yelp_df = yelp_df.dropna(subset=['categories'])
    yelp_df = yelp_df.dropna(subset=['latitude'])
    yelp_df = yelp_df.dropna(subset=['longitude'])
    yelp_df = yelp_df.dropna(subset=['stars'])
    yelp_df = yelp_df.dropna(subset=['business_id'])

    # Convert DataFrame to GeoDataFrame
    yelp_geometry = [Point(xy) for xy in zip(yelp_df['longitude'], yelp_df['latitude'])]
    yelp_geo_df = gpd.GeoDataFrame(yelp_df, geometry=yelp_geometry, crs="EPSG:4326")

    if args.verbose:
        print("Done loading json")

    if args.verbose:
        print("Calcuating point of interests in geometries, sorting by geoid")

    # Sort the gdf rows into appropriate geoid lists, calculate POIs in shapes
    shapes_by_geoid = {}
    for geoid in geoids:
        shapes_by_geoid[geoid] = []

    for index, row in gdf.iterrows():
        num_poi, rating_poi, poi_list = find_poi_in_shape(row["geometry"], yelp_geo_df)
        shape_dict = {
          "profile": row["profile"],
          "time_limit": row["time_limit"],
          "point_label": row["point_label"],
          "num_poi": num_poi,
          "rating_poi": rating_poi,
          "poi_list": poi_list
        }
        shapes_by_geoid[row["geoid"]].append(shape_dict)
        if args.verbose:
            print(row["geoid"], shape_dict["profile"], shape_dict["time_limit"], shape_dict["num_poi"], shape_dict["rating_poi"])

    if args.verbose:
        print("Done calcuating point of interests in geometries.")

    if args.verbose:
        print("Calculating average POI data of isochrone types by geoid")

    # Calculate average POI by geoid, profile, time_limit; add to rows
    rows = []
    for geoid in geoids:
        for profile in profiles:
            for time_limit in time_limits:
                num_poi = []
                rating_poi = []
                poi_list_center = []
                for shape in shapes_by_geoid[geoid]:
                    if (shape["profile"] == profile and shape["time_limit"] == time_limit):
                        num_poi.append(shape["num_poi"])
                        rating_poi.append(shape["rating_poi"])
                        if (shape["point_label"] == "center"):
                            poi_list_center = shape["poi_list"]
                average_num_poi, average_rating_poi = calculate_poi_averages(num_poi, rating_poi) 
                rows.append([geoid, profile, time_limit, average_num_poi, average_rating_poi, poi_list_center])
                if args.verbose:
                    print([geoid, profile, time_limit, average_num_poi, average_rating_poi])

    if args.verbose:
        print("Done calculating average POI data and sorting geometries. Writing to disk.")

    columns = [
        "GEOID10",
        "profile",
        "time_limit",
        "average_num_poi",
        "average_rating_poi",
        "poi_list_center"
    ]

    # Save dataframe
    df = pd.DataFrame(rows, columns=columns)
    df.to_csv("blockgroup_poi.csv", index=False)

    if args.verbose:
        print("Output saved to blockgroup_poi.csv")

if __name__ == "__main__":
    main()
