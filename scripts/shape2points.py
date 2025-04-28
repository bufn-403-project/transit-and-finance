import geopandas as gpd
import pandas as pd
import argparse
from shapely.geometry import Point

def get_centers(geometry):
    if geometry.is_empty:
        return [None] * 5

    centroid = geometry.centroid
    minx, miny, maxx, maxy = geometry.bounds

    def find_border_point(centroid, direction):
        # Start with a reasonable maximum distance
        if direction in ('north', 'south'):
            max_distance = (maxy - miny)
        else:
            max_distance = (maxx - minx)

        low = 0
        high = max_distance
        tolerance = 1e-10  # how precise we want to be

        while high - low > tolerance:
            mid = (low + high) / 2
            if direction == 'north':
                test_point = Point(centroid.x, centroid.y + mid)
            elif direction == 'south':
                test_point = Point(centroid.x, centroid.y - mid)
            elif direction == 'east':
                test_point = Point(centroid.x + mid, centroid.y)
            elif direction == 'west':
                test_point = Point(centroid.x - mid, centroid.y)

            if geometry.contains(test_point):
                low = mid  # try further
            else:
                high = mid  # try closer

        # 3/4 of the final distance
        final_distance = (low * 3) / 4
        if direction == 'north':
            return Point(centroid.x, centroid.y + final_distance)
        elif direction == 'south':
            return Point(centroid.x, centroid.y - final_distance)
        elif direction == 'east':
            return Point(centroid.x + final_distance, centroid.y)
        elif direction == 'west':
            return Point(centroid.x - final_distance, centroid.y)

    # Calculate points
    north_point = find_border_point(centroid, 'north')
    east_point = find_border_point(centroid, 'east')
    south_point = find_border_point(centroid, 'south')
    west_point = find_border_point(centroid, 'west')

    return [
        (centroid.y, centroid.x),
        (north_point.y, north_point.x),
        (east_point.y, east_point.x),
        (south_point.y, south_point.x),
        (west_point.y, west_point.x)
    ]


def main():
    parser = argparse.ArgumentParser(description="Extract block group centers and directional points.")
    parser.add_argument("geojson", help="Input GeoJSON file (e.g. map.geojson)")
    parser.add_argument("-v", "--verbose", action="store_true", help="Enable verbose output")
    args = parser.parse_args()

    gdf = gpd.read_file(args.geojson)

    if gdf.crs != "EPSG:4326":
        gdf = gdf.to_crs("EPSG:4326")
        if args.verbose:
            print("Reprojected to EPSG:4326 (WGS84)")

    rows = []
    for index, row in gdf.iterrows():
        geoid = row["GEOID10"]
        geometry = row["geometry"]
        
        centers = get_centers(geometry)
        flat_coords = [coord for latlon in centers for coord in latlon]
        rows.append([geoid] + flat_coords)

        if args.verbose:
            print(f"Processed GEOID10: {geoid}, {flat_coords}")

    columns = [
        "GEOID10",
        "center_lat", "center_lon",
        "north_lat", "north_lon",
        "east_lat", "east_lon",
        "south_lat", "south_lon",
        "west_lat", "west_lon"
    ]

    df = pd.DataFrame(rows, columns=columns)
    df.to_csv("blockgroup_centers.csv", index=False)

    if args.verbose:
        print("Output saved to blockgroup_centers.csv")

if __name__ == "__main__":
    main()
