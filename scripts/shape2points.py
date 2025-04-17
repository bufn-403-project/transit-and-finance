import geopandas as gpd
import pandas as pd
import argparse

def get_centers(geometry):
    if geometry.is_empty:
        return [None] * 5

    centroid = geometry.centroid
    minx, miny, maxx, maxy = geometry.bounds

    north = ((minx + maxx) / 2, maxy)
    east  = (maxx, (miny + maxy) / 2)
    south = ((minx + maxx) / 2, miny)
    west  = (minx, (miny + maxy) / 2)

    return [
        (centroid.y, centroid.x),
        (north[1], north[0]),
        (east[1], east[0]),
        (south[1], south[0]),
        (west[1], west[0])
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
