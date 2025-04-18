import pandas as pd
import requests
import json
from concurrent.futures import ThreadPoolExecutor
from shapely.geometry import shape
from shapely.ops import transform
from pyproj import Transformer
import os
import multiprocessing
import threading
import time

# Global lock for error file access
error_file_lock = threading.Lock()

# ---- CONFIG ----
GRAPH_HOPPER_URL = "http://localhost:8989/isochrone"
TIME_LIMITS = [300, 900, 1800]
PROFILES = ["foot", "car", "pt"]
POINT_LABELS = ["center", "north", "east", "south", "west"]
NUM_WORKERS = multiprocessing.cpu_count()
CSV_FILE = "blockgroup_centers.csv"  # Update this if needed
OUTPUT_FILE = "isochrones.geojson"   # Update this if needed
ERROR_LOG_PATH = "error_file.log"    # Update this if needed

# ---- HELPERS ----
def build_requests_from_csv(file_path):
    df = pd.read_csv(file_path)
    requests_list = []

    for _, row in df.iterrows():
        geoid = row["GEOID10"]
        for label in POINT_LABELS:
            lat = row[f"{label}_lat"]
            lon = row[f"{label}_lon"]
            for profile in PROFILES:
                for time_limit in TIME_LIMITS:
                    requests_list.append({
                        "geoid": geoid,
                        "point_label": label,
                        "profile": profile,
                        "time_limit": time_limit,
                        "coordinates": f"{lat},{lon}"
                    })
    return requests_list

def log_error(message, error_log_path=ERROR_LOG_PATH):
    print(message)  # stdout
    with open(error_log_path, "a") as error_file:
        print(message, file=error_file)

def fetch_isochrone(params):
    retries = 5

    for attempt in range(1, retries + 1):
        try:
            lat, lon = params["coordinates"].split(",")
            url = (
                f"{GRAPH_HOPPER_URL}"
                f"?profile={params['profile']}"
                f"&point={lat},{lon}"
                f"&time_limit={params['time_limit']}"
                f"&pt.earliest_departure_time=2025-03-31T14:00:00Z"
            )

            #print(f"[→] Requesting: GEOID={params['geoid']} | Point={params['point_label']} | Profile={params['profile']} | Time={params['time_limit']}s")

            response = requests.get(url)
            response.raise_for_status()


            #print(f"[✓] Success: GEOID={params['geoid']} | Point={params['point_label']} | Profile={params['profile']} | Time={params['time_limit']}s")

            return {
                "geoid": params["geoid"],
                "point_label": params["point_label"],
                "profile": params["profile"],
                "time_limit": params["time_limit"],
                "coordinates": params["coordinates"],
                "isochrone": response.json()
            }

        except Exception as e:
            with error_file_lock:
                log_error(f"[!] Error on attempt {attempt} for {params}: {e}")
                if attempt < retries:
                    log_error(f"[!] Retrying in 2 seconds... (Attempt {attempt + 1} of {retries})")
                else:
                    log_error(f"[!] Max retries reached. Giving up.")
            if attempt < retries:
                time.sleep(2)
            else:
                return None

def calculate_area(geometry):
    transformer = Transformer.from_crs("EPSG:4326", "EPSG:3857", always_xy=True)
    projected_geom = transform(transformer.transform, shape(geometry))
    return projected_geom.area

# ---- MAIN ----
def main():
    request_data = build_requests_from_csv(CSV_FILE)
    total_requests = len(request_data)
    print(f"[+] Generated {total_requests} request tasks...")

    processed_count = 0  # Counter for processed features
    lock = threading.Lock()  # Lock for synchronizing writes to the file

    with ThreadPoolExecutor(max_workers=NUM_WORKERS) as executor:
        # Open the output file once before starting to write
        with open(OUTPUT_FILE, "w") as f:
            # Write the initial structure of the geojson (beginning of the FeatureCollection)
            f.write('{"type": "FeatureCollection", "features": [')

            first_feature = True  # To handle commas between features

            for result in executor.map(fetch_isochrone, request_data):
                if result is None:
                    continue
                for poly in result["isochrone"].get("polygons", []):
                    geometry = poly["geometry"]
                    area = calculate_area(geometry)

                    #print(f"[+] Adding polygon: GEOID={result['geoid']} | Point={result['point_label']} | Profile={result['profile']} | Time={result['time_limit']}s | Area={area:.2f} m²")

                    feature = {
                        "type": "Feature",
                        "geometry": geometry,
                        "properties": {
                            "geoid": result["geoid"],
                            "point_label": result["point_label"],
                            "profile": result["profile"],
                            "time_limit": result["time_limit"],
                            "center": result["coordinates"],
                            "area_m2": area
                        }
                    }

                    # Use lock to ensure that only one thread writes to the file at a time
                    with lock:
                        # Write the feature, ensuring proper formatting (comma between features)
                        if not first_feature:
                            f.write(",\n")  # Add a comma before new features except the first one
                        f.write(json.dumps(feature, indent=2))

                    first_feature = False  # After the first feature, set the flag to False

                    # Update the processed count and check for progress
                    processed_count += 1
                    progress_percent = (processed_count / total_requests) * 100
                    print(f"[{int(progress_percent)}%] {processed_count}/{total_requests} features processed")

            # Close the geojson structure (closing the feature array and the whole object)
            with lock:
                f.write("\n]}")
            print(f"[✓] Incrementally saved features to {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
