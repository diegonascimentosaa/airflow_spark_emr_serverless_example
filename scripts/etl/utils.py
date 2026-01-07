import sys
import requests


def get_api_data(endpoint):
    base_url = "https://jsonplaceholder.typicode.com"
    try:
        response = requests.get(f"{base_url}/{endpoint}")
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"Error retrieving data from {endpoint}: {e}")
        sys.exit(1)


def write_parquet(df, layer, table_name):
    path = f"/app/data/{layer}/{table_name}"
    print(f"Saving to layer {layer.upper()}: {path}")
    df.write.mode("overwrite").parquet(path)
    return path
