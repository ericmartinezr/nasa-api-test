import os
import requests
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from datetime import datetime


load_dotenv()

# API para Asteroids - NeoWs

URL = "https://api.nasa.gov/neo/rest/v1/feed?start_date={start_date}&end_date={end_date}&api_key={api_key}"

spark = SparkSession.builder.appName("NASA Asteroids").getOrCreate()


def just_print_response(response):
    print(response)


def silver_response(response):
    asteroids = []
    today = datetime.today().strftime('%Y-%m-%d')
    for date, asteroid_list in response["near_earth_objects"].items():
        for asteroid in asteroid_list:
            obj_asteroid = {
                "name": asteroid["name"],
                "process_date": today,
                "data_date": date,
                "is_potentially_hazardous": asteroid["is_potentially_hazardous_asteroid"],
                "estimated_diameter_km_min": asteroid["estimated_diameter"]["kilometers"]["estimated_diameter_min"],
                "estimated_diameter_km_max": asteroid["estimated_diameter"]["kilometers"]["estimated_diameter_max"],
                "kms_per_second": float(asteroid["close_approach_data"][0]["relative_velocity"]["kilometers_per_second"]),
                "kms_per_hour": float(asteroid["close_approach_data"][0]["relative_velocity"]["kilometers_per_hour"]),
                "miss_distance_km": float(asteroid["close_approach_data"][0]["miss_distance"]["kilometers"]),
            }

            # Derived columns
            obj_asteroid["estimated_diameter_km_avg"] = (
                obj_asteroid["estimated_diameter_km_min"] + obj_asteroid["estimated_diameter_km_max"]) / 2
            obj_asteroid["is_large"] = obj_asteroid["estimated_diameter_km_avg"] > 1
            obj_asteroid["is_fast"] = float(
                obj_asteroid["kms_per_second"]) > 10

            asteroids.append(obj_asteroid)

    df = spark.createDataFrame(asteroids)
    df.write.mode("overwrite").parquet(
        path=f"bronze/{today}/asteroids.parquet",
        partitionBy=["process_date"]
    )


def gold_response(response):
    pass


def main():
    url = URL.format(start_date="2024-06-01",
                     end_date="2024-06-07", api_key=os.getenv("NASA_API_KEY"))

    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()

        silver_response(data)

    except Exception as e:
        print(f"An error occurred: {e}")


if __name__ == "__main__":
    main()
