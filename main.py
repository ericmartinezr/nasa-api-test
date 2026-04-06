import os
import requests
import json
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum, avg, min, year, to_date, lit
from datetime import datetime


load_dotenv()

# API para Asteroids - NeoWs
URL = "https://api.nasa.gov/neo/rest/v1/feed?start_date={start_date}&end_date={end_date}&api_key={api_key}"


spark = SparkSession.builder.appName("NASA Asteroids").getOrCreate()


def just_print_response(response):
    print(response)


def bronze_response(response):
    today = datetime.today().strftime('%Y-%m-%d')
    # Use json.dumps to ensure valid JSON strings for spark.read.json
    json_data = json.dumps(response["near_earth_objects"], indent=2)
    json_rdd = spark.sparkContext.parallelize([json_data])
    df = spark.read.json(json_rdd)
    df.write.mode("overwrite").json(f"bronze/{today}/asteroids.json")


def silver_response(response):
    today = datetime.today().strftime('%Y-%m-%d')

    # Read from Bronze (following medallion architecture)
    try:
        bronze_df = spark.read.json(f"bronze/{today}/asteroids.json")
        # Extract the dictionary back from the single row in the bronze parquet
        bronze_data = bronze_df.collect()[0].asDict()
    except Exception as e:
        print(f"Error reading bronze data: {e}")
        return

    # In Bronze, each date is a column. We process this dictionary to create our Silver DataFrame.
    asteroids = []

    for date_str, asteroid_list in bronze_data.items():
        if not asteroid_list:  # Skip null columns if any
            continue

        for asteroid in asteroid_list:
            obj_asteroid = {
                "name": asteroid["name"],
                "process_date": today,
                "date": date_str,
                "hazardous": bool(asteroid["is_potentially_hazardous_asteroid"]),
                "estimated_diameter_km_min": float(asteroid["estimated_diameter"]["kilometers"]["estimated_diameter_min"]),
                "estimated_diameter_km_max": float(asteroid["estimated_diameter"]["kilometers"]["estimated_diameter_max"]),
                "velocity_km_s": float(asteroid["close_approach_data"][0]["relative_velocity"]["kilometers_per_second"]),
                "miss_distance_km": float(asteroid["close_approach_data"][0]["miss_distance"]["kilometers"]),
            }

            # Derived columns
            obj_asteroid["avg_diameter_km"] = (
                obj_asteroid["estimated_diameter_km_min"] + obj_asteroid["estimated_diameter_km_max"]) / 2
            asteroids.append(obj_asteroid)

    silver_df = spark.createDataFrame(asteroids)

    # Add year column for Gold layer partitioning
    silver_df = silver_df.withColumn("year", year(to_date(col("date"))))

    # Write to Silver partitioned by date
    silver_df.write.mode("overwrite").partitionBy(
        "date").parquet(f"silver/{today}/asteroids.parquet")
    print(f"Silver layer written to silver/{today}/asteroids.parquet")


# Procesa los datos de silver y guarda los datos transformados en formato parquet por agregacion
# Ej: Resumen diario, puntaje de riesgo, etc
def gold_response(response):
    today = datetime.today().strftime('%Y-%m-%d')

    # Read from Silver layer
    try:
        silver_df = spark.read.parquet(f"silver/{today}/asteroids.parquet")
    except Exception as e:
        print(f"Error reading silver data: {e}")
        return

    # First aggregation: Daily summary
    gold_df = silver_df.groupBy("date", "year").agg(
        count("*").alias("total_asteroids"),
        sum(col("hazardous").cast("int")).alias("hazardous_count"),
        avg("velocity_km_s").alias("avg_velocity"),
        min("miss_distance_km").alias("closest_distance")
    )

    gold_df.write.mode("overwrite") \
        .partitionBy("year") \
        .parquet("gold/asteroid_daily_summary/")

    print("Gold Daily Summary written to gold/asteroid_daily_summary/")

    # Second aggregation: Risk scores
    gold_risk_df = silver_df.withColumn(
        "risk_score",
        col("velocity_km_s") * 0.4 +
        col("avg_diameter_km") * 0.4 +
        col("hazardous").cast("int") * 0.2
    )

    gold_risk_df.write.mode("overwrite") \
        .partitionBy("date") \
        .parquet("gold/asteroid_risk_scores/")

    print("Gold Risk Scores written to gold/asteroid_risk_scores/")


def main():
    url = URL.format(start_date="2024-06-01",
                     end_date="2024-06-07", api_key=os.getenv("NASA_API_KEY"))

    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()

        # bronze_response(data)
        silver_response(data)
        # gold_response(data)

    except Exception as e:
        print(f"An error occurred: {e}")


if __name__ == "__main__":
    main()
