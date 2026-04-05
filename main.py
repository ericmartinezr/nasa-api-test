import os
import requests
from dotenv import load_dotenv

load_dotenv()

# API para Asteroids - NeoWs

URL = "https://api.nasa.gov/neo/rest/v1/feed?start_date={start_date}&end_date={end_date}&api_key={api_key}"


def just_print_response(response):
    print(response)


def silver_response(response):
    asteroids = {}
    for date, asteroid_list in response["near_earth_objects"].items():
        asteroids[date] = []
        for asteroid in asteroid_list:
            asteroids[date].append({
                "name": asteroid["name"],
                "data_date": date,
                "is_potentially_hazardous": asteroid["is_potentially_hazardous_asteroid"],
                "estimated_diameter_km_min": asteroid["estimated_diameter"]["kilometers"]["estimated_diameter_min"],
                "estimated_diameter_km_max": asteroid["estimated_diameter"]["kilometers"]["estimated_diameter_max"]
            })
    print(asteroids)


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
