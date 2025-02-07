import json
import requests
import h3
import time
from kafka import KafkaProducer
from prefect import flow, task
from prefect.variables import Variable
from prefect.logging import get_run_logger


# --- Configuration ---
API_KEY = Variable.get("openweathermap_api_key")
KAFKA_BOOTSTRAP_SERVERS = ["kafka:9092"]
KAFKA_TOPIC = "openweather"


def get_data():
    data = None
    with open("iran_city.json") as f:
        data = json.load(f)
    return data


def get_city_hexagons(data, cities):
    """
    input: data: json file containing the boundries of the cities in Iran
    output: city_hexagons: a dictionary containing the hexagons of the cities in Iran
    """
    city_hexagons = dict()
    for city in data["features"]:
        city_name_fa, city_name_en = city["properties"]["ADM2_FA"], city["properties"]["ADM2_EN"]
        if city_name_en not in cities:
            continue

        if len(city["geometry"]["coordinates"]) == 1:
            city["geometry"]["coordinates"] = [city["geometry"]["coordinates"]]

        for boundry in city["geometry"]["coordinates"]:
            boundry_coordinates = boundry[0]

            for i in range(len(boundry_coordinates)):
                boundry_coordinates[i] = boundry_coordinates[i][::-1]

            hexagons = list(h3.polygon_to_cells(h3.LatLngPoly(boundry_coordinates), 6))

            city_hexagons[(city_name_en, city_name_fa)] = city_hexagons.get((city_name_en, city_name_fa), []) + hexagons

    return city_hexagons


@task
def get_weather_data(lat: str, lon: str) -> dict:
    """
    Fetches current weather data from OpenWeatherMap for the given coordinates.
    """
    url = f"https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={API_KEY}"
    response = requests.get(url)
    response.raise_for_status()
    return response.json()


@task
def get_air_pollution_data(lat: str, lon: str) -> dict:
    """
    Fetches current air pollution data from OpenWeatherMap for the given coordinates.
    """
    url = f"http://api.openweathermap.org/data/2.5/air_pollution?lat={lat}&lon={lon}&appid={API_KEY}"
    response = requests.get(url)
    response.raise_for_status()
    return response.json()


@task
def send_to_kafka(topic: str, data: dict) -> None:
    """
    Sends the given data as a JSON message to the specified Kafka topic.
    """
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS, value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )
    producer.send(topic, data)
    producer.flush()
    producer.close()


def flat(cities_hexagons):
    result = []
    for city, hexagons in cities_hexagons.items():
        for hexagon in hexagons:
            result.append((*city, hexagon))
    return result


@flow
def weather_and_pollution_flow(cities: dict):
    """
    Main Prefect flow that fetches weather and air pollution data for each cities
    and sends it to Kafka.
    """
    logger = get_run_logger()
    for city, coords in cities.items():
        lat = coords["lat"]
        lon = coords["lon"]

        kafka_event = {"location": city}

        # Fetch current weather data
        weather_data = get_weather_data(lat, lon)
        logger.debug("weather for %s", city)
        logger.debug(weather_data)
        # Add city name for clarity
        kafka_event["dt"] = weather_data["dt"]
        kafka_event["temp"] = weather_data["main"]["temp"]
        kafka_event["pressure"] = weather_data["main"]["pressure"]
        kafka_event["humidity"] = weather_data["main"]["humidity"]
        kafka_event["clouds_all"] = weather_data["clouds"]["all"]
        kafka_event["weather_main"] = weather_data["weather"][0]["main"]
        kafka_event["weather_description"] = weather_data["weather"][0]["description"]

        # Fetch current air pollution data
        air_pollution_data = get_air_pollution_data(lat, lon)
        logger.debug("air pollution for %s", city)
        logger.debug(air_pollution_data)
        # Add city name for clarity
        kafka_event["pol_aqi"] = air_pollution_data["list"][0]["main"]["aqi"]
        kafka_event["pol_co"] = air_pollution_data["list"][0]["components"]["co"]
        kafka_event["pol_no"] = air_pollution_data["list"][0]["components"]["no"]
        kafka_event["pol_no2"] = air_pollution_data["list"][0]["components"]["no2"]
        kafka_event["pol_o3"] = air_pollution_data["list"][0]["components"]["o3"]
        kafka_event["pol_so2"] = air_pollution_data["list"][0]["components"]["so2"]
        kafka_event["pol_pm2_5"] = air_pollution_data["list"][0]["components"]["pm2_5"]
        kafka_event["pol_pm10"] = air_pollution_data["list"][0]["components"]["pm10"]
        kafka_event["pol_nh3"] = air_pollution_data["list"][0]["components"]["nh3"]

        # Send data to Kafka topics asynchronously
        logger.debug("sending to kafka for %s", city)
        send_to_kafka.submit(KAFKA_TOPIC, kafka_event)


@flow
def weather_and_pollution_flow_v2(cities: list[str]):
    """
    Main Prefect flow that fetches weather and air pollution data for each distributed points
    in cities and sends it to Kafka.
    """
    logger = get_run_logger()
    cities_data = get_data()
    cities_hexagons = get_city_hexagons(cities_data, cities)
    logger.debug("fetching data for %d cities", len(cities_hexagons))
    flatten_city_hexagons = flat(cities_hexagons)
    for city_en, city_fa, cell in flatten_city_hexagons:
        start_time = time.time()

        kafka_event = {"city_en": city_en, "city_fa": city_fa, "cell": cell}
        lat, lon = h3.cell_to_latlng(cell)

        # Fetch current weather data
        weather_data = get_weather_data(lat, lon)
        logger.debug("weather for (%s, %s, %s)", city_en, city_fa, cell)
        logger.debug(weather_data)
        # Add city name for clarity
        kafka_event["dt"] = weather_data["dt"]
        kafka_event["temp"] = weather_data["main"]["temp"]
        kafka_event["pressure"] = weather_data["main"]["pressure"]
        kafka_event["humidity"] = weather_data["main"]["humidity"]
        kafka_event["clouds_all"] = weather_data["clouds"]["all"]
        kafka_event["weather_main"] = weather_data["weather"][0]["main"]
        kafka_event["weather_description"] = weather_data["weather"][0]["description"]

        # Fetch current air pollution data
        air_pollution_data = get_air_pollution_data(lat, lon)
        logger.debug("air pollution for (%s, %s, %s)", city_en, city_fa, cell)
        logger.debug(air_pollution_data)
        # Add city name for clarity
        kafka_event["pol_aqi"] = air_pollution_data["list"][0]["main"]["aqi"]
        kafka_event["pol_co"] = air_pollution_data["list"][0]["components"]["co"]
        kafka_event["pol_no"] = air_pollution_data["list"][0]["components"]["no"]
        kafka_event["pol_no2"] = air_pollution_data["list"][0]["components"]["no2"]
        kafka_event["pol_o3"] = air_pollution_data["list"][0]["components"]["o3"]
        kafka_event["pol_so2"] = air_pollution_data["list"][0]["components"]["so2"]
        kafka_event["pol_pm2_5"] = air_pollution_data["list"][0]["components"]["pm2_5"]
        kafka_event["pol_pm10"] = air_pollution_data["list"][0]["components"]["pm10"]
        kafka_event["pol_nh3"] = air_pollution_data["list"][0]["components"]["nh3"]

        # Send data to Kafka topics asynchronously
        logger.debug("sending to kafka for (%s, %s, %s)", city_en, city_fa, cell)
        send_to_kafka.submit(KAFKA_TOPIC, kafka_event)

        while time.time() - start_time < 1:
            pass


if __name__ == "__main__":  # Default cities
    default_cities = {
        "Tehran": {"lat": "35.715298", "lon": "51.404343"},
        "Mashhad": {"lat": "36.310699", "lon": "59.599457"},
        "Qom": {"lat": "34.639999", "lon": "50.876389"},
        "Isfahan": {"lat": "32.661343", "lon": "51.680676"},
    }
    weather_and_pollution_flow(cities=default_cities)

    default_cities = [
        "Tehran",
        "Qom",
        "Mashhad",
        "Isfahan",
        "Shiraz",
        "Ahvaz",
        "Karaj",
        "Tabriz",
        "Kermanshah",
        "Urumia",
        "Rasht",
        "Yazd",
        "Bandar-Abbas",
        "Hamadan",
        "Arak",
        "Sari",
    ]
    weather_and_pollution_flow_v2(cities=default_cities)
