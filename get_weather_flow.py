import json
import requests
from kafka import KafkaProducer
from prefect import flow, task
from prefect.variables import Variable
from prefect.logging import get_run_logger


# --- Configuration ---
API_KEY = Variable.get("openweathermap_api_key")
KAFKA_BOOTSTRAP_SERVERS = ['kafka:9092']
KAFKA_TOPIC = "openweather"


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
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )
    producer.send(topic, data)
    producer.flush()
    producer.close()


@flow
def weather_and_pollution_flow(cities: dict):
    """
    Main Prefect flow that fetches weather and air pollution data for each city
    and sends it to Kafka.
    """
    logger = get_run_logger()
    for city, coords in cities.items():
        lat = coords["lat"]
        lon = coords["lon"]

        kafka_event = {"location": city}

        # Fetch current weather data
        weather_data = get_weather_data(lat, lon)
        logger.info("weather for %s", city)
        logger.info(weather_data)
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
        logger.info("air pollution for %s", city)
        logger.info(air_pollution_data)
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
        logger.info("sending to kafka for %s", city)
        send_to_kafka.submit(KAFKA_TOPIC, kafka_event)


if __name__ == "__main__":# Default cities dictionary
    default_cities = {
        "Tehran": {"lat": "35.715298", "lon": "51.404343"},
        "Mashhad": {"lat": "36.310699", "lon": "59.599457"},
        "Qom": {"lat": "34.639999", "lon": "50.876389"},
        "Isfahan": {"lat": "32.661343", "lon": "51.680676"}  # Approximate coordinates
    }
    weather_and_pollution_flow(cities=default_cities)
