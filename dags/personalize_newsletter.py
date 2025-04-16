import os

from airflow.decorators import task
from airflow.sdk import dag, Asset
from pendulum import datetime, duration

_WEATHER_URL = (
    "https://api.open-meteo.com/v1/forecast?"
    "latitude={lat}&longitude={long}&current="
    "temperature_2m,relative_humidity_2m,"
    "apparent_temperature"
)

OBJECT_STORAGE_SYSTEM = os.getenv("OBJECT_STORAGE_SYSTEM", default="file")
OBJECT_STORAGE_CONN_ID = os.getenv("OBJECT_STORAGE_CONN_ID", default=None)
OBJECT_STORAGE_PATH_NEWSLETTER = os.getenv(
    "OBJECT_STORAGE_PATH_NEWSLETTER",
    default="include/newsletter",
)
OBJECT_STORAGE_PATH_USER_INFO = os.getenv(
    "OBJECT_STORAGE_PATH_USER_INFO",
    default="include/user_data",
)  

SYSTEM_PROMPT = (
    "You are {favorite_sci_fi_character} "
    "giving advice to your best friend {name}. "
    "{name} once said '{motivation}' and today "
    "they are especially in need of some encouragement. "
    "Please write a personalized quote for them "
    "based on the historic quotes provided, include "
    "an insider reference to {series} that only someone "
    "who has seen it would understand. "
    "Do NOT include the series name in the quote. "
    "Do NOT verbatim repeat any of the provided quotes. "
    "The quote should be between 200 and 500 characters long."
)


def _get_lat_long(location):
    import time

    from geopy.geocoders import Nominatim

    time.sleep(10)
    geolocator = Nominatim(user_agent="my-newsletter-app-3")

    location = geolocator.geocode(location)

    return (
        float(location.latitude),
        float(location.longitude),
    )


@dag(
    start_date=datetime(2025, 3, 1),
    schedule=[Asset("formatted_newsletter")],
    default_args={
        "retries": 2,
        "retry_delay": duration(minutes=3),
    },
)
def personalize_newsletter():
    @task
    def get_user_info() -> list[dict]:
        import json

        from airflow.io.path import ObjectStoragePath

        object_storage_path = ObjectStoragePath(
            f"{OBJECT_STORAGE_SYSTEM}://" f"{OBJECT_STORAGE_PATH_USER_INFO}",
            conn_id=OBJECT_STORAGE_CONN_ID,
        )  

        user_info = []
        for file in object_storage_path.iterdir():
            if file.is_file() and file.suffix == ".json":

                bytes = file.read_block(offset=0, length=None)  

                user_info.append(json.loads(bytes))

        return user_info  

    _get_user_info = get_user_info()  

    @task(max_active_tis_per_dag=1, retries=4)
    def get_weather_info(user: dict) -> dict:  
        import requests

        lat, long = _get_lat_long(user["location"])  
        r = requests.get(_WEATHER_URL.format(lat=lat, long=long))
        user["weather"] = r.json()

        return user

    _get_weather_info = get_weather_info.expand(user=_get_user_info)

    @task(max_active_tis_per_dag=16)  
    def create_personalized_quote(
        system_prompt,
        user,
        **context,  
    ):
        import re

        from airflow.io.path import ObjectStoragePath
        from airflow.providers.openai.hooks.openai import (
            OpenAIHook,
        )

        my_openai_hook = OpenAIHook(conn_id="my_openai_conn")  
        client = my_openai_hook.get_conn()  

        id = user["id"]
        name = user["name"]
        motivation = user["motivation"]
        favorite_sci_fi_character = user["favorite_sci_fi_character"]  
        series = favorite_sci_fi_character.split(" (")[1].replace(")", "")  
        date = context["dag_run"].run_after.strftime("%Y-%m-%d")

        object_storage_path = ObjectStoragePath(
            f"{OBJECT_STORAGE_SYSTEM}://{OBJECT_STORAGE_PATH_NEWSLETTER}",
            conn_id=OBJECT_STORAGE_CONN_ID,
        )  

        date_newsletter_path = object_storage_path / f"{date}_newsletter.txt"

        newsletter_content = date_newsletter_path.read_text()

        quotes = re.findall(r'\d+\.\s+"([^"]+)"', newsletter_content)  

        system_prompt = system_prompt.format(
            favorite_sci_fi_character=favorite_sci_fi_character,
            motivation=motivation,
            name=name,
            series=series,
        )  
        user_prompt = "The quotes to modify are:\n" + "\n".join(quotes)  

        completion = client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {
                    "role": "system",
                    "content": system_prompt,
                },
                {
                    "role": "user",
                    "content": user_prompt,
                },
            ],
        )  

        generated_response = completion.choices[0].message.content  

        return {
            "user_id": id,
            "personalized_quote": generated_response,
        }

    _create_personalized_quote = create_personalized_quote.partial(
        system_prompt=SYSTEM_PROMPT
    ).expand(
        user=_get_user_info
    )  

    @task
    def combine_information(
        user_info: list[dict],
        personalized_quotes: list[dict],
    ) -> None:
        user_info_dict = {user["id"]: user for user in user_info}
        for quote in personalized_quotes:
            user_id = quote["user_id"]
            user_info_dict[user_id]["personalized_quote"] = quote["personalized_quote"]

        return list(user_info_dict.values())

    _combine_information = combine_information(
        user_info=_get_weather_info,
        personalized_quotes=_create_personalized_quote,
    )

    @task
    def create_personalized_newsletter(
        user: list[dict],
        **context: dict,
    ) -> None:
        import textwrap

        from airflow.io.path import ObjectStoragePath

        date = context["dag_run"].run_after.strftime("%Y-%m-%d")

        id = user["id"]
        name = user["name"]
        location = user["location"]
        favorite_sci_fi_character = user["favorite_sci_fi_character"]
        character_name = favorite_sci_fi_character.split(" (")[0]
        actual_temp = user["weather"]["current"]["temperature_2m"]
        apparent_temp = user["weather"]["current"]["apparent_temperature"]
        rel_humidity = user["weather"]["current"]["relative_humidity_2m"]
        quote = user["personalized_quote"]
        wrapped_quote = textwrap.fill(quote, width=50)

        new_greeting = (
            f"Hi {name}! \n\nIf you venture outside right now in {location}, "
            f"you'll find the temperature to be {actual_temp}°C, but it will "
            f"feel more like {apparent_temp}°C. The relative humidity is "
            f"{rel_humidity}%."
        )

        object_storage_path = ObjectStoragePath(
            f"{OBJECT_STORAGE_SYSTEM}://" f"{OBJECT_STORAGE_PATH_NEWSLETTER}",
            conn_id=OBJECT_STORAGE_CONN_ID,
        )

        daily_newsletter_path = object_storage_path / f"{date}_newsletter.txt"

        generic_content = daily_newsletter_path.read_text()

        updated_content = generic_content.replace(
            "Hello Cosmic Traveler,", new_greeting
        )

        personalized_quote = (
            f"\n-----------\n"
            f"This is what {character_name} might say to you today:\n\n"
            f"{wrapped_quote}\n\n"
            f"-----------"
        )

        updated_content = updated_content.replace(
            "Have a fantastic journey!",
            f"{personalized_quote}\n\nHave a fantastic journey!",
        )

        personalized_newsletter_path = (
            object_storage_path / f"{date}_newsletter_userid_{id}.txt"
        )

        personalized_newsletter_path.write_text(updated_content)

    create_personalized_newsletter.expand(user=_combine_information)


personalize_newsletter()
