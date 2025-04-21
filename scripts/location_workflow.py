from airflow import DAG
from airflow.decorators import task
from airflow.utils.task_group import TaskGroup
from datetime import datetime
import requests
import os
import json

API_HEADERS = {
    'X-RapidAPI-Key': "85dc66bf03msh94fe37cd776607ep191392jsn308869d0f8ef",
    'X-RapidAPI-Host': "weatherapi-com.p.rapidapi.com"
}

OUTPUT_DIR = '/tmp/weather_data'

with DAG("weather_location_workflow",
         start_date=datetime(2024, 1, 1),
         schedule_interval=None,
         catchup=False):
    
    @task
    def print_message():
        print("The end")

    # Define the task group
    with TaskGroup("location_fetch_group") as weather_group:
        @task
        def get_zip_codes():
            return ["75189", "72956", "80525", "80209", "72903", "72762", "74966", "72714"]
        
        @task
        def fetch_and_save_weather(zip_code: str):
            url = f"https://weatherapi-com.p.rapidapi.com/forecast.json?q={zip_code}&days=3"

            try:
                response = requests.get(url, headers=API_HEADERS, timeout=10)
                response.raise_for_status()
                data = response.json()

                zip_folder = OUTPUT_DIR
                # zip_folder = os.path.join(OUTPUT_DIR, zip_code)
                os.makedirs(zip_folder, exist_ok=True)

                file_path = os.path.join(zip_folder, f"{zip_code}.json")
                with open(file_path, "w") as f:
                    json.dump(data, f, indent=2)

                print(f"Saved forecast for {zip_code} to {file_path}")

            except requests.exceptions.RequestException as e:
                print(f"Failed to fetch weather for {zip_code}: {e}")

        # Use expand inside the group
        fetch_and_save_weather.expand(zip_code=get_zip_codes())

    weather_group >> print_message() 


