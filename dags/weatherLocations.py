from __future__ import annotations

from datetime import datetime

from airflow.decorators import task
from airflow.models.dag import DAG

with DAG(dag_id="process_location", 
         schedule=None, 
         start_date=datetime(2025, 4, 4), 
         catchup=False) as dag:
    
    @task
    def getLocations(zipCode: str):

        return zipCode
    
    @task
    def printZipCode(locations):
        for location in locations:
            print(location)

    locations = getLocations.expand(zipCode=['75189','72956','80209','80525','72903','72762'])
    printZipCode(locations)

    


    
