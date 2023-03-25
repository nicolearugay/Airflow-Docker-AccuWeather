from airflow import DAG
from datetime import timedelta, datetime
from airflow.operators.python import PythonOperator
import json
import requests
from datetime import datetime 
import pandas as pd

default_args = {
        'owner' : 'airflow',
        'start_date' : datetime(2023, 3, 10),

}

# Creating DAG Object
dag = DAG(dag_id='weather_dag_2',
        default_args=default_args,
        schedule_interval='*/3 * * * *', 
        catchup=False
    )


def _notify():
        keys = [331127,331109,331130,331418,329144,331114,335732]
        cities = ['Victoria, Texas', 'Brownsville, Texas', 'Corpus Christi, Texas', 'Olympia, Washington',
        'Lake Charles, Louisiana', 'Galveston, Texas', 'Port Arthur, Texas']
        API_KEY = 'INSERT API KEY'
        final_string = ""

        for (i, x) in zip(keys,cities):
            url = f"http://dataservice.accuweather.com/currentconditions/v1/{i}?apikey={API_KEY}&details=true"
            response = requests.get(url)
            if response.status_code == 200:
                json_data = json.loads(response.text)
                flat_json = json_data[0]
                if flat_json['RelativeHumidity'] >= 60:
                    final_string += x + f" ({flat_json['RelativeHumidity']}). "
                else:
                    continue
            else:
                print(f"Error fetching weather data: {response.status_code}")

        print(f"Cities with relative humidity equal to or above 60% today: {final_string}")
 


# Creating tasks
notify = PythonOperator(
 task_id="notify",
 python_callable=_notify,
 dag=dag
)

 # Setting up dependencies 
notify