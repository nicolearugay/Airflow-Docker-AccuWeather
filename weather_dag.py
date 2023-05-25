# https://github.com/nicolearugay
# Uploaded 3/25/23

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
        city_dict = {331127: 'Victoria, Texas', 331109: 'Brownsville, Texas', 331130: 'Corpus Christi, Texas',
             331418: 'Olympia, Washington', 329144: 'Lake Charles, Louisiana', 331114: 'Galveston, Texas',
             335732: 'Port Arthur, Texas’, 347937: ‘Tampa, Florida’, 329674: ‘Rochester, New York’, 351409: ‘Seattle, Washington’}
        API_KEY = 'INSERT API KEY'
        final_string = ""

        for i, x in city_dict.items():
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
