from datetime import datetime
import logging
from airflow import DAG
from airflow.operators.python import PythonOperator


def get_data():
    import requests

    logging.info('Start requesting the api')
    res = requests.get('https://randomuser.me/api/')
    res = res.json()
    res = res['results'][0]

    return res


def format_data(res):
    data = {}
    logging.info('Format_data')
    location = res['location']
    data['first_name'] = res['name']['first']
    data['last_name'] = res['name']['last']
    data['gender'] = res['gender']
    data['address'] = f"{str(location['street']['number'])} {location['street']['name']} " \
                      f"{location['city']} {location['state']} {location['country']}"
    data['postcode'] = location['postcode']
    data['email'] = res['email']
    data['username'] = res['login']['username']
    data['dob'] = res['dob']['date']
    data['registered_date'] = res['registered']['date']
    data['phone'] = res['phone']
    data['picture'] = res['picture']['medium']

    return data


def stream_data(**kwargs):
    import json

    ti = kwargs['ti']
    print(ti)
    res = get_data()
    res = format_data(res)
    print(json.dumps(res, indent=3))


default_args = {
    'owner': 'Quentin',
    'start_date': datetime(2024, 4, 20, 10, 00),
}

with DAG(dag_id='KAFKA_STREAM',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False,
         tags=['STREAMING_DATA', 'DAILY'],
         ) as dag:

    streaming_task = PythonOperator(
        task_id="stream_data_from_api",
        python_callable=stream_data,
        op_kwargs={
            'test': 'test',
        }
    )
