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
    from kafka import KafkaProducer
    import time
    import logging

    ti = kwargs['ti']
    print(ti)

    # Publish record to the kafka cluster
    producer = KafkaProducer(boostrap_servers=['broker:29092'],
                             max_block_ms=5000)

    curr_time = time.time()

    while True:
        if time.time() > curr_time + 60:
            break
        try:
            logging.info(f"Call API")
            res = get_data()
            logging.info(f"Format data")
            res = format_data(res)

            logging.info(
                f"Send the following data {json.dumps(res, indent=3)}")
            # pushing data to the queu. Mechanism here is message-queu for streaming data.
            producer.send(topic='user_created',
                          value=json.dumps(res).encode('utf-8'))
            logging.info("The data has been send.")
        except Exception as e:
            logging.error(f"An error occured: {e}")
            continue


default_args = {
    'owner': 'Quentin',
    'start_date': datetime(2024, 4, 20, 10, 00),
}

with DAG(dag_id='USER_AUTOMATION',
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
