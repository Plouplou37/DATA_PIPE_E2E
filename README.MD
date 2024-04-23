This repository is to implemented and develop a end-to-end data pipeline as shown below:

![System_Architecture]("image/Data engineering architecture.png")

- git init
- ...
- python venv myenv_name
- pip install apache-airflow (platform to schedule and monitor workflows)
- mkdir dags
- touch kafka_stream.py
- touch docker-compose.yml
- docker --version
- docker compose up -d
- pip install kafka-python (to use kafka cluster, platform to process real-time data)
- pip install cassandra-driver to communicate with cassandra
- pip install spark pyspark (kind of a middleware between kafka cluster and cassandra database)

spark-submit --master spark://localhost:7077 spark_streams.py --> need to run before triger dag because we need a worker to listen to the streaming data.

Note: The purpose of the docker-compose.yml is to define the whole architecture using micro-services !
