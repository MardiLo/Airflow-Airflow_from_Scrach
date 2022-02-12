from airflow import DAG
from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.operators.python_operator import PythonOperator                                
from datetime import datetime

import fetching_tweet
import cleaning_tweet

default_args = {
        "start_date":datetime(2020, 1, 1),
        "owner":"airflow"
}

with DAG(dag_id="twitter_dag", schedule_interval="@daily", default_args=default_args) as dag:
    # Watch folder = "/home/vagrant/airflow/dags/data/" and filename = "data.csv"
    waiting_for_tweets = FileSensor(task_id="waiting_for_tweets",
                                    fs_conn_id="fs_tweet",  # Find a path which Conn_id="fs_tweet" in config.
                                    filepath="data.csv",  # File or folder name
                                    poke_interval=5)
    
    # Call the main function in fetching_tweet.py
    # Transform local file "/home/vagrant/airflow/dags/data/data.csv" and create a new file 
    # at local path "/tmp/data_fetched.csv"
    fetching_tweets = PythonOperator(task_id="fetching_tweets",
                                     python_callable=fetching_tweet.main)
    
    # Call the main function in cleaning_tweet.py
    # Transform local file "/tmp/data_fetched.csv" and create a new file 
    # at local path "/tmp/data_cleaned.csv"
    cleaning_tweets = PythonOperator(task_id="cleaning_tweets",
                                     python_callable=cleaning_tweet.main)