#!/bin/bash
export AIRFLOW_HOME=airflow
export export AIRFLOW__CORE__FERNET_KEY=$(cat fernet.key)

airflow scheduler &
airflow webserver -p 8080 &

