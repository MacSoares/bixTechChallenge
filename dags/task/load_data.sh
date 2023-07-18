#!/usr/bin/env bash

python3 -m pip install --upgrade pip
python3 -m pip install -r /opt/airflow/dags/task/requirements.txt
python3 /opt/airflow/dags/task/get_data.py