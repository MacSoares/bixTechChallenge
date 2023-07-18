import airflow
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG("run_load_data",  # Dag id
         start_date=airflow.utils.dates.days_ago(1),
         schedule_interval='0 * * * *',
         catchup=False  # Catchup
         ) as dag:

    get_from_db = BashOperator(
        task_id="load_full_data",
        bash_command="task/load_data.sh",
        dag=dag
    )
