from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def print_params(**kwargs):
    """
    Print all parameters passed to this function.
    """
    print("Received parameters:")
    for key, value in kwargs.items():
        print(f"{key}: {value}")
    
    # If there are any dag_run conf parameters, print those too
    dag_run = kwargs.get('dag_run')
    if dag_run and dag_run.conf:
        print("DAG Run configuration:")
        for key, value in dag_run.conf.items():
            print(f"{key}: {value}")

# Define the DAG
dag = DAG(
    'l3_task_dag',
    default_args=default_args,
    description='A placeholder DAG that prints all parameters',
    schedule_interval=None,
    catchup=False
)

# Define the task
print_params_task = PythonOperator(
    task_id='print_params',
    python_callable=print_params,
    provide_context=True,
    dag=dag,
)

print_params_task