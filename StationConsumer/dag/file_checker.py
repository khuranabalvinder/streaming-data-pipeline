from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2018, 11, 8),
    'email': ['astahl@thoughtworks.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG('file_checker_dag', default_args=default_args, schedule_interval='*/3 * * * *')

file_check_task = """
export AWS_DEFAULT_REGION=us-east-2
step=$(aws emr add-steps --cluster-id j-2ZVH9WBXFFPPN --steps Type=SPARK,Name="File check",ActionOnFailure=CONTINUE,Args=[--class,com.free2wheelers.apps.FileChecker,/home/hadoop/free2wheelers/fileChecker.jar,/free2wheelers/stationMart/data/test.csv] | python -c 'import json,sys;obj=json.load(sys.stdin);print obj.get("StepIds")[0];')
echo '========='$step
aws emr wait step-complete --cluster-id j-2ZVH9WBXFFPPN --step-id $step
"""

o1 = BashOperator(
    task_id='file_check_task',
    bash_command=file_check_task,
    dag=dag)
