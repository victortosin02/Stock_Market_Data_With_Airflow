from airflow import DAG
from datetime import datetime
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.providers.amazon.aws.operators.sns import SnsPublishOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 0
}

dag = DAG(
    dag_id='etl_pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    start_date=datetime.now(),
    template_searchpath='/opt/airflow/s3-drive/Scripts/',
)

def determine_next_task(**context):
	dagrun: DAG = context[dag_run]
	dag_tasks = {}
	for ti in dagrun.get_task_instances():
		dag_tasks[ti.task_id] = str(ti.state)
	task_status = dag_tasks['Load_to_Database']
	if task_status == 'success':
		return 'Success_Notification'
	else:
		return 'Failure_Notification'

data_processing= BashOperator(
    task_id = 'Data_Processing',
    bash_command='python /opt/airflow/s3-drive/Scripts/python_data_processing_walkthrough.py',
    dag=dag,
)

data_loading = PostgresOperator(
    task_id = 'Data_Loading',
    postgres_conn_id = 'rds-connect',
    sql = '/opt/airflow/s3-drive/Scripts/loading script.sql',
    dag=dag,
)

decide_task = BranchPythonOperator(
    task_id = 'Determine Notification',
    python_callable=determine_next_task,
    provide_context=True,
    dag=dag
)

success_publisher = SnsPublishOperator(
    task_id = 'Success_Notification',
    aws_conn_id = 'aws_conn_id',
    target_arn = 'arn:aws:sns:eu-west-2:624965154135:de-mbd-predict-victor-oladejo-SNS',
    message = 'ETL pipeline executed successfully',
    subject = 'SUCCESS',
    dag=dag
)

failure_publisher = SnsPublishOperator(
    task_id = 'Failure_Notification',
    aws_conn_id = 'aws_conn_id',
    target_arn = 'arn:aws:sns:eu-west-2:624965154135:de-mbd-predict-victor-oladejo-SNS',
    message = 'ETL pipeline executed successfully',
    subject = 'SUCCESS',
    dag=dag
)
start = DummyOperator(task_id='Start', dag=dag)
downstream_task = DummyOperator(task_id='Messaging', trigger_rule='all_done')
notification_sent = DummyOperator(task_id='Notification_Sent', trigger_rule='one_success', dag=dag)
end = DummyOperator(task_id='End', dag=dag)

data_processing >> data_loading >> downstream_task >> decide_task
decide_task >> success_publisher >> notification_sent >> end
decide_task >> failure_publisher >> notification_sent >> end
