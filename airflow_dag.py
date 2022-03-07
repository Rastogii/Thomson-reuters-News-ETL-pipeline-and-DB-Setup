from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
import os

ip = ''
pvt_key_name = '/home/hadoop/pvt2.ppk'
user = 'vallari'
fileName = '2013-07-00.csv.gz'
srcDir = os.getcwd() + '/src/'
sparkSubmit = '/usr/lib/spark/bin/spark-submit'

## Define the DAG object
default_args = {
    'owner': 'insight-dan',
    'depends_on_past': False,
    'start_date': datetime(2020, 7, 3),
    'retries': 5,
    'retry_delay': timedelta(minutes=1)
    #'retry_delay': '@daily'
}
dag = DAG('thomsonreuters', default_args=default_args, schedule_interval='@daily')

'''
Defining three tasks: one task to download S3 data
and two Spark jobs that depend on the data to be 
successfully downloaded
task to download data
'''
downloadData= BashOperator(
    task_id='download-data',
    bash_command='sftp -i ' + pvt_key_name + ' ' + user + '@' + ip,
    dag=dag)

#task to compute number of unique authors
unZip = BashOperator(
    task_id='unzip',
    bash_command='tar -xvzf ' + fileName,
    dag=dag)
unZip.set_upstream(downloadData)

preprocessAddToES = BashOperator(
    task_id='preprocess-and-add-to-ES',
    bash_command=sparkSubmit + ' home/hadoop/file.py',
    dag=dag)
#Specify that this task depends on the downloadData task
preprocessAddToES.set_upstream(unZip)
