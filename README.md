# Thomson-reuters-News-ETL-pipeline-and-DB-Setup
Set Up:
1. Spawn an AWS EMR 6.3.1 cluster - Big data stack should have Spark.
2. Spawn a m5.xlarge / m5.2xlarge Ubuntu18.04 VM on AWS.

Prerequisites Setup on AWS EMR:
1. ssh to AWS EMR cluster master node using 
'ssh -i xx.pem hadoop@ip-address'
2. Copy pipeline_prerequisites.sh to this node, as well as the private key to Remote server where the CSVs are located.
3. Run:
chmod +x pipeline_prerequisites.sh
./pipeline_prerequisites.sh
4. Change Driver memory in spark-deafults.conf to 15GB to avoid OOM errors in file '/usr/lib/spark/conf/spark-defaults.conf'.

Prerequisites to Setup ElasticSearch DB and configure table schema on m5.xlarge / m5.2xlarge VM
1. ssh to VM
2. Copy 'elastic.sh' to the VM
3. Run:
chmod +x elastic.sh
./elastic.sh

Airflow Scheduling:
1. 'airflow_dag.py' automates fetching tar files from Remote server, untar them and run pyspark application @daily.
2. Make sure sqlite3 has version > 3.15.0. AWS EMR default image may have older sqlite version.
3. Steps to Run:
Copy airflow_dag.py, file.py / completeCSVetlFile.py to AWS EMR cluster at '/home/hadoop/airflow_dag.py' and 'home/hadoop/file.py'.
Set these variables:
  ip = 'ip-address-remote-server'
  pvt_key_name = '/location/to/private-key.pem'
  user = 'username'
  fileName = '2013-07-01.csv.gz'
  
Run the file by 'python airflow_dag.py'

Pyspark application:
'file.py' processes partial CSV and 'completeCSVetlFile.py' processes complete CSV. It expects the jar is untar-ed and sitting in
To run either:
1. Copy this file to AWS EMR master node at '/home/hadoop/file.py'. 
2. Setup these varaibles:
  "es.nodes",  "x.x.x.x" //public-ip of ES Node
  "es.port" , "9200"
  "es.resource","thomreuters/2013-07-01 
   CSV file Name - '2013-07-01.csv' on line 18
 3. Run 
 'spark-submit --master yarn --deploy-mode client file.py'
 
 This pyspark file does:
 
It will read csv file and record the index where <“date”,”time”> pattern is matching using regex.
It will divide the csv file into text blobs based on the above indices and convert it into partitioned dataFrame. 
The partitioned dataFrame will go through an UDF parser, which will parse each text blob and convert it into a structured format Hash(19 fields+ 1 field for shard routing region).
The partitioned DataFrames are brought back to driver executor where the “headline”, “text” fields are converted to English Language using Spark-NLP.
The resultant dataFrame is saved to Elasticsearch into ThomReuters/<csv-date-date> table
![image](https://user-images.githubusercontent.com/28540487/156979272-6e79f074-7302-4db6-add1-8e87fce7f0c5.png)

  

PostProcessed Output looks like:
  ![image](https://user-images.githubusercontent.com/28540487/156979599-2703d4d2-feac-4e96-a71c-ae1841ea6a50.png)
