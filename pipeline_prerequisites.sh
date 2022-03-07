#!/bin/bash
wget https://s3.amazonaws.com/auxdata.johnsnowlabs.com/public/jars/spark-nlp-assembly-3.4.1.jar
wget https://repo1.maven.org/maven2/org/elasticsearch/elasticsearch-hadoop/8.0.0/elasticsearch-hadoop-8.0.0.jar
sudo mv spark-nlp-assembly-3.4.1.jar /usrlib/spark/jars/.
sudo mv elasticsearch-hadoop-8.0.0.jar /usrlib/spark/jars/.
pip install 'apache-airflow[amazon]'
curl https://www.sqlite.org/2020/sqlite-autoconf-3320300.tar.gz | tar xzf -
cd ./sqlite-autoconf-3320300 && ./configure
make
sudo make install
cd .. && rm -r ./sqlite-autoconf-3320300
