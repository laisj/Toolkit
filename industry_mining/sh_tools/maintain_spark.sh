#kill all worker's jps
ssh 001
cd /home/adengine
pssh -P -h slaves 'ps -ef | grep [sS]park|grep -v grep|cut -c 9-15 |xargs kill -9'
/opt/spark-1.6.0/sbin/start-slaves.sh

#submit

/opt/spark-1.5.1-bin-2.6.0/bin/spark-submit train_on_spark.py  --master yarn --deploy-mode cluster --num-executors 10 --queue adengine
/opt/spark-1.5.1-bin-2.6.0/bin/spark-shell --master yarn --num-executors 10 --queue adengine

/opt/spark-1.5.1-bin-2.6.0/bin/spark-submit --master yarn --num-executors 100 --queue adengine --deploy-mode cluster --class feature_extraction.filter_ext_feature spark_scala.jar 2016-06-21 2016-06-23

#deploy

#! /bin/bash
sudo cp -r  /home/adengine/hadoop-2.6.0 /opt
sudo cp -r  /home/adengine/jdk1.7.0_71 /opt
sudo cp -r  /home/adengine/spark-1.6.0 /opt
ln -sf /opt/hadoop-2.6.0 /opt/hadoop
chown -R adengine:adengine /opt/hadoop-2.6.0
chown -R adengine:adengine /opt/spark-1.6.0
sudo mkdir -p /home/data/spark
chown -R adengine:adengine /home/data/spark
rm -rf /home/adengine/hadoop-2.6.0 /home/adengine/jdk1.7.0_71 /home/adengine/spark-1.6.0
