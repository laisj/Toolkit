#!/bin/bash

master="yarn-cluster"
num_executor=2400
queue="root.service.cloud_group.data_platform.online"
queue="root.production.cloud_group.feeds"

class="com.xiaomi.contest.cvr.test.FeatureProcess"

start_day=`date -d '-1 day' +%Y-%m-%d`

start_day="2017-08-28"
days=1

${INFRA_CLIENT_HOME}/bin/spark-submit \
    --cluster c3prc-hadoop-spark2.1 \
    --force-update \
    --class "$class" \
    --master "$master" \
    --queue "$queue" \
    --conf spark.yarn.job.owners=lizhixu \
    --conf spark.yarn.alert.phone.number=18511878129 \
    --conf spark.yarn.executor.memoryOverhead=1024 \
    --conf spark.speculation=true \
    --conf spark.executor.extraJavaOptions="-XX:MaxDirectMemorySize=1024m" \
    --num-executors "$num_executor" \
    --driver-memory 8g \
    --executor-memory 8g \
    ./target/contest-pipeline-cvr-1.0-SNAPSHOT.jar ${start_day} ${days}
