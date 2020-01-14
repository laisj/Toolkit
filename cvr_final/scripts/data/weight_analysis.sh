#!/bin/bash

master="yarn-cluster"
num_executor=800
queue="root.production.cloud_group.feeds"
queue="root.service.cloud_group.data_platform.online"

class="com.xiaomi.contest.cvr.analysis.WeightAnalysis"

feature_path=/user/h_user_profile/lizhixu/contest/cvr1/lr_v1/samples/stats/fea_hash
model_path=/user/h_user_profile/lizhixu/recommend/training/ctr_test_output/model/

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
    --num-executors "$num_executor" \
    --driver-memory 8g \
    --executor-memory 8g \
    ./target/contest-pipeline-cvr-1.0-SNAPSHOT.jar ${feature_path} ${model_path}
