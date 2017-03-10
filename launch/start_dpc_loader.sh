#!/usr/bin/env bash

#STAGE=$1
#shift
#TIME_KEY=$1
#shift
#FCT_RTC_PATH=$1
#shift
#AGG_SMS_PROOF_PATH=$1
#shift
#AGG_SMS_ROUTER_PATH=$1
#shift
#AGG_SMS_CENTER_PATH=$1
#shift
#CREDIT_PATTERNS_PATH=$1
#shift
#DEBIT_PATTERNS_PATH=$1
#BANK_NAMES_PATTERNS_PATH=$2
#MONTHS_BACK=$3
#WORK_DIR=$4
#DATA_DIR=$5
#OUTPUT_DIR=$6
#JAR_PATH=$7
#QUEUE_NAME=$8

PROJECT_NAME="dpc-loader"
HDFS_JSON_PATH="/user/$USER/json/second"
HDFS_OUTPUT_DIR="/user/$USER/json/parsed"
STAGE=1
QUEUE_NAME=adhoc
JAR_PATH="dpc-loader-shadowed-1.0-SNAPSHOT.jar"

echo "We are here!"
echo stage ${STAGE}
echo PROJECT_NAME ${PROJECT_NAME}
echo HDFS_JSON_PATH ${HDFS_JSON_PATH}
echo HDFS_OUTPUT_DIR ${HDFS_OUTPUT_DIR}
echo jar ${JAR_PATH}

case ${STAGE} in
    1)
        spark-submit \
        --class ru.at_consulting.bigdata.dpc.cluster.DimGroups \
        --master yarn-cluster \
        --num-executors 100 \
        --executor-cores 5 \
        --driver-memory 3G \
        --executor-memory 3G \
		--driver-java-options -Dhdp.version=2.2.8.0-3150 \
        --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
        --conf spark.kryoserializer.buffer.max=128 \
        --conf spark.yarn.executor.memoryOverhead=2048 \
        --conf spark.default.parallelism=50 \
        --conf spark.shuffle.consolidateFiles=true \
        --conf spark.shuffle.io.maxRetries=5 \
        --conf spark.akka.frameSize=500 \
        --conf spark.task.maxFailures=10 \
        --conf spark.storage.memoryFraction=0.1 \
        --conf spark.rpc.askTimeout=360 \
        --conf spark.app.name=agg_bank_loans \
		--conf spark.driver.extraJavaOptions=-Dhdp.version=2.2.8.0-3150 \
		--conf spark.yarn.am.extraJavaOptions=-Dhdp.version=2.2.8.0-3150 \
        --queue ${QUEUE_NAME} \
        ${JAR_PATH} \
        PROJECT_NAME=${PROJECT_NAME} \
        HDFS_JSON_PATH=${HDFS_JSON_PATH} \
        HDFS_OUTPUT_DIR=${HDFS_OUTPUT_DIR} \
        ;;
    *)
    echo 'wrong stage number!'
    ;;
esac