#!/usr/bin/env bash
#----------------------------------------------------------
# Set up environment variable OOZIE_URL
#----------------------------------------------------------
OOZIE_URL="http://hd-has015.vimpelcom.ru:11000/oozie"
NAME_NODE="hdfs://nameservice1"
export OOZIE_URL="${OOZIE_URL}"
#----------------------------------------------------------
# Set up Unix FS and HDFS path to config files
#----------------------------------------------------------
PROJECT_NAME="AGG_TAXI_SERVICE"
LINUX_PROJECT_PATH="${PWD}"
PROJECT_PATH=/user/$USER/${PROJECT_NAME}
WORKING_DIR=${PROJECT_PATH}"/work"
OUTPUT_DIR=${PROJECT_PATH}"/data"
#----------------------------------------------------------
# Clear current workflow directory from HDFS
# and upload updated workflow config files
#----------------------------------------------------------
echo 'Copying Oozie configs to HDFS...'
hadoop fs -rm -R ${WORKING_DIR}
hadoop fs -mkdir -p ${WORKING_DIR}
hadoop fs -mkdir -p ${WORKING_DIR}/spark
hadoop fs -put ${LINUX_PROJECT_PATH}/* ${WORKING_DIR}
#hadoop fs -put /usr/hdp/2.5.0.0-1245/spark/lib/spark-assembly*.jar ${WORKING_DIR}/spark/spark-assembly.jar
echo 'Done.'
#----------------------------------------------------------
# Create tables
#----------------------------------------------------------
QUEUE_NAME=prod
#QUEUE_NAME=adhoc
SPP=spp.
#SPP=arstel.aos_spp_
echo 'Start create tables...'

hive \
-hiveconf mapreduce.job.queuename=${QUEUE_NAME} \
-hivevar SPP=${SPP} \
-hivevar LOCATION=${OUTPUT_DIR} \
-f hive/create_agg_taxi_service.hql;

hive \
-hiveconf mapreduce.job.queuename=${QUEUE_NAME} \
-hivevar SPP=${SPP} \
-hivevar LOCATION=${OUTPUT_DIR}\STG_USER_ACTIVITY \
-f hive/create_stg_taxi_user_activity.hql;

echo 'Done.'
#----------------------------------------------------------
# Start Oozie job
#----------------------------------------------------------
echo 'Starting coordinator'
oozie job -run \
-auth KERBEROS \
-config job.properties  \
-D WORKING_DIR=${WORKING_DIR} \
-D PROJECT_NAME=${PROJECT_NAME} \
-D OUTPUT_DIR=${OUTPUT_DIR} \
-D QUEUE_NAME=${QUEUE_NAME} \
-D SPP=${SPP}