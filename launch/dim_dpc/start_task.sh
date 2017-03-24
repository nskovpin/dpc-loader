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
PROJECT_NAME="DIM_DPC"
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
#QUEUE_NAME=prod
QUEUE_NAME=adhoc
#DIGITAL=digital.
DIGITAL=arstel.digital_
echo 'Start create tables...'

hive \
-hiveconf mapreduce.job.queuename=${QUEUE_NAME} \
-hivevar DIGITAL=${DIGITAL} \
-hivevar LOCATION=${OUTPUT_DIR}\product \
-f hive/create_dim_product.hql;

hive \
-hiveconf mapreduce.job.queuename=${QUEUE_NAME} \
-hivevar DIGITAL=${DIGITAL} \
-hivevar LOCATION=${OUTPUT_DIR}\marketingProduct \
-f hive/create_dim_marketing_product.hql;

hive \
-hiveconf mapreduce.job.queuename=${QUEUE_NAME} \
-hivevar DIGITAL=${DIGITAL} \
-hivevar LOCATION=${OUTPUT_DIR}\webEntity \
-f hive/create_dim_web_entity.hql;

hive \
-hiveconf mapreduce.job.queuename=${QUEUE_NAME} \
-hivevar DIGITAL=${DIGITAL} \
-hivevar LOCATION=${OUTPUT_DIR}\productRegionLink \
-f hive/create_dim_product_region_link.hql;

hive \
-hiveconf mapreduce.job.queuename=${QUEUE_NAME} \
-hivevar DIGITAL=${DIGITAL} \
-hivevar LOCATION=${OUTPUT_DIR}\region \
-f hive/create_dim_region.hql;

hive \
-hiveconf mapreduce.job.queuename=${QUEUE_NAME} \
-hivevar DIGITAL=${DIGITAL} \
-hivevar LOCATION=${OUTPUT_DIR}\externalRegionMapping \
-f hive/create_dim_external_region_mapping.hql;

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
-D DIGITAL=${DIGITAL}