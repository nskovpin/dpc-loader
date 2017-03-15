#!/usr/bin/env bash date
date=$1
shift
output=$1
shift
SPP=$1
shift
QUEUE=adhoc

pathSTG="${output}/STG_USER_ACTIVITY/"
pathAGG="${output}/"

startdate=$(date -I -d "$date") || exit -1
enddate=$(date -I -d "$date + 7 days") || exit -1

d="$startdate"
while [ $(date -d "$d" +%Y%m%d) -lt $(date -d "$enddate" +%Y%m%d) ]; do
	echo $d
	
	formatted=$(date -d "$d" +%Y/%m%d)
	pathS="$pathSTG$formatted"
	pathA="$pathAGG$formatted.tsv"
	echo $pathA
	echo $pathS
	#if [ hadoop fs -test -d $pathS ]; then
	#	echo "Exists"
		hive \
		-hiveconf mapreduce.job.queuename=${QUEUE_NAME} \
		-hivevar SPP=${SPP} \
		-hivevar LOCATION=${pathS} \
		-f hive/alter_create_agg_taxi_service.hql;
	#else
		hive \
		-hiveconf mapreduce.job.queuename=${QUEUE_NAME} \
		-hivevar SPP=${SPP} \
		-hivevar LOCATION=${pathA} \
		-f hive/alter_create_stg_taxi_user_activity.hql;
	#fi
	d=$(date -I -d "$d + 1 day")
done
