#!/bin/bash

##############################################################################################
##       Purpose: This script calls pyspark script to run insert queries for Service Agreement 
####Author:                 Date:           Version:
###UFTY,UGJ6                 2019-08-30           1.0
####Inputs:         1) ENVM(lowercase: eng,test,prod)
#                   2) as_of_date ("YYYY-MM-DD")
#                   3) domain("account,transaction")
#                   4) optional--file_name (individual file name: Value or None)
#                   5) optional--client360_state (daily ingestion or history recovery: history or no value for daily)
##############################################################################################
export ENVM=$1
export as_of_date=$2
export domain=$3
export file_name=$4
export client360_state=$5
if [ -z $file_name ]; then

   file_name="None"
fi
if [ -z $client360_state ]; then
   client360_state="current"
fi
tmp="/tmp/"
err="ERROR|"
warn="WARN|"
note="NOTE|"
exception="Exception|"
SPLNKLOG="/var/log/hadoop/steps/dq_fileLevel_errors.log"
propertiesFile="/home/hadoop/scripts/scripts/transactions/configs/data_ingestion.properties"
hql_path="/home/hadoop/scripts/scripts/transactions/hive/Insert"
f_path="/home/hadoop/scripts/scripts/transactions/configs/insert_mapping.txt"
spark_log_conf="file:/home/hadoop/scripts/scripts/transactions/configs/spark_logs.properties"
spark_conf="--master yarn --deploy-mode client --conf spark.sql.catalogImplementation=hive --conf spark.sql.parquet.writeLegacyFormat=true --conf spark.driver.extraJavaOptions=-Dlog4j.configuration=${spark_log_conf} --conf spark.executor.extraJavaOptions=-Dlog4j.configuration=${spark_log_conf}"
temp_log_file="${domain}_data_ingestion.log"
splunk_insert="INFO|99|${domain}"
file="${domain}_data_ingestion"

if [ "${client360_state}" == "history" ]; then
    hql_path="/home/hadoop/scripts/scripts/transactions/hive/History_Insert"
fi

sed -i "s~\(ENVM=\).*~\1${ENVM}~g" $propertiesFile
sed -i "s~\(partition_date=\).*~\1${as_of_date}~g" $propertiesFile
sed -i "s~\(domain=\).*~\1${domain}~g" $propertiesFile
sed -i "s~\(file_name=\).*~\1${file_name}~g" $propertiesFile
sed -i "s~\(client360_state=\).*~\1${client360_state}~g" $propertiesFile
sed -i "s~\(hql_path=\).*~\1${hql_path}~g" $propertiesFile
sed -i "s~\(f_path=\).*~\1${f_path}~g" $propertiesFile
spark-submit ${spark_conf} /home/hadoop/scripts/python/transactions/data_ingestion.py > ${tmp}${temp_log_file} 2>&1

if [ $? -ne 0 ]; then

S3_OUTFILE_PATH="s3://vgi-platform-${ENVM}-${AWS_DEFAULT_REGION}-csdo-client360/csdo_master/metrics/transform_counts_s3copy/logs/as_of_date=${as_of_date}"
hdfs dfs -mkdir -p ${S3_OUTFILE_PATH}
hdfs dfs -put -f ${tmp}${temp_log_file} ${S3_OUTFILE_PATH}
egrep "Error" ${tmp}${temp_log_file} | sed "s/^/${as_of_date}|${splunk_insert}|${file}|${err}/g" >> ${SPLNKLOG}
egrep "WARN" ${tmp}${temp_log_file} | sed "s/^/${as_of_date}|${splunk_insert}|${file}|${warn}/g" >> ${SPLNKLOG}
egrep "NOTE" ${tmp}${temp_log_file} | sed "s/^/${as_of_date}|${splunk_insert}|${file}|${note}/g" >> ${SPLNKLOG}
egrep "Exception" ${tmp}${temp_log_file} | sed "s/^/${as_of_date}|${splunk_insert}|${file}|${exception}/g" >> ${SPLNKLOG}
exit 100
else
S3_OUTFILE_PATH="s3://vgi-platform-${ENVM}-${AWS_DEFAULT_REGION}-csdo-client360/csdo_master/metrics/transform_counts_s3copy/logs/as_of_date=${as_of_date}"
hdfs dfs -mkdir -p ${S3_OUTFILE_PATH}
hdfs dfs -put -f ${tmp}${temp_log_file} ${S3_OUTFILE_PATH}
egrep "Error" ${tmp}${temp_log_file} | sed "s/^/${as_of_date}|${splunk_insert}|${file}|${err}/g" >> ${SPLNKLOG}
egrep "WARN" ${tmp}${temp_log_file} | sed "s/^/${as_of_date}|${splunk_insert}|${file}|${warn}/g" >> ${SPLNKLOG}
egrep "NOTE" ${tmp}${temp_log_file} | sed "s/^/${as_of_date}|${splunk_insert}|${file}|${note}/g" >> ${SPLNKLOG}
egrep "Exception" ${tmp}${temp_log_file} | sed "s/^/${as_of_date}|${splunk_insert}|${file}|${exception}/g" >> ${SPLNKLOG}
fi
