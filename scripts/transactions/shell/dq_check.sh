#!/bin/bash
#############################################################################################################################
###Date         Version     Owner                   Comments
###20221115      1.0         Hari Prasad Mangali       test
###This script gets the null counts for key fields and sends to splunk.
###Test Example:
### /home/hadoop/scripts/scripts/transactions/shell/dq_check.sh test 20222115 transaction
###
###  Inputs:        1) ENVNM: region in aws - eng, test, or prod
###                 2) as_of_date: order date
###                 3) domain:should be in (transaction) (lower case)
#############################################################################################################################
if [ ! -z "${1}" ]; then
  ENVM="${1}"
fi
if [ ! -z "${2}" ]; then
  as_of_date=$(date -d ${2} +"%Y-%m-%d")
fi
if [ ! -z "${3}" ]; then
  domain="${3}"
fi
file_name="${4}"
client360_state="${5}"

TMP="/tmp/"
OUTFILE="${domain}_dq_outfile_$(date +'%Y-%m-%d').txt"
file="${domain}_dq_check"
echo $domain
temp_log_file_logging="${domain}_dq_check.log"
splunk_insert_logging="INFO|99|${domain^^}"
spark_log_conf="/home/hadoop/scripts/scripts/transactions/configs/spark_logs.properties"
SPLNKLOG="/var/log/hadoop/steps/dq_fileLevel_errors.log"
err="ERROR|"
warn="WARN|"
note="NOTE|"
exception="Exception|"
v1="transaction"
v2="account"
TOS3="/tmp/${domain}toS3"
propertiesFile="/home/hadoop/scripts/scripts/transactions/configs/dq_check.properties"

echo "*************************** BEGIN : $(date +'%Y-%m-%d %H:%M:%S,%2N') ***************************"
###from cmd line get Enviornment Region:test,eng,prod
as_of_date="${as_of_date}"
echo $as_of_date
ENVNM="${ENVM}"
echo $ENVNM

if [ "${client360_state}" == "history" ]; then
   spark_params="-r ${ENVM} -d ${as_of_date} -dm ${domain} -f ${file_name}"
else
   spark_params="-r ${ENVM} -d ${as_of_date} -dm ${domain} -f None"
fi

if [ -f "$propertiesFile" ] ; then
    rm "$propertiesFile" 
fi

echo "[dq_check]" >> $propertiesFile
echo "ENVM=${ENVM}" >> $propertiesFile
echo "partition_date=${as_of_date}" >> $propertiesFile
echo "domain=${domain}" >> $propertiesFile
echo "file_name=${file_name}" >> $propertiesFile

# Calls the pyspark script and calculates the null counts for key fields
spark-submit ${spark_conf} /home/hadoop/scripts/python/transactions/dq_check.py ${domain} > ${TMP}${temp_log_file_logging} 2>&1
if [ $? -ne 0 ]
then
    echo "Failure in null_count_dedup.py"
    exit 1
fi

if [ "$3" == "${v1}" ]; then
	SPLNK_INSERT="INFO|2000|"
	hive -e "select as_of_date,source_type,tbl_nm,error_type,null_count,error_detail,last_updt_ts from csdo_master.metrics_sql_errors where as_of_date='${as_of_date}' and tbl_nm IN ('ttransactions_by_account');" >> ${TMP}${OUTFILE}
elif [ "$3" == "${v2}" ]; then
	SPLNK_INSERT="INFO|2003|"
	hive -e "select as_of_date,source_type,tbl_nm,error_type,null_count,error_detail,last_updt_ts from csdo_master.metrics_sql_errors where as_of_date='${as_of_date}' and tbl_nm IN ('tbalances_by_account');" >> ${TMP}${OUTFILE}
else
	echo "--------------------Could not find the domain-----------------------"
fi

if [ $? -ne 0 ]; then

S3_LOG_OUTFILE_PATH="s3://vgi-platform-${ENVM}-${AWS_DEFAULT_REGION}-csdo-client360/csdo_master/metrics/sql_errors_s3copy/logs/as_of_date=${as_of_date}"
hdfs dfs -mkdir -p ${S3_LOG_OUTFILE_PATH}
hdfs dfs -put -f ${TMP}${temp_log_file_logging} ${S3_LOG_OUTFILE_PATH}

egrep "Error" ${TMP}${temp_log_file_logging} | sed "s/^/${as_of_date}|${splunk_insert_logging}|${file}|${err}/g" >> ${SPLNKLOG}
egrep "WARN" ${TMP}${temp_log_file_logging} | sed "s/^/${as_of_date}|${splunk_insert_logging}|${file}|${warn}/g" >> ${SPLNKLOG}
egrep "NOTE" ${TMP}${temp_log_file_logging} | sed "s/^/${as_of_date}|${splunk_insert_logging}|${file}|${note}/g" >> ${SPLNKLOG}
egrep "Exception" ${TMP}${temp_log_file_logging} | sed "s/^/${as_of_date}|${splunk_insert_logging}|${file}|${exception}/g" >> ${SPLNKLOG}
exit 1
fi

sed -i 's/\t/|/g' ${TMP}${OUTFILE}
if [ ${as_of_date} == 'NULL' ];then
     echo "as_of_date is null"
else
###push to s3:
S3_OUTFILE_PATH="s3://vgi-platform-${ENVM}-${AWS_DEFAULT_REGION}-csdo-client360/csdo_master/metrics/sql_errors_s3copy/as_of_date=${as_of_date}"
S3_LOG_OUTFILE_PATH="s3://vgi-platform-${ENVM}-${AWS_DEFAULT_REGION}-csdo-client360/csdo_master/metrics/sql_errors_s3copy/logs/as_of_date=${as_of_date}"
  hdfs dfs -mkdir -p ${S3_LOG_OUTFILE_PATH}
  hdfs dfs -mkdir -p ${S3_OUTFILE_PATH}
  hdfs dfs -put -f ${TMP}${temp_log_file_logging} ${S3_LOG_OUTFILE_PATH}
  mkdir -p ${TOS3}
  aws s3 cp ${S3_OUTFILE_PATH}/${OUTFILE} ${TOS3}
  cat ${TMP}${OUTFILE} >> ${TOS3}/${OUTFILE}
  hdfs dfs -put -f ${TOS3}/${OUTFILE} ${S3_OUTFILE_PATH}
  hdfs dfs -ls s3://vgi-platform-${ENVM}-${AWS_DEFAULT_REGION}-csdo-client360/csdo_master/metrics/sql_errors_s3copy/as_of_date=${as_of_date}
  egrep "Error" ${TMP}${temp_log_file_logging} | sed "s/^/${as_of_date}|${splunk_insert_logging}|${file}|${err}/g" >> ${SPLNKLOG}
  egrep "WARN" ${TMP}${temp_log_file_logging} | sed "s/^/${as_of_date}|${splunk_insert_logging}|${file}|${warn}/g" >> ${SPLNKLOG}
  egrep "NOTE" ${tmp}${temp_log_file_logging} | sed "s/^/${as_of_date}|${splunk_insert_logging}|${file}|${note}/g" >> ${SPLNKLOG}
  egrep "Exception" ${TMP}${temp_log_file_logging} | sed "s/^/${as_of_date}|${splunk_insert_logging}|${file}|${exception}/g" >> ${SPLNKLOG}

fi

###prep for splunk:
while read -r row; do
  echo ${row}
  echo ${row} | sed  "s/${as_of_date}|/${as_of_date}|${SPLNK_INSERT}/g" >> ${SPLNKLOG}
done < ${TMP}${OUTFILE}

if [ -f  ${TMP}${OUTFILE} ]; then
  rm ${TMP}${OUTFILE}
fi
echo "*************************** END : $(date +'%Y-%m-%d %H:%M:%S,%2N') ***************************"
