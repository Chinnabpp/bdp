'''
Author: Hari Prasad
Purpose: Data into Datalake Using pyspark
Date: 11/14/2022
Version: 1.0
Input parameters Region: eng or test or prod
Input parameters as_of_date: 2022-11-14 format(yyyy-mm-dd)
batch_type (A)
use spark-submit <python_script_file> <region> <partition_date> <batch_type> <batch type>
'''

from pyspark import *
from pyspark.sql.types import DecimalType, IntegerType, StringType, DateType
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.context import SparkContext as sc
import sys,os,subprocess
from datetime import datetime,timedelta
from pyspark.sql import Row

def validation_args(partition_date):
    if len(partition_date) == 0 or partition_date == "None":
        exit("ERROR: as_of_date to pull from or 'none' passed, referring to default.")
    else:
        try:
            datetime.strptime(partition_date, '%Y-%m-%d')
        except ValueError:
            print("ERROR:-----------------------------------Exiting Code Due To Wrong Input Date----------------------------------")
            raise ValueError("ERROR:" + partition_date + " passed as pdate to pull from, but is malformed. Please input date in yyyy-mm-dd format.")

def capture_params(spark,f_path,batch_type,batch_file):
    f_data=spark.sparkContext.textFile(f_path).map(lambda x: x.split('|'))
    if batch_file!="None":
        conf_params=(f_data.filter(lambda x: x[0] == batch_file and x[5] == batch_type )).collect()
    else:
        conf_params=(f_data.filter(lambda x: x[5] == batch_type )).collect()
    return conf_params
    
    
def file_data(file):
    source_table=str(file[0])
    target_table=str(file[1])
    source_database=str(file[2])
    target_database=str(file[3])
    target_folder=str(file[4])
    batch_type=str(file[5])
    return source_table,target_table,source_database,target_database,target_folder,batch_type

def splunk_insert(partition_date,target_table,storage_path,spark):
    yest_date_query='''SELECT as_of_date as yest_ingest_dt FROM csdo_master.{}  where as_of_date < '{}' GROUP BY as_of_date order by as_of_date desc limit 1'''.format(target_table,partition_date)
    yest_date=spark.sql(yest_date_query)
    if yest_date.count() == 0:
        yest_ingest_dt='NULL'
        print("NOTE:-------------------Yesterday Ingestdate is Null--------------------------------------------")
    else:
        yest_ingest_dt=str((yest_date).first()[0])
    df=spark.sql('''SELECT a.as_of_date,a.src_db,a.src_tbl,a.src_count,a.src_add,a.src_edit,a.src_delete,a.dest_db,a.dest_tbl,a.dest_count,a.dest_add,a.dest_edit,a.dest_delete,a.dest_edit_add,a.dest_delete_add,a.dest_add_change,CAST(a.lst_updt_ts AS string),0 as yest_count,a.dest_count as today_total,'{}' as yest_ingest_dt, a.dest_count as today_count FROM csdo_master.metrics_transform_counts a where as_of_date='{}' and dest_tbl='{}' '''.format(yest_ingest_dt,partition_date,target_table))
    df.coalesce(1).write.mode("Overwrite").format("com.databricks.spark.csv").option("sep","|").option("header","false").save("{}".format(storage_path))
    print("Note:----splunk code ran successfully----")

def launch_spark(partition_date,prev_ingest_date):
    spark = SparkSession.builder.appName("CSDO360 Transformation Counts").config("spark.sql.parquet.writeLegacyFormat","true").config("hive.exec.dynamic.partition","true").config("hive.exec.dynamic.partition.mode","nonstrict").enableHiveSupport().getOrCreate()
    spark.sql('''set as_of_date="{}" '''.format(partition_date))
    spark.sql('''set prev_ingest_date="{}" '''.format(prev_ingest_date))

    return spark

#initializing spark session
if __name__ == "__main__":
    awsRegionLevel=str(sys.argv[1])
    partition_date=str(sys.argv[2])
    prev_ingest_date=str((datetime.strptime(partition_date, '%Y-%m-%d') - timedelta(days=1)))
    prev_ingest_date=prev_ingest_date[:10]
    batch_type=str(sys.argv[3])
    batch_file=str(sys.argv[4])
    spark=launch_spark(partition_date,prev_ingest_date)
    f_path="/home/hadoop/scripts/scripts/transactions/configs/transformation_metrics_mapping.txt"
    conf_params=capture_params(spark,f_path,batch_type,batch_file)
    for file in conf_params:
        source_table,target_table,source_database,target_database,target_folder,batch_type=file_data(file)
        storage_path="/tmp/splunk_data/as_of_date={}/{}".format(partition_date,batch_type)
        splunk_insert(partition_date,target_table,storage_path,spark)
