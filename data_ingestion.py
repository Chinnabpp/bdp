'''
Author: Hari Prasad
Purpose: ingest transactions and account Data into Datalake Using pyspark
Input parameters ENVM: eng or test or prod
Input parameters as_of_date: 2022-11-14 format(yyyy-mm-dd)
domain (transaction)
use spark-submit <python_script_file> <ENVM> <ingestion_date> <domain> <optional--standalone table>
'''

from pyspark import *
from pyspark.sql.types import DecimalType, IntegerType, StringType, DateType
from pyspark.sql.functions import *
from pyspark import SQLContext
from pyspark.sql import SparkSession
from pyspark.context import SparkContext as sc
import sys,subprocess
from datetime import datetime,timedelta
import configparser

# Validating as_of_date argument.
def validate_date(d):
    try:
        return str(datetime.strptime(d, "%Y-%m-%d").date())
    except ValueError:
        msg = "Not a valid date: '{0}'.".format(d)
        raise argparse.ArgumentTypeError(msg)

#Capture config file data witout header and matched data with domain name
def capture_params(spark,f_path,file_name,domain):
    f_data =spark.sparkContext.textFile(f_path).map(lambda x: x.split('|'))
    #check if file_name is not None and want to load data for specific table
    if  file_name!="None":
        conf_params =(f_data.filter(lambda x: x[1].lower() == file_name and x[5] == domain)).collect()
    #run for all tables in domain
    else:
        conf_params =(f_data.filter(lambda x: x[5] == domain)).collect()
    if len(conf_params)==0:
        exit('ERROR: Invalid Domain name or table name passed.')
    print("NOTE: STARTING DATA INGESTION AND METRIC COUNTS FOR {} TABLES: {}".format(len(conf_params),datetime.now()))
    return conf_params

#Assign variable for each list of data with | delimited
def file_data(file):
    source_table=str(file[0])
    target_table=str(file[1])
    target_folder=str(file[2])
    source_database=str(file[3])
    target_database=str(file[4])
    return source_table,target_table,target_folder,source_database,target_database

#Assign file S3 path for ingestion as well as metric
def file_path(target_database,target_folder,target_table,ENVM,partition_date):
    ingestion_s3_path='''s3://vgi-platform-{}-us-east-1-csdo-client360/{}/{}/{}/as_of_date={}'''.format(ENVM,target_database,target_folder,target_table,partition_date)
    metric_s3_path='''s3://vgi-platform-{}-us-east-1-csdo-client360/{}/metrics/transform_counts/as_of_date={}/dest_tbl={}'''.format(ENVM,target_database,partition_date,target_table)
    print("NOTE:Output file path set to: %s" %ingestion_s3_path)
    print("NOTE:Output file path set to: %s" %metric_s3_path)
    return ingestion_s3_path,metric_s3_path


#Drop ingestion Data and Metric data if exist
def drop_ingestion_data(*params):
    for path in params:
        if subprocess.call(["hadoop", "fs", "-ls", path]) == 0:
            subprocess.call(["hadoop", "fs", "-rm", "-r", path])

# insert data into OGC_Dna_Legalcompliance Datalake
def ingestion(ingestion_s3_path, target_table, spark,partition_date,hql_path,client360_state):
    source_count = None
    as_of_date=str(datetime.strptime(partition_date,"%Y-%m-%d"))
    spark.sql('''ALTER TABLE csdo_master.{} DROP IF EXISTS PARTITION(as_of_date="{}")'''.format(target_table,partition_date))
    hive_hql = "{}/{}.hql".format(hql_path, target_table.lower())
    final_df = spark.sql(open(hive_hql).read())
    final_df.persist(storageLevel=StorageLevel.MEMORY_AND_DISK)
    source_count = str(final_df.count())
    final_df.repartition(25).write.mode("overwrite").parquet(ingestion_s3_path)
    final_df.unpersist()
    print("NOTE:---------------------------------Ingest data successfully for  " + target_table + "--------------------------------------------------------")
    print("NOTE:Source_Count for " + target_table + " is:" + str(source_count) + "----")
    return source_count
    
# Update partition with respected table
def add_partition(spark, target_table,partition_date):
    """
    Sync s3 and hive metastore
    """
    try:
        print("NOTE:Adding s3 partitions to hive metastore for all tables.")
        spark.sql("ALTER TABLE csdo_master.{} ADD PARTITION(as_of_date='{}')".format(target_table,partition_date))
    except Exception as err:
        print("Error: {}".format(err))
        exit("ERROR:Failed in add_partition.")

# Destination count for respected table
def calculate_dest_counts(target_database, target_table, spark,partition_date):
    next_ingest_date = str((datetime.strptime(partition_date, '%Y-%m-%d').date() + timedelta(days=1)).strftime('%Y-%m-%d'))
    query = '''SELECT COUNT(*) FROM {0}.{1} WHERE as_of_date='{2}' '''.format(target_database, target_table, partition_date)
    print(query)
    dest_cnt = str((spark.sql(query)).first()[0])
    print("NOTE:Destination_Count for " + target_table + " is:" + dest_cnt + "----")
    return dest_cnt


# Insert Count Values into metric_count table
def metric_count(source_database, source_table, target_table, spark, src_cnt, dest_cnt,partition_date):
    error_msg = ' '
    lst_updt_ts = str((datetime.now()).strftime("%Y-%m-%d %H:%M:%S"))
    metric_query = '''SELECT '{}' as src_db,'{}' as src_tbl,{} as src_cnt,{} as src_add,{} as src_edit,{} as src_del,'csdo_master' as dest_db,{} as dest_count,{} as dest_add,{} as dest_edit,{} as dest_delete,{} as dest_edit_add,{} as dest_delete_add,{} as dest_add_change,cast('{}' as timestamp) as lst_updt_ts,'{}' as error_msg,'{}' as as_of_date,'{}' as dest_tbl'''.format(source_database, source_table, src_cnt, '0', '0', '0',  dest_cnt, dest_cnt, '0', '0', '0', '0','0', lst_updt_ts,error_msg, partition_date, target_table)
    print(metric_query)
    insert_metric_df = spark.sql(metric_query)
    insert_metric_df.write.insertInto("csdo_master.metrics_transform_counts", overwrite=True)
    print("NOTE: DATA INGESTION AND METRIC COUNTS END : %s" % (datetime.now()))


# initializing spark session
def launch_spark(partition_date):
    spark = (SparkSession.builder.appName("Data Ingestion And Metric Count Script for client360").config("spark.sql.parquet.writeLegacyFormat", "true")
        .config("hive.exec.dynamic.partition", "true").config("hive.exec.dynamic.partition.mode", "nonstrict")
        .config("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "Legacy").config("spark.sql.legacy.parquet.datetimeRebaseModeInRead", "Legacy")
        .config("spark.sql.autoBroadcastJoinThreshold","-1").enableHiveSupport().getOrCreate())
    spark.sql('''set as_of_date="{}" '''.format(partition_date))
    next_ingest_date=str((datetime.strptime(partition_date, '%Y-%m-%d') + timedelta(days=1,hours=9)))
    spark.sql('''set next_ingest_date="{}" '''.format(next_ingest_date))
    return spark

def get_property(section,prepertyName):
    config = configparser.RawConfigParser()
    config.read('/home/hadoop/scripts/scripts/transactions/configs/data_ingestion.properties')
    try:
        return config.get(section,prepertyName)
    except Exception as e:
        return ""
        
if __name__ == "__main__":
    global ENVM,partition_date,domain,hql_path,f_path,client360_state,file_name
    #captruing arguments
    ENVM = get_property('data_ingestion','ENVM')
    partition_date= get_property('data_ingestion','partition_date')
    domain = get_property('data_ingestion','domain')
    file_name=get_property('data_ingestion','file_name')
    client360_state=get_property('data_ingestion','client360_state')
    hql_path= get_property('data_ingestion','hql_path')
    f_path = get_property('data_ingestion','f_path')
    spark = launch_spark(partition_date)
    print(ENVM,partition_date,domain,hql_path,f_path,client360_state,file_name)
    conf_params = capture_params(spark,f_path,file_name,domain)
    for file in conf_params:
        source_table, target_table, target_folder, source_database, target_database = file_data(file)
        ingestion_s3_path, metric_s3_path = file_path(target_database, target_folder, target_table,ENVM,partition_date)
        drop_ingestion_data(ingestion_s3_path, metric_s3_path)
        src_cnt = ingestion(ingestion_s3_path, target_table, spark,partition_date,hql_path,client360_state)
        add_partition(spark, target_table,partition_date)
        dest_cnt = calculate_dest_counts(target_database, target_table, spark,partition_date)
        metric_count(source_database, source_table, target_table, spark, src_cnt, dest_cnt,partition_date)
