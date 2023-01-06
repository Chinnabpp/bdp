'''
Purpose: Script will perform Duplicate and NULL check on Data ingested into AWS Datalake.
Input parameters ENVM: eng or test or prod
Input parameters Ingestion_Date: format(yyyy-mm-dd)
domain ('transaction')
use spark-submit <python_script_file> <ENVM> <as_of_date> <domain>
'''

from datetime import datetime
from pyspark import *
from pyspark.sql.window import Window
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark import SparkContext
import pyspark.sql.functions as F
from pyspark.sql import Row
import sys,subprocess
import configparser

lst_updt_ts = (datetime.now()).strftime("%Y-%m-%d %H:%M:%S")
DQ_OVERRIDES_LIST = ['None']

# Validating arguments
def validate_date(d):
    print("NOTE: BEGIN : DQ CHECK %s" %(datetime.now()))
    try:
        return str(datetime.strptime(d, "%Y-%m-%d").date())
    except ValueError:
        msg = "Not a valid date: '{0}'.".format(d)
        raise argparse.ArgumentTypeError(msg)

# filter config file based on domain
def capture_params(f_path,spark,domain):
    print("--Capture_Params--")
    f_data =spark.sparkContext.textFile(f_path).map(lambda x: x.split('|'))
    #run for all tables in domain
    conf_params =(f_data.filter(lambda x: x[3] == domain)).collect()
    return conf_params

# Mapping config file to variables
def file_data(file):
    target_database='csdo_master'
    target_table=str(file[0])
    null_check_field=list(file[1].split(','))
    dedup_check_field=str(file[2])
    DQ_OVERRIDES_LIST=list(file[4].split(','))
    return target_database,target_table,null_check_field,dedup_check_field,DQ_OVERRIDES_LIST

# Calculate Null Counts flow 
# 1.full Nulls  query creation 
def construct_null_count_query(target_database,null_check_field,target_table,spark,partition_date):
    if ('ConditionalNulls' in DQ_OVERRIDES_LIST):
        query = ConditionalNulls(target_database,null_check_field,target_table,spark,partition_date)
    elif('ConditionalNullsNonLegacy' in DQ_OVERRIDES_LIST):
        query = ConditionalNullsNonLegacy(target_database,null_check_field,target_table,spark,partition_date)
    elif('ConditionalNullsNonVBO' in DQ_OVERRIDES_LIST):
        query = ConditionalNullsNonVBO(target_database,null_check_field,target_table,spark,partition_date)
    else:
        query = f'''SELECT COUNT(*) FROM {target_database}.{target_table} WHERE as_of_date="{partition_date}" AND '''
        for index in range(len(null_check_field)):
            print ("Current Key: ", null_check_field[index])
            if index == 0:
                query += f'''(({null_check_field[index]} IS NULL OR {null_check_field[index]} IN ("","null","NULL") OR length(trim({null_check_field[index]}))=0)'''
            else:
                query += f''' OR ({null_check_field[index]} IS NULL OR {null_check_field[index]} IN ("","null","NULL") OR length(trim({null_check_field[index]}))=0)'''

    query += ')'     
    print("Final Query for Null Counts:",query)
    return query

# 2.ConditionalNulls  query creation 
def ConditionalNulls(target_database,null_check_field,target_table,spark,partition_date):
    print("ConditionalNulls Override Enabled")
    query = f'''SELECT COUNT(*) FROM {target_database}.{target_table} WHERE as_of_date="{partition_date}" AND account_Type IN ('VBA') AND '''
    for index in range(len(null_check_field)):
        print ("Current Key: ", null_check_field[index])
        if index == 0:
            query += f'''(({null_check_field[index]} IS NULL OR {null_check_field[index]} IN ("","null","NULL") OR length(trim({null_check_field[index]}))=0)'''
        else:
            query += f''' OR ({null_check_field[index]} IS NULL OR {null_check_field[index]} IN ("","null","NULL") OR length(trim({null_check_field[index]}))=0)'''

    return query

# 3.ConditionalNulls  query creation 
def ConditionalNullsNonLegacy(target_database,null_check_field,target_table,spark,partition_date):
    print("ConditionalNulls for Non Legacy Override Enabled")
    query = f'''SELECT COUNT(*) FROM {target_database}.{target_table} WHERE as_of_date="{partition_date}" AND account_Type IN ('VBA', 'TA') AND '''
    for index in range(len(null_check_field)):
        print ("Current Key: ", null_check_field[index])
        if index == 0:
            query += f'''(({null_check_field[index]} IS NULL OR {null_check_field[index]} IN ("","null","NULL") OR length(trim({null_check_field[index]}))=0)'''
        else:
            query += f''' OR ({null_check_field[index]} IS NULL OR {null_check_field[index]} IN ("","null","NULL") OR length(trim({null_check_field[index]}))=0)'''

    return query    
    
# 4.ConditionalNulls  query creation 
def ConditionalNullsNonVBO(target_database,null_check_field,target_table,spark,partition_date):
    print("ConditionalNulls for Non Legacy Override Enabled")
    query = f'''SELECT COUNT(*) FROM {target_database}.{target_table} WHERE as_of_date="{partition_date}" AND account_Type NOT IN ('VBO') AND '''
    for index in range(len(null_check_field)):
        print ("Current Key: ", null_check_field[index])
        if index == 0:
            query += f'''(({null_check_field[index]} IS NULL OR {null_check_field[index]} IN ("","null","NULL") OR length(trim({null_check_field[index]}))=0)'''
        else:
            query += f''' OR ({null_check_field[index]} IS NULL OR {null_check_field[index]} IN ("","null","NULL") OR length(trim({null_check_field[index]}))=0)'''

    return query

# 5.full Nulls  query running
def calculate_null_counts(target_database,null_check_field,target_table,spark,partition_date):
    print ("-----Calculate null Counts---------")
    query = construct_null_count_query(target_database,null_check_field,target_table,spark,partition_date)
    null_count_df = spark.sql(query.strip())
    null_count = str(null_count_df.first()[0])
    print("--Debugging--")
    print(null_count_df)
    print(query)
    print(null_count)
    return null_count
    
# 6.calling Nulls function and inserting into metricsqlerror table
def run_null_check(target_database,null_check_field,target_table,spark,partition_date):
    print("---Running Null Check---")
    if (null_check_field[0] != "None"):
        null_count_df = calculate_null_counts(target_database,null_check_field,target_table,spark,partition_date)
        print("--Debugging--")
        print(null_count_df)
        query = "SELECT 'csdo_master' as source_type,'NULL_VALUES' as error_type,'" + null_count_df + "' as null_count,'" + ','.join([str(elem) for elem in null_check_field]) + "' as error_detail,cast('" + lst_updt_ts +"' as timestamp) as lst_updt_ts, '" + partition_date +"' AS as_of_date, '" + target_table + "' as tbl_nm"
        print (query)
        data = spark.sql(query)
        data.show()
        data.write.insertInto("csdo_master.metrics_sql_errors",overwrite =False)
    print("---completed Null Check---")

# Calculates duplicates
def calculateDedupcheck(target_database,dup_check_field,target_table,spark,partition_date):
    print("NOTE: Dedup Checks START : %s" %(datetime.now()))
    if ('ConditionalDupNonLegacy' in DQ_OVERRIDES_LIST):
        print("NOTE: Legacy Brokerage Dedup Checks START : %s" %(datetime.now()))
        table_df = spark.sql(f"select * from {target_database}.{target_table} where as_of_date='{partition_date}' AND account_Type IN ('VBA','TA') ")
    else:
        table_df = spark.sql(f"select * from {target_database}.{target_table} where as_of_date='{partition_date}'")
    dup_cnt = table_df.groupby(dup_check_field.split(",")).count().where(F.col("count")>1)
    dup_cnt.registerTempTable("temp")
    DUP_STRING="DUP_ERROR:" + dup_check_field
    query=f"select '{target_database}' as source_type,'{DUP_STRING}' as error_type,count as null_count,(concat_ws(',',{dup_check_field})) as error_detail,cast('{lst_updt_ts}' as timestamp) as lst_updt_ts,'{partition_date}' AS as_of_date,'{target_table}' as tbl_nm  from temp"
    print(query)
    final_df=spark.sql(query)
    final_df.write.insertInto("csdo_master.metrics_sql_errors",overwrite =False)
    print("NOTE: Dedup Checks END : %s" %(datetime.now()))

# Constructing S3 file path
def construct_file_path(ENVM,partition_date,table_name):
    path="s3://vgi-platform-{}-us-east-1-csdo-client360/csdo_master/metrics/sql_errors/as_of_date={}/tbl_nm={}".format(ENVM,partition_date,table_name)
    print("Note:Output file path set to: %s" %path)
    return path

# Cleaning up S3 path if it already exists
#Drop dq data if exist
def clean_hadoop_directory(path):
    if subprocess.call(["hadoop", "fs", "-ls", path]) == 0:
        subprocess.call(["hadoop", "fs", "-rm", "-r", path])
        print("NOTE:-------------Hdfs Directory Are Deleted for "  + path +  " ---------")

# Setting up spark configuration.
def launch_spark():
    spark=(SparkSession
           .builder
           .enableHiveSupport()
           .appName("DQ Check For OGC Table")
           .config("set spark.sql.caseSensitive", "false")
           .config("set hive.exec.compress.output", "true")
           .config("spark.sql.legacy.parquet.datetimeRebaseModeInRead", "Legacy")
           .getOrCreate())
    spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    return spark

def get_property(section,propertyName):
    config = configparser.RawConfigParser()
    config.read(propertiesFile)
    try:
        return config.get(section,propertyName)
    except Exception as e:
        return ""

if __name__ == "__main__":
    global domain,propertiesFile
    dmn=sys.argv[1]
    propertiesFile="/home/hadoop/scripts/scripts/transactions/configs/dq_check.properties"    
    f_path="/home/hadoop/scripts/scripts/transactions/configs/dq_mapping.txt"
    ENVM = get_property('dq_check','ENVM')
    partition_date = get_property('dq_check','partition_date')
    domain = get_property('dq_check','domain')    
    spark=launch_spark()
    conf_params=capture_params(f_path,spark,domain)
    print(conf_params)
    counter=1
    for file in conf_params:
        target_database,target_table,null_check_field,dedup_check_field,DQ_OVERRIDES_LIST=file_data(file)
        if counter==1:
            clean_hadoop_directory(construct_file_path(ENVM,partition_date,target_table))
            counter=counter+1
        run_null_check(target_database,null_check_field,target_table,spark,partition_date)
        if dedup_check_field != 'None':
            calculateDedupcheck(target_database,dedup_check_field,target_table,spark,partition_date)
        
