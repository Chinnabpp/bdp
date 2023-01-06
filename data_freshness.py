'''
Author: Hari Prasad
Purpose: This script is used to check data freshness for source tables Using pyspark
Input parameters ENVM: eng or test or prod
Input parameters as_of_date: 2022-11-14 format(yyyy-mm-dd)
domain (transaction)
use spark-submit <python_script_file> <ENVM> <as_of_date> <domain> <optional--standalone table>
'''
from notificationclient.notification_client import executeNotificationClient
from datetime import date,datetime
from dateutil.parser import parse
from collections import namedtuple
from pytz import timezone
import sys,argparse,json,time
from pyspark import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
import datetime as dt
import configparser

Table_Info = namedtuple("Table_Info", "defaults, message, domain")

Data_Freshness_Cut_Off_Time = dt.time(11, 59, 0)

def parse_params(domain, f_path):
    message = []
    with open(f_path, "r") as data_file:
        data = json.load(data_file)
        f_data = data.get(domain)
    for database,tables in f_data.items():
        for table_nm in tables:
            data_dict = data.get(database)
            data_dict['table']=table_nm
            message.append(data_dict.copy())
    tb = Table_Info(data.get('defaults'),str(message),domain)
    return tb

def get_table_status(func):
    def wrapper(tb, **kwargs):
        print("\nStart polling:\n")
        try:
            res=executeNotificationClient(**tb.defaults,message=tb.message)
            data=res['body']
            print("\nEnd polling:\n")
            return func(tb, data)
        except TypeError:
            print("Printing type error capture")
    return wrapper

def format_json(func):
    def wrapper1(tb, data):
        suc_list=[]
        error_list=[]
        for table,status in data.items():
            if(status['lastSuccess']!='NA'):
                lastSuccess = parse(status['lastSuccess']).date()
            else:
                lastSuccess = status['lastSuccess']
            if(status['lastNoChange']!='NA'):
                lastNoChange = parse(status['lastNoChange']).date()
            else:
                lastNoChange = status['lastNoChange']
            data[table]['domain']=tb.domain
            data[table]['table']=table
            if lastSuccess==date.today() or lastNoChange==date.today():
                suc_list.append(data[table])
            else:
                error_list.append(data[table])
        return func(tb,suc_list,error_list)
    return wrapper1
    
def temp_status(tb, error_list=None, suc_list=None):
    if suc_list is not None:
        temp_location = "/tmp/ready/ready_"
        temp_data = suc_list
    else:
        temp_location = "/tmp/failed/failure_"
        temp_data = error_list
    try:
        with open(temp_location+tb.domain+".txt", 'w+') as outfile:
            json.dump(temp_data, outfile)
    except IOError:
        print('Error list data write failed.')

@get_table_status
@format_json
def check_status(tb, suc_list, error_list):
    est_time = datetime.now(timezone('EST5EDT'))
    while est_time.time() < Data_Freshness_Cut_Off_Time:
        if len(error_list)>0:
            temp_status(tb, error_list=error_list, suc_list=None)
            time.sleep(60)
            return check_status(tb)
        else:
            temp_status(tb, error_list=None, suc_list=suc_list)
            return suc_list
    else:
        temp_status(tb, error_list=error_list, suc_list=None)
        exit("Source tables are not updated")
    return error_list

#Assign file S3 path for ingestion as well as metric
def s3_copy_data(suc_list,spark):
    metric_s3_path = f's3://vgi-platform-{ENVM}-us-east-1-csdo-client360/csdo_master/metrics/table_status/as_of_date={partition_date}/dest_tbl={domain}'
    print("NOTE:Output file path set to: %s" %metric_s3_path)
    json_rdd=spark.sparkContext.parallelize(suc_list)
    df=spark.read.json(json_rdd)
    df.repartition(1).write.mode("overwrite").parquet(metric_s3_path)
    
# initializing spark session
def launch_spark():
    spark = SparkSession.builder.appName("Data Freshness check").config("spark.sql.parquet.writeLegacyFormat", "true").config("hive.exec.dynamic.partition", "true").config("hive.exec.dynamic.partition.mode", "nonstrict").enableHiveSupport().getOrCreate()
    return spark
    
def get_property(section,propertyName):
    config = configparser.RawConfigParser()
    config.read(propertiesFile)
    try:
        return config.get(section,propertyName)
    except Exception as e:
        return ""


if __name__ == "__main__":
    global ENVM,partition_date,domain,f_path,propertiesFile
    dmn=sys.argv[1]
    propertiesFile="/home/hadoop/scripts/scripts/transactions/configs/data_freshness.properties"
    ENVM=get_property('dataFreshness','ENVM')
    partition_date=get_property('dataFreshness','as_of_date')
    f_path=get_property('dataFreshness','f_path')
    domain=get_property('dataFreshness','domain')
    spark =launch_spark()
    tb = parse_params(domain, f_path)
    suc_list = check_status(tb)
    s3_copy_data(suc_list,spark)
