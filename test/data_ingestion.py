import unittest
from pyspark import *
from pyspark.sql.types import DecimalType, IntegerType, StringType, DateType
from pyspark.sql.functions import *
from pyspark import SQLContext
from pyspark.sql import SparkSession
from pyspark.context import SparkContext as sc
from pyspark.sql import Row
from collections import namedtuple
from pytz import timezone
from datetime import date,datetime
from dateutil.parser import parse
import sys,argparse,json,time 
import re,glob,sys,os
from os import path
import datetime
from unittest.mock import patch
from datetime import date,datetime
import io
import sys
import data_ingestion
import sys,subprocess
from datetime import datetime,timedelta

os.environ["PYSPARK_PYTHON"] = "python3"

class TestDataIngestionCheck(unittest.TestCase):
    def setup():
        pass

    def teardown():
        pass
    
    def test_launch_spark(self):
        partition_date='2023-01-10'
        print("test_launch_spark----------------------------------------------------")
        spark=data_ingestion.launch_spark(partition_date)
        #test spark session object
        print ("spark type: ", type(spark))
        self.assertEqual(True,True)
        #test as_of_date,next_ingest_date
        expected=(partition_date,str((datetime.strptime(partition_date, '%Y-%m-%d') + timedelta(days=1,hours=9))))
        actual=spark.sql("SELECT ${as_of_date} AS as_of_date,${next_ingest_date} AS next_ingest_date")
        self.assertEqual(expected[0],actual.collect()[0]['as_of_date'])
        self.assertEqual(expected[0],actual.collect()[0]['next_ingest_date'])
        
    def test_capture_params_domain_account(self):
        partition_date='2023-01-10'
        spark=data_ingestion.launch_spark(partition_date)
        f_path='/hdfs/insert_mapping.txt'
        file_name='tbalances_by_account'
        domain='account'
        actual = data_ingestion.capture_params(spark,f_path,file_name,domain)
        expected=spark.createDataFrame('act_ta_act','tbalances_by_account','Accounts','OGC_Dna_Legalcompliance','csdo_master','account')
        self.assertEqual(expected,actual)
        
    def test_capture_params_domain_transaction(self):
        partition_date='2023-01-10'
        spark=data_ingestion.launch_spark(partition_date)
        f_path='/hdfs/insert_mapping.txt'
        file_name='ttransactions_by_account'
        domain='transaction'
        actual = data_ingestion.capture_params(spark,f_path,file_name,domain)
        expected=spark.createDataFrame('txn_ta','ttransactions_by_account','Transactions','OGC_Dna_Legalcompliance','csdo_master','transaction')
        self.assertEqual(expected,actual)
        
    def test_capture_params_file_name_none(self):
        partition_date='2023-01-10'
        spark=data_ingestion.launch_spark(partition_date)
        f_path='/hdfs/insert_mapping.txt'
        file_name='None'
        domain='transaction'
        actual = data_ingestion.capture_params(spark,f_path,file_name,domain)
        expected=spark.createDataFrame('txn_ta','ttransactions_by_account','Transactions','OGC_Dna_Legalcompliance','csdo_master','transaction')
        self.assertEqual(expected,actual)
        
    def test_capture_params_empty(self):
        partition_date='2023-01-10'
        spark=data_ingestion.launch_spark(partition_date)
        f_path='/hdfs/insert_mapping.txt'
        file_name='ABC'
        domain='transaction'
        actual = data_ingestion.capture_params(spark,f_path,file_name,domain)
        expected=spark.createDataFrame(data = [])
        self.assertEquals(expected,actual)
        
    def test_file_data(self):
        partition_date='2023-01-10'
        spark=data_ingestion.launch_spark(partition_date)
        f_path='/hdfs/insert_mapping.txt'
        file_name='ttransactions_by_account'
        domain='transaction'
        conf_params = data_ingestion.capture_params(spark,f_path,file_name,domain)
        for file in conf_params:
            source_table, target_table, target_folder, source_database, target_database = data_ingestion.file_data(file)
            self.assertEquals(source_table,'txn_ta')
            self.assertEquals(target_table,'ttransactions_by_account')
            self.assertEquals(target_folder,'Transactions')
            self.assertEquals(source_database,'OGC_Dna_Legalcompliance')
            self.assertEquals(target_database,'csdo_master')
            
    def test_file_path(self):
        partition_date='2023-01-10'
        spark=data_ingestion.launch_spark(partition_date)
        f_path='/hdfs/insert_mapping.txt'
        file_name='ttransactions_by_account'
        domain='transaction'
        ENVM=''
        conf_params = data_ingestion.capture_params(spark,f_path,file_name,domain)
        for file in conf_params:
            source_table, target_table, target_folder, source_database, target_database = data_ingestion.file_data(file)
            ingestion_s3_path, metric_s3_path = data_ingestion.file_path(target_database, target_folder, target_table,ENVM,partition_date)
            expected_ingestion_path='''s3://vgi-platform-{}-us-east-1-csdo-client360/{}/{}/{}/as_of_date={}'''.format(ENVM,'csdo_master','Transactions','ttransactions_by_account',partition_date)
            expected_metric_path='''s3://vgi-platform-{}-us-east-1-csdo-client360/{}/metrics/transform_counts/as_of_date={}/dest_tbl={}'''.format(ENVM,'Transactions',partition_date,'ttransactions_by_account')
            self.assertEquals(ingestion_s3_path,expected_ingestion_path)
            self.assertEquals(metric_s3_path,expected_metric_path)
            
    def test_drop_ingestion_data(self):
        partition_date='2023-01-10'
        spark=data_ingestion.launch_spark(partition_date)
        f_path='/hdfs/insert_mapping.txt'
        file_name='ttransactions_by_account'
        domain='transaction'
        ENVM=''
        conf_params = data_ingestion.capture_params(spark,f_path,file_name,domain)
        for file in conf_params:
            source_table, target_table, target_folder, source_database, target_database = data_ingestion.file_data(file)
            ingestion_s3_path, metric_s3_path = data_ingestion.file_path(target_database, target_folder, target_table,ENVM,partition_date)
            data_ingestion.drop_ingestion_data(ingestion_s3_path)           
            self.assertEquals(subprocess.call(["hadoop", "fs", "-ls", ingestion_s3_path]),0)
    
    def test_ingestion(self):
        partition_date='2023-01-10'
        spark=data_ingestion.launch_spark(partition_date)
        f_path='/hdfs/insert_mapping.txt'
        file_name='ttransactions_by_account'
        domain='transaction'
        ENVM=''
        client360_state=''
        hql_path= ''
        conf_params = data_ingestion.capture_params(spark,f_path,file_name,domain)
        for file in conf_params:
            source_table, target_table, target_folder, source_database, target_database = data_ingestion.file_data(file)
            ingestion_s3_path, metric_s3_path = data_ingestion.file_path(target_database, target_folder, target_table,ENVM,partition_date)
            actual_src_cnt=data_ingestion.ingestion(ingestion_s3_path, target_table, spark,partition_date,hql_path,client360_state)
            expected_src_cnt='TO UPDATE'
            self.assertEquals(expected_src_cnt,actual_src_cnt)
                              
    def test_add_partition_failure(self):
        partition_date='2023-01-10'
        spark=data_ingestion.launch_spark(partition_date)
        f_path='/hdfs/insert_mapping.txt'
        file_name='ttransactions_by_account'
        domain='transaction'
        ENVM=''
        client360_state=''
        hql_path= ''
        conf_params = data_ingestion.capture_params(spark,f_path,file_name,domain)
        for file in conf_params:
            source_table, target_table, target_folder, source_database, target_database = data_ingestion.file_data(file)
            ingestion_s3_path, metric_s3_path = data_ingestion.file_path(target_database, target_folder, target_table,ENVM,partition_date)
            src_cnt=data_ingestion.ingestion(ingestion_s3_path, target_table, spark,partition_date,hql_path,client360_state)
            capturedOutput = io.StringIO()                  
            sys.stdout = capturedOutput                     
            data_ingestion.add_partition(spark, target_table,partition_date)
            capturedOutput = io.StringIO()                 
            sys.stdout = capturedOutput                    
            self.assertIn('Error',capturedOutput.getvalue())
                              
    def test_calculate_dest_counts(self):
        partition_date='2023-01-10'
        spark=data_ingestion.launch_spark(partition_date)
        f_path='/hdfs/insert_mapping.txt'
        file_name='ttransactions_by_account'
        domain='transaction'
        ENVM=''
        client360_state=''
        hql_path= ''
        conf_params = data_ingestion.capture_params(spark,f_path,file_name,domain)
        for file in conf_params:
            source_table, target_table, target_folder, source_database, target_database = data_ingestion.file_data(file)
            ingestion_s3_path, metric_s3_path = data_ingestion.file_path(target_database, target_folder, target_table,ENVM,partition_date)
            src_cnt=data_ingestion.ingestion(ingestion_s3_path, target_table, spark,partition_date,hql_path,client360_state)
            data_ingestion.add_partition(spark, target_table,partition_date)
            actual_dest_cnt = data_ingestion.calculate_dest_counts(target_database, target_table, spark,partition_date)
            expected_dest_cnt = 'TO UPDATE'
            self.assertEquals(expected_dest_cnt,actual_dest_cnt)
                              
    def test_metric_count(self):
        partition_date='2023-01-10'
        spark=data_ingestion.launch_spark(partition_date)
        f_path='/hdfs/insert_mapping.txt'
        file_name='ttransactions_by_account'
        domain='transaction'
        ENVM=''
        client360_state=''
        hql_path= ''
        conf_params = data_ingestion.capture_params(spark,f_path,file_name,domain)
        for file in conf_params:
            source_table, target_table, target_folder, source_database, target_database = data_ingestion.file_data(file)
            ingestion_s3_path, metric_s3_path = data_ingestion.file_path(target_database, target_folder, target_table,ENVM,partition_date)
            src_cnt=data_ingestion.ingestion(ingestion_s3_path, target_table, spark,partition_date,hql_path,client360_state)
            data_ingestion.add_partition(spark, target_table,partition_date)
            dest_cnt = data_ingestion.calculate_dest_counts(target_database, target_table, spark,partition_date)
            capturedOutput = io.StringIO()                  
            sys.stdout = capturedOutput   
            data_ingestion.metric_count(source_database, source_table, target_table, spark, src_cnt, dest_cnt,partition_date)
            capturedOutput = io.StringIO()                 
            sys.stdout = capturedOutput
            self.assertIn("NOTE: DATA INGESTION AND METRIC COUNTS END",capturedOutput.getvalue())
            
if __name__ == '__main__':
    unittest.main()
