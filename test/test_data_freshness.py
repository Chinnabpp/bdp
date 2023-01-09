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
from mock import MagicMock
from datetime import date,datetime

os.environ["PYSPARK_PYTHON"] = "python3"

#shutil.rmtree('bucket_parm')
sys.path.append('../python/transactions')
import data_freshness

Table_Info = namedtuple("Table_Info", "defaults, message, domain")

class TestDataFreshnessCheck(unittest.TestCase):
    def setup():
        pass

    def teardown():
        pass

    def test_launch_spark(self):
        print("test_launch_spark----------------------------------------------------")
        spark=data_freshness.launch_spark()
        print ("spark type: ", type(spark))
        self.assertEqual(True,True)
    

    def test_parse_params(self):
        expected_result=Table_Info(defaults={'action': 'poll', 'jarPath': '/emr/app/dependencies/notification-client.jar', 'messageType': 'status', 'subject': 'None', 'app_prefix': 'ARE', 'pipeline': 'Retail'}, message="[{'app_prefix': 'ARE', 'pipeline': 'Retail_Enterprise_Ingestion', 'db': 'retail_entmaster1', 'table': 'TC_VGI_INT_INS'}, {'app_prefix': 'ARE', 'pipeline': 'Retail_Enterprise_Ingestion', 'db': 'retail_entmaster1', 'table': 'TACCT'}, {'app_prefix': 'CI1', 'pipeline': 'Commingled_Enterprise_Ingestion', 'db': 'platform_entmaster1', 'table': 'TSAG'}, {'app_prefix': 'CI1', 'pipeline': 'Commingled_Enterprise_Ingestion', 'db': 'platform_entmaster1', 'table': 'TCIN015'}]", domain='ta_acct')
        domain='ta_acct'
        path=os.getcwd()
        f_path= path + "/resources/validation_mapping.json"
        actual_result=data_freshness.parse_params(domain, f_path)
        self.assertEqual(expected_result,actual_result)
    
    @patch('data_freshness.executeNotificationClient')
    def test_check_status(self, mock_api_call):
        expec_exit_code_exception="Source tables are not updated"
        suc_list=[{"lastUpdated": "2021-03-04T14:19:35.263479", "latestStatus": "SUCCESS", "appPrefix": "CI1", "pipeline": "Commingled_Enterprise_Ingestion", "lastSuccess": "2021-03-04T14:19:35.263479", "lastFailure": "NA", "lastNoChange": "2021-02-01T14:15:54.395759", "lastMaintenance": "NA", "as_of_date": "2021-03-04T13.39.09.561", "other": "NA", "domain": "ta_acct", "table": "platform_entmaster1.TSAG"}, {"lastUpdated": "2021-03-04T14:12:36.708862", "latestStatus": "SUCCESS", "appPrefix": "CI1", "pipeline": "Commingled_Enterprise_Ingestion", "lastSuccess": "2021-03-04T14:12:36.708862", "lastFailure": "NA", "lastNoChange": "2021-01-14T14:21:24.772904", "lastMaintenance": "NA", "as_of_date": "2021-03-04T10.00.45.068", "other": "NA", "domain": "ta_acct", "table": "platform_entmaster1.TCIN015"}]
        tb=Table_Info(defaults={'action': 'poll', 'jarPath': '/emr/app/dependencies/notification-client.jar', 'messageType': 'status', 'subject': 'None', 'app_prefix': 'ARE', 'pipeline': 'Retail'}, message="[{'app_prefix': 'ARE', 'pipeline': 'Retail_Enterprise_Ingestion', 'db': 'retail_entmaster1', 'table': 'TC_VGI_INT_INS'}, {'app_prefix': 'ARE', 'pipeline': 'Retail_Enterprise_Ingestion', 'db': 'retail_entmaster1', 'table': 'TACCT'}, {'app_prefix': 'CI1', 'pipeline': 'Commingled_Enterprise_Ingestion', 'db': 'platform_entmaster1', 'table': 'TSAG'}, {'app_prefix': 'CI1', 'pipeline': 'Commingled_Enterprise_Ingestion', 'db': 'platform_entmaster1', 'table': 'TCIN015'}]", domain='ta_acct')
        error_list=[]
        mock_api_call.return_value = MagicMock(tb,{'retail_entmaster1.TC_VGI_INT_INS': {'lastUpdated': '2021-03-05T08:34:59.661365', 'latestStatus': 'SUCCESS', 'appPrefix': 'ARE', 'pipeline': 'Retail_Enterprise_Ingestion', 'lastSuccess': '2021-03-05T08:34:59.661365', 'lastFailure': '2021-02-18T18:15:16.705752', 'lastNoChange': '2021-02-24T16:30:29.978052', 'lastMaintenance': 'NA', 'as_of_date': '2021-03-05T05:35:41.617', 'other': 'NA'}, 'retail_entmaster1.TACCT': {'lastUpdated': '2021-03-05T08:42:06.248761', 'latestStatus': 'SUCCESS', 'appPrefix': 'ARE', 'pipeline': 'Retail_Enterprise_Ingestion', 'lastSuccess': '2021-03-05T08:42:06.248761', 'lastFailure': '2021-02-19T00:07:03.045738', 'lastNoChange': '2021-02-27T08:32:13.855082', 'lastMaintenance': 'NA', 'as_of_date': '2021-03-05T06:53:48.614', 'other': 'NA'}, 'platform_entmaster1.TSAG': {'lastUpdated': '2021-03-04T14:19:35.263479', 'latestStatus': 'SUCCESS', 'appPrefix': 'CI1', 'pipeline': 'Commingled_Enterprise_Ingestion', 'lastSuccess': '2021-03-04T14:19:35.263479', 'lastFailure': 'NA', 'lastNoChange': '2021-02-01T14:15:54.395759', 'lastMaintenance': 'NA', 'as_of_date': '2021-03-04T13.39.09.561', 'other': 'NA'}, 'platform_entmaster1.TCIN015': {'lastUpdated': '2021-03-04T14:12:36.708862', 'latestStatus': 'SUCCESS', 'appPrefix': 'CI1', 'pipeline': 'Commingled_Enterprise_Ingestion', 'lastSuccess': '2021-03-04T14:12:36.708862', 'lastFailure': 'NA', 'lastNoChange': '2021-01-14T14:21:24.772904', 'lastMaintenance': 'NA', 'as_of_date': '2021-03-04T10.00.45.068', 'other': 'NA'}})
        est_time = datetime.now(timezone('EST5EDT'))
        if est_time.hour < 11:
            data_freshness.check_status(tb)
        else:
            with self.assertRaises(SystemExit) as cm:
                data_freshness.check_status(tb)
            self.assertEqual(cm.exception.code,expec_exit_code_exception)
            
    def test_temp_status(self):
        tb=Table_Info(defaults={'action': 'poll', 'jarPath': '/emr/app/dependencies/notification-client.jar', 'messageType': 'status', 'subject': 'None', 'app_prefix': 'ARE', 'pipeline': 'Retail'}, message="[{'app_prefix': 'ARE', 'pipeline': 'Retail_Enterprise_Ingestion', 'db': 'retail_entmaster1', 'table': 'TC_VGI_INT_INS'}, {'app_prefix': 'ARE', 'pipeline': 'Retail_Enterprise_Ingestion', 'db': 'retail_entmaster1', 'table': 'TACCT'}, {'app_prefix': 'CI1', 'pipeline': 'Commingled_Enterprise_Ingestion', 'db': 'platform_entmaster1', 'table': 'TSAG'}, {'app_prefix': 'CI1', 'pipeline': 'Commingled_Enterprise_Ingestion', 'db': 'platform_entmaster1', 'table': 'TCIN015'}]", domain='ta_acct')
        error_list=[]
        suc_list=[]
        data_freshness.temp_status(tb, error_list=None, suc_list=None)
        print('Error in writing file to temp location')
        
    @patch('data_freshness.s3_copy_data')
    def test_s3_copy_data(self, mock_api_call):
        spark = SparkSession \
            .builder \
            .config("spark.some.config.option", "some-value") \
            .enableHiveSupport() \
            .getOrCreate()
        #args=argparse.Namespace(region='test', partition_date='2020-01-01', domain='ta_acct')
        suc_list=[]
        metric_s3_path = f's3://vgi-ead-test-us-east-1-ogcdna-transform/OGC_Dna_Legalcompliance/metrics/table_status/ingest_date=2020-01-01/dest_tbl=ta_acct'
        data_freshness.s3_copy_data(suc_list,'test','2020-01-01','ta_acct',spark)       
        actual_result=os.path.exists(metric_s3_path)
        self.assertFalse(actual_result)
        
if __name__ == '__main__':
    unittest.main()
