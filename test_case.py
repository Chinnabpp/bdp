import os
import datetime
import pandas as pd
import pg8000
import boto3
from madbUtilities import utilities as utilities
from unittest.mock import Mock
from awsglue.utils import getResolvedOptions

import pytest

from etl.scripts import cash_flow_etl
from pyspark.sql.types import StructType

dirname = os.path.dirname
TEST_DATA_DIR = os.path.join(dirname(os.path.abspath(__file__)), "resources")
FUND_JSON_FILE = os.path.join(TEST_DATA_DIR, "fund_test.json")

def test_transform_happy_path(spark, spark_context, logger):
    cash_flow_etl.logger = logger
    cash_flow_etl.env = "local"
    cash_flow_etl.sc = spark_context
    cash_flow_etl.spark = spark

    cash_flow_df = spark.read.option("multiline", True).json(
        os.path.join(TEST_DATA_DIR, "CashFlow_data_happy_path.json")
    )
    security_identifier_df = spark.read.csv(
        os.path.join(TEST_DATA_DIR, "data.csv"),
        header=True,
    )

    transformed_df = cash_flow_etl.transform(
        cash_flow_df,
        security_identifier_df,
        "staging_identifier",
    )

    transformed_df_dict = transformed_df.toPandas().to_dict()
    assert transformed_df.count() == 2
    assert transformed_df_dict["port_id"][0] == 2
    assert transformed_df_dict["fund_name"][0] == "Balanced Index Fund Inv"
    assert transformed_df_dict["net_cash_flow"][0] == 55693.63
    assert transformed_df_dict["subscriptions"][0] == 41298.55
    assert transformed_df_dict["redemptions"][0] == -16947.94
    assert transformed_df_dict["exchanges_in"][0] == 31343.02
    assert transformed_df_dict["exchanges_out"][0] == 0
    assert transformed_df_dict["conversions_in"][0] == 0
    assert transformed_df_dict["conversions_out"][0] == 0
    assert transformed_df_dict["reinvested_income"][0] == 0
    assert transformed_df_dict["reinvested_short_term_capital_gains"][0] == 0
    assert transformed_df_dict["reinvested_long_term_capital_gains"][0] == 0
    assert transformed_df_dict["date"][0] == datetime.datetime(2022,3,17)
    assert transformed_df_dict["all_sahre_port_id"][0] == 9951
    # assert(transformed_df_dict['effective_date'][0] == datetime.datetime(2021,9,3))


def compare_schema(schema_a: StructType, schema_b: StructType) -> bool:
    """
    Utility menthod to comapre two schema and return the results of comparison
    Args:
        schema_a (StructType): Schema for comparison
        schema_b (StructType): Schema for comparison
    Returns:
        bool: Result of schema comparison
    """
    return len(schema_a) == len(schema_b) and all(
        (a.name, a.dataType) == (b.name, b.dataType)
        for a, b in zip(schema_a, schema_b)
    )

def get_db_conn(aurora_details):
    """
    Creates and returns a connection to the MADB Database
    """
    conn = pg8000.dbapi.connect(
        user=aurora_details["user"],
        password=aurora_details["pass"],
        host=aurora_details["host"],
        database=aurora_details["database"],
    )


def test_schema(spark, spark_context, logger):
    cash_flow_etl.logger = logger
    cash_flow_etl.env = "local"
    cash_flow_etl.sc = spark_context
    cash_flow_etl.spark = spark

    cash_flow_df = spark.read.option("multiline", True).json(
        os.path.join(TEST_DATA_DIR, "CashFlow_data_happy_path.json")
    )
    security_identifier_df = spark.read.csv(
        os.path.join(TEST_DATA_DIR, "data.csv"),
        header=True,
    )
    
    transformed_df = cash_flow_etl.transform(
        cash_flow_df,
        security_identifier_df,
        "staging_identifier",
    )
    session = boto3.session.Session()
    region = session.region_name
    sm_client = boto3.client("secretsmanager")
    args = getResolvedOptions(sys.argv, ["env", "daily_trigger_key", "effective_date"])
    env = args["env"]
    aurora_details = utilities.retrieve_aurora_details(sm_client, region, env)
    connection= get_db_conn(aurora_details)
    db_df=pd.read_sql_table("cash_flow",connection)
    assert transformed_df.compare_schema(transformed_df.schema, db_df.schema) == True
