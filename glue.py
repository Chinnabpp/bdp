from logging import exception
import boto3
import itertools
import json
import requests
import sys
import urllib.parse
import pandas
from datetime import datetime, date, timedelta
from decimal import Decimal
from enum import Enum
from itertools import chain
from itertools import groupby
from typing import Any, List, Union, Optional
import calendar
import datetime
from datetime import datetime, timedelta
from pyspark.sql.functions import lit
import pyspark.sql.functions as F


import pg8000
from madbUtilities import utilities as utilities
#from pydantic import BaseModel, Field, validate_arguments, validator

from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SQLContext, dataframe
from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from awsglue.utils import getResolvedOptions
from pyspark.sql.types import (
    BooleanType,
    StructType,
    StructField,
    StringType,
    DecimalType,
    DateType,
)

logger = utilities.config_logger()
file_key = None

def get_file_key():
    """generates a file key based on date + time
    sets once and returns the same key in a single run
    """
    global file_key
    if not file_key:
        file_key = str(datetime.utcnow()).replace(
            " ", "."
        )  # 2022-03-10.14:27:57.424429
    return file_key
    
def audit_fund_data(query, aurora_details):
    try:
        conn = get_db_conn(aurora_details)
        cursor = conn.cursor()
        cursor.execute(query)
        conn.commit()
    except Exception as e:
        logger.error("There is issue while inserting data to audit table")
        raise e
        
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

    return conn

def delete_staging_data(table_name, aurora_details):
    file_key = get_file_key()
    query = "DELETE FROM " + table_name + " WHERE file_key = '" + file_key + "'"
    logger.info("### cash Delete SQL Query: {0}".format(query))
    utilities.run_sql_with_pg(query, aurora_details)

def load_cash_data_staging(df, table_name, aurora_details):

    logger.info("deleting cash data: {0} prior to load...".format(table_name))
    delete_staging_data(table_name, aurora_details)
    try:
        logger.info("loading cash data: {0}".format(table_name))
        write_df = utilities.write_to_aurora(
            df,
            table_name,
            aurora_details,
            glueContext,
        )
        logger.info(
            "load completed Successfully! Record Count: {0}".format(write_df.count())
        )
    except Exception as e:
        logger.error(
            "Failed to load Staging Data for: {0} with error: {1}".format(table_name, e)
        )
        delete_staging_data(table_name, aurora_details)
        raise e
        
def load_cash_data_master(aurora_details, sql, tablename):
    try:
        madb_s3_bucket_name = "vgi-gis-{0}-us-east-1-maa-gifs-madb-staging".format(env)
        sql_file_path = "s3://{0}/{1}".format(madb_s3_bucket_name,
                "glue-etl/cash-flow-job//sql/cash_flow_upsert.sql",
            )
        query = utilities.get_query_from_text(sql_file_path, sc)
        update_query = query.format(file_key)
        logger.info("### cash Upsert SQL Query: {0}".format(update_query))
        logger.info("Upserting the cash Data to master table.")
        utilities.run_sql_with_pg(update_query, aurora_details)
        logger.info("Going to delete from cash staging table")
        delete_staging_data(tablename, aurora_details)
        logger.info("Done Deleting from cash staging table")
    except Exception as e:
        logger.error(f"Failed to load data to cash table :{e}")
        delete_staging_data(tablename, aurora_details)
        raise e

def load(transformed_cash_df: DataFrame, sql, aurora_details) -> None:
    """
    Stages the transformed Rimes DataFrame and then performs an upsert to the master table

    :param rimes_df: DataFrame, Transformed Rimes DataFrame to load into DB
    :param staging_identifier: str, Identifier used to delete and upsert from index_constituent_staging table
    :return: None
    """
    load_cash_data_staging(
        transformed_cash_df, "cash_flow_staging", aurora_details)
    records_written = transformed_cash_df.count()
    logger.info(f"Total records to be written to database: {records_written}")
    load_cash_data_master(
        aurora_details,
        "cash_flow_upsert.sql",
        "cash_flow_staging",
    )

def extract(cash_bucket, trigger_key, aurora_details):
    """
    Read the passed file key and the MAdb fund table into pyspark dataframes and return them

    :return: cash_df, fund_df: tuple(DataFrame),
        returns read file and MAdb fund table in dataframes
    """
    logger.info("Begin extract")

    cash_file_path = f"s3://{cash_bucket}/{trigger_key}"
    logger.info("Reading df from %s", cash_file_path)
    ch_df = spark.read.format("csv").option("header", "true").load(cash_file_path)
    logger.info("making cash_df from s3 to spark")
    cash_df = ch_df.toPandas()
    cash_df = cash_df.drop(['Fund Name', 'Date', 'all_share_port_id'], 1)
    logger.info("Done making cash_df from s3 to pandas")
    
    exclude_distribution_group_codes_query = "SELECT PORT_ID FROM FUND_DISTRIBUTION_GROUP WHERE DISTRIBUTION_GROUP_CODE IN ('G020','G035','G080','G110','G150','G190','G200','G220','G240','G250','G390','G393','G395','G440','G460','G510','G520','G999')"
    fund_id_sql_query = f"SELECT PORT_ID FROM FUND WHERE PRODT_TYPE IN ('FUND','SHARE-CLASS') AND DEACTIVATION_DATE IS NULL AND PORT_ID NOT IN ({exclude_distribution_group_codes_query})"

    # Get List of valid fund ids from madb database
    db_conn = pg8000.dbapi.connect(
        user=aurora_details["user"],
        password=aurora_details["pass"],
        host=aurora_details["host"],
        database=aurora_details["database"],
    )
    
    with db_conn:
        cursor = db_conn.cursor()
        logger.info("Going to execute select port_id query")
        cursor.execute(fund_id_sql_query)
        logger.info("Done executing select port_id query")
        fund_ids_result_set = cursor.fetchall()

        fund_df = pandas.DataFrame(fund_ids_result_set, columns=['port_id'])

    logger.info("Extract complete")
    return cash_df, fund_df

def transform(cash_df,fund_df,eff_dt):

    logger.info("Begin transform")
    csh_df = pandas.merge(cash_df, fund_df, left_on='Port ID', right_on='port_id')
    logger.info("joins done  on df's and staring renaming")
    df = csh_df.rename(columns={"Net Cash Flow": "net_cash_flow", "Exchanges In": "exchanges_in", "Exchanges Out": "exchanges_out", "Conversions In": "conversions_in", "Conversions Out": "Conversions_out", "Reinvested Income": "reinvested_income", "Reinvested Short-Term Capital Gains": "reinvested_short_term_capital_gains", "Reinvested Long-Term Capital Gains": "reinvested_long_term_capital_gains"})
    logger.info("Done renaming and starting renaming")
    df["time_period_code"] = "D"
    df["effective_date"] = eff_dt
    df["file_key"] = get_file_key()
    logger.info("Done renaming")
    df.drop('Port ID', axis=1, inplace=True)
    df['effective_date'] = pandas.to_datetime(df['effective_date']).dt.date
    df['net_cash_flow'] = pandas.to_numeric(df['net_cash_flow'])
    df['Subscriptions'] = pandas.to_numeric(df['Subscriptions'])
    df['Redemptions'] = pandas.to_numeric(df['Redemptions'])
    df['exchanges_in'] = pandas.to_numeric(df['exchanges_in'])
    df['exchanges_out'] = pandas.to_numeric(df['exchanges_out'])
    df['conversions_in'] = pandas.to_numeric(df['conversions_in'])
    df['Conversions_out'] = pandas.to_numeric(df['Conversions_out'])
    df['reinvested_income'] = pandas.to_numeric(df['reinvested_income'])
    df['reinvested_short_term_capital_gains'] = pandas.to_numeric(df['reinvested_short_term_capital_gains'])
    df['reinvested_long_term_capital_gains'] = pandas.to_numeric(df['reinvested_long_term_capital_gains'])
    result_df = df.drop_duplicates()
    final_df=spark.createDataFrame(result_df)
    records_read = final_df.count()
    final_dups_df = (final_df.groupBy("port_id", "effective_date", "time_period_code").agg(F.count("*").alias("total_count")))
    dup_records = final_dups_df.filter(final_dups_df.total_count > 1)
    if dup_records.count() > 0:
        logger.info("### cashf low contains multiple records. duplicate rows: {0}".format(dup_records.show()))
    logger.info(f"Total matched records readen from s3 and fund db : {records_read}")
    
    return final_df

def get_date(effective_date):
    try:
        if effective_date == "null":
            effective_date = None
        if effective_date:
            year = int(effective_date[0:4])
            month = int(effective_date[4:6])
            day = int(effective_date[6:8])
            final_dt = effective_date
            eff_dt = effective_date[:4]+"-"+ effective_date[4:6]+"-"+effective_date[6:]
        else:
            utc_curr_date = datetime.now()
            # change to this value on Winter November
            est_time_delta = timedelta(hours=-5)
            # change to this value on Spring March
            #est_time_delta = timedelta(hours=-4)
            curr_date = utc_curr_date + est_time_delta
            year, month, day = curr_date.year, curr_date.month, curr_date.day
            logger.info("### Current Date is : {0}".format(curr_date))
            if date.weekday(curr_date) < 1:  # if it's Monday
                lastBusDay = curr_date - timedelta(days=3)  # then make it Friday
            else:
                lastBusDay = curr_date - timedelta(days=1)
            eff_dt = lastBusDay.strftime("%Y-%m-%d")
            yst_dt = datetime.strftime(lastBusDay, '%Y-%m-%d').split("-")
            year = yst_dt[0]
            month = yst_dt[1]
            day = yst_dt[2]
            final_dt = year + month + day
        return year, month, final_dt, eff_dt
    except Exception as e:
        logger.error(f"Failed to get data :{e}")


def main():

    logger.info("Initializing Glue Job Execution")
    # RECORD START TIME FOR THE AUDIT
    job_start_time = datetime.now()

    year, month, final_dt, eff_dt = get_date(effective_date)

    cash_bucket = "vgi-gis-{0}-us-east-1-polaris-cashflow".format(env)
    trigger_key = f'{daily_trigger_key}/effective_date_year={year}/effective_date_month={month}/Daily_CashFlow_ShareClass_{final_dt}.csv'
    # trigger_key = f'/curated/security_cls=internal/data=daily_shareclass/format=csv/type=fund/effective_date_year=2022/effective_date_month=12/Daily_CashFlow_ShareClass_20221202.csv'

    cash_df, fund_df = extract(cash_bucket, trigger_key, aurora_details)

    transformed_cash_df = transform(cash_df,fund_df, eff_dt)

    load(transformed_cash_df, "cash_flow_upsert.sql", aurora_details)
    
    #END OF JOB, BEGIN AUDIT
    job_end_time = datetime.now()
    
    audit_dict = {
        "job_start_time": job_start_time,
        "job_end_time": job_end_time,
        "records_read": records_read,
        "records_written": records_written,
        "data_type": "CASH",
        "data_component": "CASH_FLOW",
        "file_key": get_file_key()
    }

    query = utilities.return_audit_query(audit_dict)
    logger.info("### Inserting audit details for this job run...")
    audit_fund_data(query, aurora_details)
    logger.info("### Audit complete")

if __name__ == "__main__":
    records_read = 0
    records_written = 0
    job_start_time = datetime.now()
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
   
    session = boto3.session.Session()
    region = session.region_name
    sm_client = boto3.client("secretsmanager")
    args = getResolvedOptions(sys.argv, ["env", "daily_trigger_key", "effective_date"])
    env = args["env"]
    daily_trigger_key = args["daily_trigger_key"]
    effective_date = args["effective_date"]
    logger.info("Found ENV to be: %s", env)
    aurora_details = utilities.retrieve_aurora_details(sm_client, region, env)
    main()
