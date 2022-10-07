import boto3
import itertools
import json
import requests
import sys
import urllib.parse
from datetime import datetime, date, timedelta
from decimal import Decimal
from enum import Enum
from itertools import chain
from itertools import groupby
from typing import Any, List, Union, Optional
import calendar


import pg8000
from madbUtilities import utilities as utilities
from pydantic import BaseModel, Field, validate_arguments, validator

from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SQLContext, dataframe
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    StructType,
    StructField,
    StringType,
    DecimalType,
    DateType,
)

logger = utilities.config_logger()


ADVISOR_VALUATION_API = None
FUND_MANAGED_ASSETS_API = None
FUND_OUTSTANDING_SHARES_API = None
FUND_PRICES_API = None
EXCLUDE_DISTRIBUTION_GROUPS = [
    "G020",
    "G035",
    "G080",
    "G110",
    "G150",
    "G190",
    "G200",
    "G220",
    "G240",
    "G250",
    "G390",
    "G393",
    "G395",
    "G440",
    "G460",
    "G510",
    "G520",
    "G999",
]
DISTRIBUTION_GROUPS_TO_VALIDATE = [
    "G090",
    "G010",
    "G015",
    "G030",
    "G140",
    "G170",
    "G180",
    "G182",
    "G185",
    "G230",
    "G260",
    "G320",
    "G323",
    "G330",
    "G350",
    "G353",
    "G370",
    "G380",
    "G490",
]
DISTRIBUTION_GROUPS_AUM = [
    "G010",
    "G015",
    "G140",
    "G180",
    "G182",
    "G185",
    "G230",
    "G320",
    "G323",
    "G330",
    "G350",
    "G353",
    "G370",
    "G380",
    "G490",
]

_quoted_dgs = "'" + "','".join(DISTRIBUTION_GROUPS_TO_VALIDATE) + "'"
_quoted_dgs2 = "'" + "','".join(DISTRIBUTION_GROUPS_AUM) + "'"
_port_ids_sql = f"select port_id from fund_distribution_group where distribution_group_code in ({_quoted_dgs})"
_port_ids_sql_aum = f"select port_id from fund_distribution_group where distribution_group_code in ({_quoted_dgs2})"
ACTIVE_FILTER = "(deactivation_date is NULL or deactivation_date > cast(CURRENT_DATE as date)) and inception_date <= cast(CURRENT_DATE as date) and (seeded_flag = 'Y' or seeded_flag is null)"
ACTIVE_PORT_IDS_SQL = (
    f"select port_id from fund where port_id in ({_port_ids_sql}) and {ACTIVE_FILTER}"
)
ACTIVE_PORT_IDS_SQL_AUM = f"select port_id from fund where port_id in ({_port_ids_sql_aum}) and {ACTIVE_FILTER}"
USD_BASE_CURRENCY_ACTIVE_PORT_IDS_SQL = f"{ACTIVE_PORT_IDS_SQL} and port_currency_code IN ('USD', 'United States of America, US Dollar')"
ACTIVE_PORT_PARENT_IDS_LIST_SQL = (
    f"select parent_port_id from fund where port_id in ({ACTIVE_PORT_IDS_SQL})"
)
ACTIVE_PARENT_PORT_IDS_SQL = f"select f.port_id from fund f left join fund_distribution_group fdg on f.port_id = fdg.port_id where f.port_id in ({ACTIVE_PORT_PARENT_IDS_LIST_SQL}) and fdg.distribution_group_code in ({_quoted_dgs2})"
MULT_ADVISOR_FLAGGED_FUND_PORTS_SQL = f"select port_id from fund where prodt_type='FUND' and {ACTIVE_FILTER} and multi_advisor_flag = 'Y'"
ACTIVE_MULTI_ADVISED_PORT_IDS_SQL = f"select port_id from fund where prodt_type='ADVISOR' and parent_port_id in ({MULT_ADVISOR_FLAGGED_FUND_PORTS_SQL})"

ENV = None
file_key = None

# UTIL*************##


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


# Enums


class ProdtType(Enum):
    """
    This class hold different prodt_type found in funds table
    """

    ADVISOR = "ADVISOR"
    FUND = "FUND"
    SHARECLASS = "SHARE-CLASS"


###********** MODELS ***********###


class AliasModel(BaseModel):
    """
    Responsible for mapping snake_case attributes with camelCase
    eg: It will ensure that vg_token_url and vgTokenUrl are same
    """

    class Config:
        # allow population of an attribute either by pascal_case or camelCase
        allow_population_by_field_name = True


class SysArgs(AliasModel):
    """
    Holds the system arguments received when the glue job is triggered
    """

    env: str = Field(alias="env")
    vg_token_url: str = Field(alias="vgTokenUrl")
    client_secret: str = Field(alias="clientSecret")
    advisorValuationAPI: str = Field(alias="advisoValuationAPI")
    fundManagedAssetsAPI: str = Field(alias="fundManagedAssetsAPI")
    fundOutstandingSharesAPI: str = Field(alias="fundOutstandingSharesAPI")
    fundPricesAPI: str = Field(alias="fundPricesAPI")
    effective_date: Optional[str] = Field(None, alias="effectiveDate")
    valuation_type_codes: str = Field(alias="valuationTypeCodes")
    table_name: str = Field(alias="tableName")
    skip_tna: Optional[str] = Field(alias="skipTna")
    skip_nav: Optional[str] = Field(alias="skipNav")
    glueJobData: Optional[str] = Field(alias="glueJobData")
    clear_all_exceptions: Optional[str] = Field(alias="clearAllExceptions")
    scopes: str


class ParamsModel(AliasModel):
    """
    This is a data model for the query parameters passed in a URL
    eg: http://example.com?effectiveDate=10-11-2020&limit=800
    here, effectiveDate and limit are the query parameters
    """

    time_period_code: str = Field(alias="timePeriodCode")
    effective_date: Optional[str] = Field(None, alias="effectiveDate")
    limit: int = 100
    offset: int = 0

    @property
    def api(self):
        """
        This should be implemented by child classes
        """
        raise NotImplementedError

    def transform_list(cls, v):
        """
        method reponsible for converting a list to comma separated string
        eg: [1,2,3] gets converted to "1,2,3"
        eg: ["001", "A012", "BCD3"] gets converted to "001,A012,BCD3"
        We need this conversion, to transform a list to a query param.
        We cannot send a list as a query param. Hence need a comma separated string.
        """
        if type(v) == list:
            return ",".join(v)
        return v


class AdvisorValuationParams(ParamsModel):
    """
    URL Query parameters for Advisor
    """

    _prod_type: ProdtType = ProdtType.ADVISOR
    advisor_ids: Union[List[str], str] = Field(alias="advisorIds")
    valuation_type_codes: Union[List[str], str] = Field(alias="valuationTypeCodes")

    @property
    def api(self):
        global ADVISOR_VALUATION_API
        return ADVISOR_VALUATION_API

    @validator("advisor_ids", pre=True)
    def transform_advisor_ids(cls, v):
        """
        convert list of advisor_ids into comma separated string.
        """
        return cls.transform_list(cls, v)

    @validator("valuation_type_codes", pre=True)
    def transform_valuation_type_codes(cls, v):
        """
        convert list of valuation_type_codes into comma separated string.
        """
        return cls.transform_list(cls, v)


class BasePortIdsParams(ParamsModel):
    """
    Generic params class with portIds param
    """

    port_ids: Union[List[str], str] = Field(None, alias="portIds")

    @validator("port_ids", pre=True)
    def transform_advisor_ids(cls, v):
        """
        convert list of port_ids into comma separated string.
        """
        return cls.transform_list(cls, v)


class FundManagedAssetsParams(BasePortIdsParams):
    """
    URL Query parameters for Fund Managed Assets
    """

    _prod_type: ProdtType = ProdtType.FUND
    asset_type_codes: Union[List[str], str] = Field(alias="assetTypeCodes")
    identifier_type: Optional[str] = Field(None, alias="identifierType")
    currency_code: Optional[str] = Field(None, alias="currencyCodes")

    @property
    def api(self):
        global FUND_MANAGED_ASSETS_API
        return FUND_MANAGED_ASSETS_API

    @validator("asset_type_codes", pre=True)
    def transform_valuation_type_codes(cls, v):
        """
        convert list of asset_type_codes into comma separated string.
        """
        return cls.transform_list(cls, v)


class ShareClassParams(FundManagedAssetsParams):
    """
    URL Query parameters for Share-Class
    """

    _prod_type: ProdtType = ProdtType.SHARECLASS


class FundOustandingSharesParams(BasePortIdsParams):
    """
    URL Query parameters for Fund Outstanding Shares API
    """

    _prod_type: ProdtType = ProdtType.SHARECLASS

    @property
    def api(self):
        """
        Corresponding API endpoint for these param
        """
        global FUND_OUTSTANDING_SHARES_API
        return FUND_OUTSTANDING_SHARES_API


class FundPricesParams(BasePortIdsParams):
    """
    URL Query parameters for Fund Prices API
    """

    _prod_type: ProdtType = ProdtType.SHARECLASS
    price_type_codes: Union[List[str], str] = Field(alias="priceTypeCodes")

    @property
    def api(self):
        """
        Corresponding API endpoint for these params
        """
        global FUND_PRICES_API
        return FUND_PRICES_API

    @validator("price_type_codes", pre=True)
    def transform_valuation_type_codes(cls, v):
        """
        convert list of price_type_codes into comma separated string.
        """
        return cls.transform_list(cls, v)


class DBRecord(BaseModel):
    """
    Data Model defining the Table record.
    """

    effective_date: date
    time_period_code: str
    port_id: Optional[str] = None
    currency: Optional[str] = None
    TNA: Optional[Decimal] = None
    NAV: Optional[Decimal] = None
    AUM: Optional[Decimal] = None
    outstanding_shares: Optional[Decimal] = None

    @classmethod
    def spark_schema(cls):
        """
        Responsible for generating a Pyspark Data Schema from the above attributes
        """
        schema = []
        field_type_class = None
        # get all fields (effective_date, time_period_code, port_id, ... etc) from above
        for key, mf in cls.__fields__.items():
            # check the type of the field from above.
            if mf.type_ == str:
                # based on the type get the respective type of Pyspark Datatype to build the pyspark schema
                field_type_class = StringType()
            elif mf.type_ == Decimal:
                field_type_class = DecimalType(38, 15)
            elif mf.type_ == date:
                field_type_class = DateType()
            schema.append(StructField(key, field_type_class))
        return StructType(schema)


class AdvisorValuationData(AliasModel):
    """
    The Data Model for key "advisorValuationData" received in the API Response for advisors endpoint
    """

    effective_date: date = Field(alias="effectiveDate")
    time_period_code: str = Field(alias="timePeriodCode")
    currency: Optional[str] = None
    TNA: Optional[Decimal] = Field(None, alias="TNA")
    NAV: Optional[Decimal] = Field(None, alias="NAV")
    outstanding_shares: Optional[Decimal] = Field(None, alias="outstandingShares")


class ContentModel(AliasModel):
    """
    Generic Data Model for key "content" received in the API Response for all API endpoints
    """

    def db_records(self):
        raise NotImplementedError

    def dataset(self):
        """
        Builds a dataset from the DBRecords to be inserted into table using pyspark
        """
        return list({tuple(record.dict().values()) for record in self.db_records()})


class AdvisorValuationContent(ContentModel):
    """
    The Data Model for key "content" received in the API Response for advisors endpoint
    """

    advisor_id: str = Field(alias="advisorId")
    advisor_valuation_data: List[AdvisorValuationData] = Field(
        alias="advisorValuationData"
    )

    def db_records(self):
        """
        Forms the DBRecord for each valudation data and returns a list
        """
        dct = {"port_id": self.advisor_id}
        for avd in self.advisor_valuation_data:
            avd_dct = {key: val for key, val in avd.dict().items() if val}
            dct.update(avd_dct)
        return [DBRecord(**dct)]


class FundManagedAssetsData(AliasModel):
    """
    The Data Model for key "assets" received in the API Response for funds
    """

    port_id: str = Field(alias="portId")
    effective_date: date = Field(alias="effectiveDate")
    time_period_code: str = Field(alias="timePeriodCode")
    currency: str
    fund_asset_amount: Decimal = Field(alias="fundAssetAmount")
    asset_type_code: str = Field(alias="assetTypeCode")


class FundManagedAssetsContent(ContentModel):
    """
    The Data Model for key "content" received in the API Response for funds
    """

    parent_port_id: str = Field(alias="parentPortId")
    assets: List[FundManagedAssetsData]

    def db_records(self):
        """
        Forms the DBRecord for each valudation data and returns a list
        """
        return [
            DBRecord(
                port_id=asset.port_id,
                effective_date=asset.effective_date,
                time_period_code=asset.time_period_code,
                currency=asset.currency,
                TNA=asset.fund_asset_amount if asset.asset_type_code == "TNA" else None,
                AUM=asset.fund_asset_amount if asset.asset_type_code == "AUM" else None,
            )
            for asset in self.assets
        ]


class FundOutstandingShareDetail(AliasModel):
    """
    The Data Model for key "outstandingShareDetails" received in the API Response for fund outstanding shares API
    """

    effective_date: date = Field(alias="effectiveDate")
    time_period_code: str = Field(alias="timePeriodCode")
    outstanding_share: Decimal = Field(alias="outstandingShare")


class FundOutstandingSharesContent(ContentModel):
    """
    The Data Model for key "content" received in the API Response for fund outstanding shares API
    """

    port_id: str = Field(alias="portId")
    outstanding_share_details: List[FundOutstandingShareDetail] = Field(
        alias="outstandingShareDetails"
    )

    def db_records(self):
        """
        Forms the DBRecord for each valudation data and returns a list
        """
        return [
            DBRecord(
                port_id=self.port_id,
                effective_date=outstanding_share_detail.effective_date,
                time_period_code=outstanding_share_detail.time_period_code,
                outstanding_shares=outstanding_share_detail.outstanding_share,
            )
            for outstanding_share_detail in self.outstanding_share_details
        ]


class FundPricesContent(ContentModel):
    """
    The Data Model for key "content" received in the API Response for fund prices API
    """

    port_id: str = Field(alias="portId")
    effective_date: date = Field(alias="effectiveDate")
    time_period_code: str = Field(alias="timePeriodCode")
    currency_code: str = Field(alias="currencyCode")
    price: Decimal

    def db_records(self):
        """
        Forms the DBRecord for each valudation data and returns a list
        """
        return [
            DBRecord(
                port_id=self.port_id,
                effective_date=self.effective_date,
                time_period_code=self.time_period_code,
                currency=self.currency_code,
                NAV=self.price,
            )
        ]


class ResponseMetadataPagination(BaseModel):
    """
    The Data Model for key "pagination" received in the API Response
    """

    next: Optional[bool] = False
    offset: int
    limit: int


class ResponseMetadata(BaseModel):
    """
    The Data Model for key "metadata" received in the API Response
    """

    pagination: Optional[ResponseMetadataPagination] = None


class ResponseModel(BaseModel):
    """
    The Data Model for response received from API
    """

    metadata: ResponseMetadata
    content: Optional[
        List[
            Union[
                AdvisorValuationContent,
                FundManagedAssetsContent,
                FundOutstandingSharesContent,
                FundPricesContent,
            ]
        ]
    ] = []

    def db_records(self):
        return list(
            itertools.chain.from_iterable([c.db_records() for c in self.content])
        )

    def dataset(self):
        return list(itertools.chain.from_iterable([c.dataset() for c in self.content]))


###**********UTILS**********###


def encode_url(params: dict):
    """
    Converts a params dictionary into a single  query parameters string
    eg: {'effectiveDate': '10-11-2020', 'name': 'APP'} gets converted into
    effectiveDate=10-11-2020&name=APP
    """
    filtered_dct = {k: v for k, v in params.items() if v != None}
    return urllib.parse.urlencode(filtered_dct)


@validate_arguments
def prepare_url(params: ParamsModel):
    """
    Responsible for preparing the API Url based on the params type
    """
    # build the complete url by encoding the params dict to a query params string
    return f"{params.api}?{encode_url(params.dict(by_alias=True))}"


def get_authorization_headers(
    token_url: str, client_secret: str, scopes: str, sm_client=None
):
    """
    Get vanguard token which will be used for the Api request. Added scopes.
    """
    if not sm_client:
        sm_client = boto3.client("secretsmanager")

    response = sm_client.get_secret_value(
        SecretId=client_secret, VersionStage="AWSCURRENT"
    )

    secret_obj = json.loads(response["SecretString"])
    secret_id = secret_obj["client_id"]
    secret = secret_obj["client_secret"]
    params = {"client_id": secret_id, "client_secret": secret, "scope": scopes}
    response = requests.post(token_url, data=params)
    token = response.json()["access_token"]
    headers = {"Authorization": "Bearer " + token}
    return headers


###**********MAIN*******###


def parse_sys_args():
    """
    Get all the system parameters from the fund Valuation glue job.
    """
    logger.info("###Parsing Sys Args.")
    args = getResolvedOptions(
        sys.argv,
        [
            "env",
            "vgTokenUrl",
            "clientSecret",
            "advisoValuationAPI",
            "effectiveDate",
            "fundManagedAssetsAPI",
            "fundOutstandingSharesAPI",
            "fundPricesAPI",
            "valuationTypeCodes",
            "tableName",
            "scopes",
            "skipTna",
            "clearAllExceptions",
            "skipNav",
            "glueJobData"
        ],
    )
    try:
        sys_args = SysArgs(**args)
    except Exception as e:
        import json

        raise Exception(json.dumps(args))
    if sys_args.effective_date == "null":
        sys_args.effective_date = None
    elif sys_args.effective_date and not isinstance(sys_args.effective_date, str):
        sys_args.effective_date = None
    global ADVISOR_VALUATION_API
    global FUND_MANAGED_ASSETS_API
    global FUND_OUTSTANDING_SHARES_API
    global FUND_PRICES_API
    global ENV
    ADVISOR_VALUATION_API = sys_args.advisorValuationAPI
    FUND_MANAGED_ASSETS_API = sys_args.fundManagedAssetsAPI
    FUND_OUTSTANDING_SHARES_API = sys_args.fundOutstandingSharesAPI
    FUND_PRICES_API = sys_args.fundPricesAPI
    ENV = sys_args.env
    return sys_args


def extract(params: ParamsModel, headers):
    """
    Fetch the data from API
    Incrementally calls the API fetching 800 records on each call.
    Returns a funddataset array
    """
    db_records = []
    has_next = True
    params.offset = 0
    while has_next:
        url = prepare_url(params)
        response = requests.get(url, headers=headers)
        res_json = response.json()

        try:
            response_obj = ResponseModel(**res_json)
        except Exception as e:
            error_message = (
                f"{response.status_code} : {response.content} {params.api} {e}"
            )
            logger.error(error_message)
            raise Exception(error_message)
        has_next = (
            response_obj.metadata.pagination.next
            if response_obj.metadata.pagination
            else False
        )

        params.offset = params.offset + 100
        db_records.extend(response_obj.db_records())
    return db_records


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


def execute_sql_query(aurora_details: Any, sql: str):
    """
    Executes the given sql query and returns the result set
    """
    logger.info(f"Executing SQL: {sql}")
    conn = get_db_conn(aurora_details)
    cursor = conn.cursor()
    cursor.execute(sql)
    records = cursor.fetchall()
    records = list(chain.from_iterable(records))
    return records


def clear_exceptions_table(
    aurora_details,
    effective_date_daily: str,
    effective_date_monthly: str,
    clear_all_exceptions: str,
):
    """
    Clears exception table rows with the given effective date
    """
    conn = get_db_conn(aurora_details)
    cursor = conn.cursor()
    if clear_all_exceptions == "N":
        logger.info(
            f"Clearing daily exceptions for effective date {effective_date_daily}."
        )
        daily_exceptions_sql = f"DELETE from fund_valuation_exceptions where effective_date in ({effective_date_daily}) and time_period_code='D'"
        cursor.execute(daily_exceptions_sql)
        if effective_date_monthly:
            logger.info(
                f"Clearing monthly exceptions for effective date {effective_date_monthly}."
            )
            monthly_exception_sql = f"DELETE from fund_valuation_exceptions where effective_date in ({effective_date_monthly}) and time_period_code='M'"
            cursor.execute(monthly_exception_sql)
        conn.commit()
    else:
        logger.info(
            f"Clearing all exceptions as clear_all_exceptions flag: {clear_all_exceptions}"
        )
        all_exceptions_sql = "DELETE from fund_valuation_exceptions"
        cursor.execute(all_exceptions_sql)
        conn.commit()
    logger.info("Cleared exceptions")


@validate_arguments
def fetch_portids(
    aurora_details: Any, prodt_type: ProdtType, non_usd_base_currency=False
):
    """
    Fetch port ids from the fund table for a given prodt_type i.e Advisor, Fund, Share-class
    """

    quoted_dgs = "'" + "','".join(EXCLUDE_DISTRIBUTION_GROUPS) + "'"
    exclude_port_ids_sql = f"select port_id from fund_distribution_group where distribution_group_code in ({quoted_dgs})"
    sql = f"select port_id from fund where prodt_type = '{prodt_type.value}'"
    if prodt_type == ProdtType.ADVISOR:
        sql = (
            sql
            + " and (a_account_deactivation_date = '9999-12-31' or a_account_deactivation_date is Null)"
        )
    else:
        sql = (
            sql + " and (deactivation_date is NULL or deactivation_date = '9999-12-31')"
        )
    sql = sql + f" and port_id not in ({exclude_port_ids_sql})"
    if non_usd_base_currency:
        sql += " and port_currency_code NOT IN ('USD', 'United States of America, US Dollar')"
    else:
        sql += (
            " and port_currency_code IN ('USD', 'United States of America, US Dollar')"
        )
    return execute_sql_query(aurora_details, sql)


def get_sql_upsert_to_master(sql_file):
    global ENV
    madb_s3_bucket_name = f"vgi-gis-{ENV}-us-east-1-maa-gifs-madb-staging"
    sql_file_path = (
        f"s3://{madb_s3_bucket_name}/glue-etl/maa-fund-valuation-job/sql/{sql_file}"
    )
    logger.info(f"Reading SQL File for master table upsert: {sql_file_path}")
    query_file = sc.textFile(sql_file_path)
    query_file_content_list = query_file.collect()
    query = " ".join(i for i in query_file_content_list)
    upsert_query = query.format("'" + get_file_key() + "'")
    logger.info(f"Upsert SQL Query: {upsert_query}")
    return upsert_query


def load_to_master(aurora_details):
    # Loading to master
    exc = None
    upsert_query = get_sql_upsert_to_master("fund_valuation_master_upsert.sql")
    conn = get_db_conn(aurora_details)
    cursor = conn.cursor()
    try:
        cursor.execute("START TRANSACTION")
        cursor.execute(upsert_query)
        conn.commit()
        logger.info("### Data load to fund_valutaion is completed.")
    except Exception as e:
        logger.error(
            "There is issue related to load the data to master, deleting existing data from staging table"
        )
        cursor.execute("ROLLBACK;")
        exc = e
    finally:
        utilities.delete_from_table(sys_args.table_name, get_file_key(), aurora_details)

    if exc:
        logger.error(exc)
        raise exc


def load(fund_df: dataframe, table_name: str, aurora_details: dict):
    utilities.delete_from_table(table_name, get_file_key(), aurora_details)
    utilities.write_to_aurora(fund_df, table_name, aurora_details, glueContext)
    load_to_master(aurora_details)


def audit_fund_data(query, aurora_details):
    try:
        conn = get_db_conn(aurora_details)
        cursor = conn.cursor()
        cursor.execute(query)
        conn.commit()
    except Exception as e:
        logger.error("There is issue while inserting data to audit table")
        raise e


def run_for(date_str: Optional[str], skip_tna: str):
    utc_curr_date = datetime.now().date()
    est_time_delta = timedelta(hours=-4)
    curr_date = utc_curr_date + est_time_delta
    logger.info("### Current Date is : {0}".format(curr_date))
    if date.weekday(curr_date) < 1:  # if it's Monday
        lastBusDay = curr_date - timedelta(days=3)  # then make it Friday
    else:
        lastBusDay = curr_date - timedelta(days=1)
    previous_day_effective_date = lastBusDay.strftime("%Y-%m-%d")
    logger.info(
        "### Previous business date is: {0}".format(previous_day_effective_date)
    )
    # If date string is not passed, then we run for todays date
    if not date_str:
        # sys_args.effective_date is not passed2, which means its systematic executions
        year, month, day = curr_date.year, curr_date.month, curr_date.day
        # skip_tna = 'N' means that its 11 pm run, and we will only run for current effective date T
        if skip_tna == "N":
            # if its sunday 11 pm run, we need to convert the current date to Friday so we get Friday's data
            if date.weekday(curr_date) == 6:
                curr_date_new = curr_date - timedelta(days=2)
                periods = [(curr_date_new.strftime("%Y-%m-%d"), "D")]
                effective_date_for_exception_daily = (
                    "'" + curr_date_new.strftime("%Y-%m-%d") + "'"
                )
            else:
                periods = [(curr_date.strftime("%Y-%m-%d"), "D")]
                effective_date_for_exception_daily = (
                    "'" + curr_date.strftime("%Y-%m-%d") + "'"
                )
            logger.info(
                "### 11 PM Job: effective_date_for_exception_daily: {0}".format(
                    effective_date_for_exception_daily
                )
            )
        else:
            # skip_tna = 'Y' means that its 11 am run, and we will run both T and T - 1 as some of the fund pricing data come late from previous date
            periods = [(curr_date.strftime("%Y-%m-%d"), "D")]
            periods.append((previous_day_effective_date, "D"))
            effective_date_for_exception_daily = (
                "'"
                + previous_day_effective_date
                + "','"
                + curr_date.strftime("%Y-%m-%d")
                + "'"
            )
            logger.info(
                "### effective_date_for_exception_daily: {0}".format(
                    effective_date_for_exception_daily
                )
            )
    else:
        # if sys_args.effective_date is not null, means its a rerun by the user from control panel
        # and we only run for the specified effective date
        date_str = date_str.split(":")[-1]
        year, month, day = map(int, date_str.split("-"))
        periods = [(date_str, "D")]
        effective_date_for_exception_daily = "'" + date_str + "'"
    dt = date(year, month, day)

    # Lets check if we need to run for a last date of month
    last_date_of_month = None
    if day <= 4:
        # sometimes we may receive monthly data in the first four days of next month
        # pick the last date of last month
        last_date_of_month = dt.replace(day=1) - timedelta(days=1)
    else:
        # pick the last date of current month
        # and check if todays date is the last date
        days_in_current_month = calendar.monthrange(year, month)[1]
        last_date_of_current_month = date(year, month, days_in_current_month)
        if dt == last_date_of_current_month:
            # todays date is the last date, set last_date_of_month
            last_date_of_month = last_date_of_current_month
    if last_date_of_month:
        # add the last date of a month for Monthly run
        # periods.append(
        #     (None if is_current_date else last_date_of_month.strftime("%Y-%m-%d"), "M")
        # )
        periods.append((last_date_of_month.strftime("%Y-%m-%d"), "M"))
        effective_date_for_exception_monthly = (
            "'" + last_date_of_month.strftime("%Y-%m-%d") + "'"
        )
    else:
        effective_date_for_exception_monthly = None
    logger.info("### periods array : {0}".format(periods))
    return (
        periods,
        effective_date_for_exception_daily,
        effective_date_for_exception_monthly,
    )


def transform(tna_nav_aum_df, outstanding_shares_df):
    """Group fund valuation records so there is one row per table constraint.
         Then join outstanding shares df, so there are outstandings shares for each currency.
    :param tna_nav_aum_df: DF containing rows of all tna, nav, aum records
    :param outstanding_shares_df: DF containing rows of all outstanding shares records
    :return: returns normalized df with one record per db constraints
    """
    if "outstanding_shares" in tna_nav_aum_df.columns:
        tna_nav_aum_df = tna_nav_aum_df.drop(tna_nav_aum_df.outstanding_shares)
    exp = [F.first(x, ignorenulls=True).alias(x) for x in tna_nav_aum_df.columns[4:]]
    tna_nav_aum_df = tna_nav_aum_df.groupBy(
        F.col("port_id"),
        F.col("effective_date"),
        F.col("time_period_code"),
        F.col("currency"),
    ).agg(*exp)

    outstanding_shares_df = outstanding_shares_df[
        "port_id", "effective_date", "time_period_code", "outstanding_shares"
    ]
    outstanding_shares_df = outstanding_shares_df.filter(
        outstanding_shares_df.outstanding_shares.isNotNull()
    )
    outstanding_shares_df = outstanding_shares_df.dropDuplicates(
        ["port_id", "effective_date", "time_period_code"]
    )

    fund_valuation_df = tna_nav_aum_df.join(
        outstanding_shares_df,
        ["port_id", "effective_date", "time_period_code"],
        how="left",
    )
    fund_valuation_df = fund_valuation_df.withColumn("file_key", F.lit(get_file_key()))

    return fund_valuation_df


def run_primary_data_quality_checks(fund_valuation_df):
    """Runs data quality checks on normalized fund valuation df to avoid DB insert errors
    :param fund_valuation_df: normalized fund valuation df
    :return: Tuple of fund_valuation_df and fund_exception_df
    """
    logger.info("Running primary data quality checks...")

    logger.info("Checking for null currencies")
    fund_valuation_exception_df = fund_valuation_df.filter(
        fund_valuation_df.currency.isNull()
    )

    fund_valuation_exception_df = fund_valuation_exception_df.withColumn(
        "exception_reason", F.lit("NULL CURRENCY CODE")
    )
    fund_valuation_df = fund_valuation_df.filter(fund_valuation_df.currency.isNotNull())
    return fund_valuation_df, fund_valuation_exception_df


def run_data_quality_checks(
    fund_valuation_df,
    active_port_ids: List[str] = [],
    usd_base_currency_active_port_ids: List[str] = [],
    active_parent_port_ids: List[str] = [],
    multi_advised_port_ids: List[str] = [],
):
    """Runs data quality checks on normalized fund valuation df
    And returns two dataframes: one with valid data and the other with invalid data
    These are secondary data quality checks. Even if the data is invalid, we will still insert it
    into the main table.
    """
    logger.info("Running secondary data quality checks")

    # Validation of include list starts here
    logger.info("Separating the active port ids data to be validated from funds data")
    active_port_ids_df = fund_valuation_df.filter(
        fund_valuation_df.port_id.isin(active_port_ids)
    )
    fund_valuation_df = fund_valuation_df.filter(
        ~fund_valuation_df.port_id.isin(active_port_ids)
    )
    usd_base_currency_active_port_ids_df = active_port_ids_df.filter(
        active_port_ids_df.port_id.isin(usd_base_currency_active_port_ids)
    )
    other_base_currency_active_port_ids_df = active_port_ids_df.filter(
        ~active_port_ids_df.port_id.isin(usd_base_currency_active_port_ids)
    )
    # At this point, the fund_valuation_df doesn't hold active port ids
    # we need to run the validation for active port ids separately and join the valid dataframe back into fund_valuation_df
    validation_name = "'active port ids whose base currency is USD'"
    logger.info(
        f"Checking if any of NAV, TNA, outstanding_shares shares is NULL for {validation_name}"
    )
    usd_base_currency_active_port_ids_exception_df = (
        usd_base_currency_active_port_ids_df.filter(
            usd_base_currency_active_port_ids_df.NAV.isNull()
            | usd_base_currency_active_port_ids_df.TNA.isNull()
            | usd_base_currency_active_port_ids_df.outstanding_shares.isNull()
        )
    )
    fund_valuation_exception_df = (
        usd_base_currency_active_port_ids_exception_df.withColumn(
            "exception_reason",
            F.lit(f"NAV, TNA, Outstanding Shares cannot be NULL for {validation_name}"),
        )
    )
    logger.info(
        f"Filtering out {validation_name} that doesn't have either of NAV, TNA, outstanding_shares"
    )
    usd_base_currency_active_port_ids_df = usd_base_currency_active_port_ids_df.filter(
        usd_base_currency_active_port_ids_df.NAV.isNotNull()
        & usd_base_currency_active_port_ids_df.TNA.isNotNull()
        & usd_base_currency_active_port_ids_df.outstanding_shares.isNotNull()
    )
    logger.info(f"Adding back the valid data of {validation_name} back to funds data")
    fund_valuation_df = fund_valuation_df.union(usd_base_currency_active_port_ids_df)

    # Validating active portfolios with base currency other than USD
    # Filter for portfolios with USD as currency. We don't need to do validation on NAV for these.
    validation_name = "'active port ids whose base currency is not USD, but the fund valuation currency is USD'"
    logger.info(
        f"Checking if any of NAV, TNA, outstanding_shares shares is NULL for {validation_name}"
    )
    non_base_usd_df = other_base_currency_active_port_ids_df.filter(
        other_base_currency_active_port_ids_df.currency == "USD"
    )
    non_base_usd_exceptions_df = non_base_usd_df.filter(
        non_base_usd_df.TNA.isNull() | non_base_usd_df.outstanding_shares.isNull()
    )
    non_base_usd_exceptions_df = non_base_usd_exceptions_df.withColumn(
        "exception_reason",
        F.lit(f"TNA & Outstanding Shares cannot be NULL for {validation_name}"),
    )
    logger.info(
        f"Filtering out {validation_name} that doesn't have either of TNA, outstanding_shares"
    )
    non_base_usd_df = non_base_usd_df.filter(
        non_base_usd_df.TNA.isNotNull() & non_base_usd_df.outstanding_shares.isNotNull()
    )
    logger.info(f"Adding back the valid data of {validation_name} back to funds data")
    fund_valuation_df = fund_valuation_df.union(non_base_usd_df)
    logger.info(
        f"Adding back the invalid data of {validation_name} to funds exception data"
    )
    fund_valuation_exception_df = fund_valuation_exception_df.union(
        non_base_usd_exceptions_df
    )

    # Filter for portfolios with non USD as currency & base currency. We need to validate all three: NAV, TNA, Outstanding shares.
    validation_name = "'active portfolios where neither the base currency nor the fund valuation currency is USD'"
    logger.info(
        f"Checking if any of NAV, TNA, outstanding_shares shares is NULL for {validation_name}"
    )
    other_non_base_usd_df = other_base_currency_active_port_ids_df.filter(
        other_base_currency_active_port_ids_df.currency != "USD"
    )
    other_non_base_usd_exceptions_df = other_non_base_usd_df.filter(
        other_non_base_usd_df.NAV.isNull()
        | other_non_base_usd_df.TNA.isNull()
        | other_non_base_usd_df.outstanding_shares.isNull()
    )
    other_non_base_usd_exceptions_df = other_non_base_usd_exceptions_df.withColumn(
        "exception_reason",
        F.lit(f"NAV,TNA & Outstanding Shares cannot be NULL for {validation_name}"),
    )
    logger.info(
        f"Filtering out {validation_name} that doesn't have either of NAV, TNA, outstanding_shares"
    )
    other_non_base_usd_df = other_non_base_usd_df.filter(
        other_non_base_usd_df.NAV.isNotNull()
        & other_non_base_usd_df.TNA.isNotNull()
        & other_non_base_usd_df.outstanding_shares.isNotNull()
    )
    logger.info(f"Adding back the valid data of {validation_name} back to funds data")
    fund_valuation_df = fund_valuation_df.union(other_non_base_usd_df)
    logger.info(
        f"Adding back the invalid data of {validation_name} to funds exception data"
    )
    fund_valuation_exception_df = fund_valuation_exception_df.union(
        other_non_base_usd_exceptions_df
    )

    # Validation of active port ids parent port ids
    logger.info(
        "Separating the active port ids parent port ids data to be validated from funds data"
    )
    active_parent_port_ids_df = fund_valuation_df.filter(
        fund_valuation_df.port_id.isin(active_parent_port_ids)
    )
    fund_valuation_df = fund_valuation_df.filter(
        ~fund_valuation_df.port_id.isin(active_parent_port_ids)
    )
    # At this point, the fund_valuation_df doesn't hold active port ids parent port ids
    # we need to run the validation for active port ids parent port ids separately and join the valid dataframe back into fund_valuation_df

    logger.info("Checking if AUM is NULL for active port id's parent port ids")
    active_parent_port_ids_exception_df = active_parent_port_ids_df.filter(
        active_parent_port_ids_df.AUM.isNull()
    )
    active_parent_port_ids_exception_df = (
        active_parent_port_ids_exception_df.withColumn(
            "exception_reason",
            F.lit("AUM cannot be NULL for active port id's parent port id"),
        )
    )
    logger.info("Filtering out active port id's parent port ids where AUM is NULL")
    active_parent_port_ids_df = active_parent_port_ids_df.filter(
        active_parent_port_ids_df.AUM.isNotNull()
    )
    logger.info(
        "Adding back the valid data of active port id's paret port ids back to funds data"
    )
    fund_valuation_df = fund_valuation_df.union(active_parent_port_ids_df)
    logger.info(
        "Adding back the invalid data of active port id's parent port ids to funds exception data"
    )
    fund_valuation_exception_df = fund_valuation_exception_df.union(
        active_parent_port_ids_exception_df
    )

    # Validation of multi advised port ids
    logger.info(
        "Separating the multi advised port ids data to be validated from funds data"
    )
    multi_advised_port_ids_df = fund_valuation_df.filter(
        fund_valuation_df.port_id.isin(multi_advised_port_ids)
    )
    fund_valuation_df = fund_valuation_df.filter(
        ~fund_valuation_df.port_id.isin(multi_advised_port_ids)
    )
    # At this point, the fund_valuation_df doesn't hold active port ids parent port ids
    # we need to run the validation for active port ids parent port ids separately and join the valid dataframe back into fund_valuation_df

    logger.info("Checking if TNA is NULL for multi advised port ids")
    multi_advised_port_ids_exception_df = multi_advised_port_ids_df.filter(
        multi_advised_port_ids_df.TNA.isNull()
    )
    multi_advised_port_ids_exception_df = (
        multi_advised_port_ids_exception_df.withColumn(
            "exception_reason", F.lit("TNA cannot be NULL for multi advised port id")
        )
    )
    logger.info("Filtering out multi advised port ids where TNA is NULL")
    multi_advised_port_ids_df = multi_advised_port_ids_df.filter(
        multi_advised_port_ids_df.TNA.isNotNull()
    )
    logger.info(
        "Adding back the valid data of multi advised port ids back to funds data"
    )
    fund_valuation_df = fund_valuation_df.union(multi_advised_port_ids_df)
    logger.info(
        "Adding back the invalid data of multi advised port ids to funds exception data"
    )
    fund_valuation_exception_df = fund_valuation_exception_df.union(
        multi_advised_port_ids_exception_df
    )

    return fund_valuation_df, fund_valuation_exception_df


def load_exceptions(fund_valuation_exception_df, aurora_details):
    """Loads exceptions into the fund_valuation_exceptions table in the DB
    :param fund_valuation_exception_df: df generated from data_quality_checks
    :param aurora_details: MAdb connection details
    """
    utilities.write_to_aurora(
        df=fund_valuation_exception_df,
        table_name="fund_valuation_exceptions",
        aurora_details=aurora_details,
        glueContext=glueContext,
    )


if __name__ == "__main__":
    # fetch the arguments available when the glue job is triggered
    sys_args: SysArgs = parse_sys_args()
    # create the spark, glue and sql context
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    sqlContext = SQLContext(sc)
    session = boto3.session.Session()

    # Initialize secretmanager client
    region = session.region_name
    sm_client = boto3.client("secretsmanager")

    job_start_time = datetime.now()
    fund_count = 0
    advisor_count = 0

    # Fetch authorization headers required to call our API endpoints to fetch data
    headers = get_authorization_headers(
        sys_args.vg_token_url, sys_args.client_secret, sys_args.scopes, sm_client
    )

    # Fetch the database credentials needed to load to/from a MADB table
    aurora_details = utilities.retrieve_aurora_details(sm_client, region, sys_args.env)

    # Fetch respective port ids from the fund table
    logger.info("Retrieving port_ids for API calls")
    advisor_ports = fetch_portids(aurora_details, ProdtType.ADVISOR)
    fund_ports = fetch_portids(aurora_details, ProdtType.FUND)
    share_class_ports = fetch_portids(aurora_details, ProdtType.SHARECLASS)

    # Fetch respective port ids from the fund table
    non_usd_advisor_ports = fetch_portids(
        aurora_details=aurora_details,
        prodt_type=ProdtType.ADVISOR,
        non_usd_base_currency=True,
    )
    non_usd_fund_ports = fetch_portids(
        aurora_details=aurora_details,
        prodt_type=ProdtType.FUND,
        non_usd_base_currency=True,
    )
    non_usd_share_class_ports = fetch_portids(
        aurora_details=aurora_details,
        prodt_type=ProdtType.SHARECLASS,
        non_usd_base_currency=True,
    )

    logger.info("Finished retrieving port_ids for API calls")

    params = []
    outstanding_shares_params = []
    # The port ids can be > 1000 and cannot fit in a URL. Hence, we need to split them and call our API
    split_offset = 90  # No. of port_ids using which, we will make one API call.

    # create params for both monthly and daily
    (
        periods,
        effective_date_for_exception_daily,
        effective_date_for_exception_monthly,
    ) = run_for(sys_args.effective_date, sys_args.skip_tna)
    clear_exceptions_table(
        aurora_details,
        effective_date_for_exception_daily,
        effective_date_for_exception_monthly,
        sys_args.clear_all_exceptions,
    )
    if sys_args.skip_nav != "Y"
        for date_str, time_period_code in periods:
            # Creating params for USD advisor ports
            logger.info(
                "### run for date: {0} and time period code: {1}".format(
                    date_str, time_period_code
                )
            )
            for i in range(0, len(advisor_ports), split_offset):
                params.append(
                    AdvisorValuationParams(
                        effective_date=date_str,
                        time_period_code=time_period_code,
                        advisor_ids=advisor_ports[i : i + split_offset],
                        valuation_type_codes="TNA,NAV",
                        currency_code="USD",
                    )
                )

                outstanding_shares_params.append(
                    AdvisorValuationParams(
                        effective_date=date_str,
                        time_period_code=time_period_code,
                        advisor_ids=advisor_ports[i : i + split_offset],
                        valuation_type_codes="OUTSTANDING_SHARES",
                    )
                )

            # Creating params for non USD advisor ports
            for i in range(0, len(non_usd_advisor_ports), split_offset):
                params.append(
                    AdvisorValuationParams(
                        effective_date=date_str,
                        time_period_code=time_period_code,
                        advisor_ids=non_usd_advisor_ports[i : i + split_offset],
                        valuation_type_codes="TNA,NAV",
                    )
                )

                if sys_args.skip_tna == "N":
                    logger.info(
                        "skip tna flag is N, loading USD TNA for Non USD base currency advisor funds..."
                    )
                    params.append(
                        AdvisorValuationParams(
                            effective_date=date_str,
                            time_period_code=time_period_code,
                            advisor_ids=non_usd_advisor_ports[i : i + split_offset],
                            valuation_type_codes="TNA",
                            currency_code="USD",
                        )
                    )

                outstanding_shares_params.append(
                    AdvisorValuationParams(
                        effective_date=date_str,
                        time_period_code=time_period_code,
                        advisor_ids=non_usd_advisor_ports[i : i + split_offset],
                        valuation_type_codes="OUTSTANDING_SHARES",
                    )
                )

            # Creating params for USD fund ports
            for i in range(0, len(fund_ports), split_offset):
                params.append(
                    FundManagedAssetsParams(
                        effective_date=date_str,
                        time_period_code=time_period_code,
                        asset_type_codes="AUM,TNA",
                        identifier_type="PORT",
                        port_ids=fund_ports[i : i + split_offset],
                        currency_code="USD",
                    )
                )

                params.append(
                    FundPricesParams(
                        effective_date=date_str,
                        time_period_code=time_period_code,
                        price_type_codes="NAV",
                        port_ids=fund_ports[i : i + split_offset],
                    )
                )

                outstanding_shares_params.append(
                    FundOustandingSharesParams(
                        effective_date=date_str,
                        time_period_code=time_period_code,
                        port_ids=fund_ports[i : i + split_offset],
                    )
                )

            # Creating params for non USD fund ports
            for i in range(0, len(non_usd_fund_ports), split_offset):
                params.append(
                    FundManagedAssetsParams(
                        effective_date=date_str,
                        time_period_code=time_period_code,
                        asset_type_codes="AUM,TNA",
                        identifier_type="PORT",
                        port_ids=non_usd_fund_ports[i : i + split_offset],
                    )
                )

                params.append(
                    FundPricesParams(
                        effective_date=date_str,
                        time_period_code=time_period_code,
                        price_type_codes="NAV",
                        port_ids=non_usd_fund_ports[i : i + split_offset],
                    )
                )
                if sys_args.skip_tna == "N":
                    logger.info(
                        "skip tna flag is N, loading USD TNA for Non USD base currency all share class funds..."
                    )
                    params.append(
                        FundManagedAssetsParams(
                            effective_date=date_str,
                            time_period_code=time_period_code,
                            asset_type_codes="TNA",
                            identifier_type="PORT",
                            currency_code="USD",
                            port_ids=non_usd_fund_ports[i : i + split_offset],
                        )
                    )

                outstanding_shares_params.append(
                    FundOustandingSharesParams(
                        effective_date=date_str,
                        time_period_code=time_period_code,
                        port_ids=non_usd_fund_ports[i : i + split_offset],
                    )
                )

            # Gathering params for Non USD Share class ports
            for i in range(0, len(share_class_ports), split_offset):
                params.append(
                    ShareClassParams(
                        effective_date=date_str,
                        time_period_code=time_period_code,
                        asset_type_codes="AUM,TNA",
                        identifier_type="PORT",
                        port_ids=share_class_ports[i : i + split_offset],
                        currency_code="USD",
                    )
                )

                params.append(
                    FundPricesParams(
                        effective_date=date_str,
                        time_period_code=time_period_code,
                        price_type_codes="NAV",
                        port_ids=share_class_ports[i : i + split_offset],
                    )
                )

                outstanding_shares_params.append(
                    FundOustandingSharesParams(
                        effective_date=date_str,
                        time_period_code=time_period_code,
                        port_ids=share_class_ports[i : i + split_offset],
                    )
                )

            # splitting Share-Class Params
            for i in range(0, len(non_usd_share_class_ports), split_offset):
                params.append(
                    ShareClassParams(
                        effective_date=date_str,
                        time_period_code=time_period_code,
                        asset_type_codes="AUM,TNA",
                        identifier_type="PORT",
                        port_ids=non_usd_share_class_ports[i : i + split_offset],
                    )
                )

                params.append(
                    FundPricesParams(
                        effective_date=date_str,
                        time_period_code=time_period_code,
                        price_type_codes="NAV",
                        port_ids=non_usd_share_class_ports[i : i + split_offset],
                    )
                )
                if sys_args.skip_tna == "N":
                    logger.info(
                        "skip tna flag is N, loading USD TNA for Non USD base currency share class funds..."
                    )
                    params.append(
                        ShareClassParams(
                            effective_date=date_str,
                            time_period_code=time_period_code,
                            asset_type_codes="TNA",
                            identifier_type="PORT",
                            port_ids=share_class_ports[i : i + split_offset],
                            currency_code="USD",
                        )
                    )

                outstanding_shares_params.append(
                    FundOustandingSharesParams(
                        effective_date=date_str,
                        time_period_code=time_period_code,
                        port_ids=non_usd_share_class_ports[i : i + split_offset],
                    )
                )
    if sys_args.skip_nav == "Y":
        glue_job_params = sys_args.glueJobData
        for i in range(0, len(glue_job_params)):
            params.append(
                FundPricesParams(
                    effective_date=glue_job_params[i].get("effectiveDate"),
                    time_period_code=glue_job_params[i].get("timePeriodCode"),
                    price_type_codes="NAV",
                    port_ids=glue_job_params[i].get("portId"),
                )
            )


    # to hold the complete data fetched from all APIs
    db_records = []
    outstanding_shares_records = []
    # extract data from the API for each splitted params above
    for param in params:
        records = extract(param, headers=headers)
        if type(param) == AdvisorValuationParams:
            advisor_count += len(records)
        else:
            fund_count += len(records)
        db_records.extend(records)

    for outstanding_shares_param in outstanding_shares_params:
        records = extract(outstanding_shares_param, headers=headers)
        outstanding_shares_records.extend(records)

    tna_aum_nav_df = spark.createDataFrame(
        data=db_records, schema=DBRecord.spark_schema()
    )

    outstanding_shares_df = spark.createDataFrame(
        data=outstanding_shares_records, schema=DBRecord.spark_schema()
    )

    fund_valuation_df = transform(tna_aum_nav_df, outstanding_shares_df)
    records_read = fund_valuation_df.count()

    active_port_ids = execute_sql_query(aurora_details, ACTIVE_PORT_IDS_SQL)
    usd_base_currency_active_port_ids = execute_sql_query(
        aurora_details, USD_BASE_CURRENCY_ACTIVE_PORT_IDS_SQL
    )
    active_parent_port_ids = execute_sql_query(
        aurora_details, ACTIVE_PARENT_PORT_IDS_SQL
    )
    multi_advised_port_ids = execute_sql_query(
        aurora_details, ACTIVE_MULTI_ADVISED_PORT_IDS_SQL
    )

    logger.info(f"Total records retrieved from API : {fund_valuation_df.count()}")
    # checking for data that might cause DB insertion errors
    fund_valuation_df, fund_valuation_exception_df = run_primary_data_quality_checks(
        fund_valuation_df
    )
    records_written = fund_valuation_df.count()
    logger.info(
        f"Records that might cause DB insertion errors | records with null currency: {fund_valuation_exception_df.count()}"
    )
    logger.info(f"Total records to be written to database: {records_written}")

    # Finding invalid data.
    (
        fund_valuation_valid_df,
        fund_valuation_secondary_exceptions_df,
    ) = run_data_quality_checks(
        fund_valuation_df,
        active_port_ids,
        usd_base_currency_active_port_ids,
        active_parent_port_ids,
        multi_advised_port_ids,
    )
    logger.info(
        f"Total valid records found after secondary data quality checks: {fund_valuation_valid_df.count()}"
    )
    if fund_valuation_secondary_exceptions_df.count() > 0:
        logger.info(
            f"There are at least 1 record found with incomplete data.Total records found with incomplete data after secondary data quality checks: {fund_valuation_secondary_exceptions_df.count()}"
        )
    else:
        logger.info("There are no record found with incomplete data.")

    # combining all exceptions
    fund_valuation_exception_df = fund_valuation_exception_df.union(
        fund_valuation_secondary_exceptions_df
    )

    # Finally, Load into the madb fund-valuation table
    load(
        fund_df=fund_valuation_df,
        table_name=sys_args.table_name,
        aurora_details=aurora_details,
    )

    if fund_valuation_exception_df.count() > 0:
        logger.info("Loading exceptions found to fund_valuation_exceptions table")
        load_exceptions(
            fund_valuation_exception_df,
            aurora_details=aurora_details,
        )

    # END OF JOB, BEGIN AUDIT
    job_end_time = datetime.now()

    data_component = "FUND_VALUATION"
    audit_dict = {
        "job_start_time": job_start_time,
        "job_end_time": job_end_time,
        "records_read": records_read,
        "records_written": records_written,
        "exception_count": fund_valuation_exception_df.count(),
        "data_type": "FUND",
        "data_component": data_component,
        "file_key": get_file_key(),
        "fund_count": fund_count,
        "advisor_count": advisor_count,
    }

    query = utilities.return_audit_query(audit_dict)
    logger.info("### Inserting audit details for this job run...")
    audit_fund_data(query, aurora_details)
    logger.info("### Audit complete")