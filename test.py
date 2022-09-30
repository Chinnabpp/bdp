from pyspark import SparkContext
from pyspark.sql import SparkSession, SQLContext
from decimal import Decimal
from etl.scripts.fund_valuation_etl import (
    AdvisorValuationParams,
    FundManagedAssetsParams,
    ShareClassParams,
)
from etl.scripts import fund_valuation_etl
import unittest
from logging import getLogger
import json
import os
from unittest.mock import MagicMock
from datetime import datetime
import calendar

LOGGING_WARN = 30
TEST_DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")


class Test(unittest.TestCase):
    _spark = None
    _spark_context = None
    _sql = None

    _advisor_port_ids = None
    _fund_port_ids = None
    _share_class_port_ids = None

    _advisor_params = None
    _fund_params = None
    _share_class_params = None
    _logger = None

    @classmethod
    def setUpClass(cls):
        cls._logger = getLogger("py4j")

        # -- enter --
        cls._logger.setLevel(LOGGING_WARN)
        sc = SparkContext.getOrCreate()
        cls._spark_context = sc
        cls._spark = SparkSession(cls._spark_context)
        cls._sql = SQLContext(cls._spark_context)

        with open(f"{TEST_DATA_DIR}/port-ids-advisor.json", "r") as f:
            cls._advisor_port_ids = json.load(f)
        with open(f"{TEST_DATA_DIR}/port-ids-fund.json", "r") as f:
            cls._fund_port_ids = json.load(f)
        with open(f"{TEST_DATA_DIR}/port-ids-share-class.json", "r") as f:
            cls._share_class_port_ids = json.load(f)

        cls._advisor_params = AdvisorValuationParams(
            effective_date="2020-12-31",
            time_period_code="M",
            advisor_ids=cls._advisor_port_ids,
            valuation_type_codes="TNA,NAV,OUTSTANDING_SHARES",
        )

        cls._fund_params = FundManagedAssetsParams(
            effective_date="2020-12-31",
            time_period_code="M",
            asset_type_codes="AUM,TNA",
            identifier_type="PORT",
            port_ids=cls._fund_port_ids,
        )

        cls._share_class_params = ShareClassParams(
            effective_date="2020-12-31",
            time_period_code="M",
            asset_type_codes="AUM,TNA",
            identifier_type="PORT",
            port_ids=cls._share_class_port_ids,
            currencyCodes="USD",
        )

    def test_advisor_prepare_url(self):
        fund_valuation_etl.ADVISOR_VALUATION_API = (
            "https://valuation-advisor.polaris.gise.c1.vanguard.com/advisor-valuations"
        )
        advisor_url = fund_valuation_etl.prepare_url(self.__class__._advisor_params)
        assert (
            advisor_url
            == "https://valuation-advisor.polaris.gise.c1.vanguard.com/advisor-valuations?timePeriodCode=M&effectiveDate=2020-12-31&limit=100&offset=0&advisorIds=ABEO%2CABET%2CAABM%2CAAFW%2CABHJ%2CABHK%2CABHO%2CABHP%2CABHQ%2CACK8%2CAD01%2CAAEU%2CAAEW%2CAAEX%2CAAFB%2CAPBY%2CAALW%2CAPAD&valuationTypeCodes=TNA%2CNAV%2COUTSTANDING_SHARES"
        )

    # def test_fund_prepare_url(self):
    #     fund_valuation_etl.FUND_MANAGED_ASSETS_API = "https://valuation-analytics.polaris.gise.c1.vanguard.com/fund-managed-assets"
    #     fund_url = fund_valuation_etl.prepare_url(self.__class__._fund_params)
    #     assert (
    #         fund_url
    #         == "https://valuation-analytics.polaris.gise.c1.vanguard.com/fund-managed-assets?timePeriodCode=M&effectiveDate=2020-12-31&limit=100&offset=0&portIds=0811%2C1445%2C4642%2C4643%2C3246%2C3247%2C3095%2C4644%2C4645%2C4157%2C4162%2C4163%2C4164%2C4165%2C4166%2C4167%2C3140%2C3150%2C3151%2C2545&assetTypeCodes=AUM%2CTNA&identifierType=PORT"
    #    )

    def test_share_class_prepare_url(self):
        fund_valuation_etl.FUND_MANAGED_ASSETS_API = "https://valuation-analytics.polaris.gise.c1.vanguard.com/fund-managed-assets"
        sc_url = fund_valuation_etl.prepare_url(self.__class__._share_class_params)
        assert (
            sc_url
            == "https://valuation-analytics.polaris.gise.c1.vanguard.com/fund-managed-assets?timePeriodCode=M&effectiveDate=2020-12-31&limit=100&offset=0&portIds=0807%2C4181%2C0730%2C0970%2C0093%2C4158%2C0094%2C0101%2C0102%2C0103%2C1120%2C1677%2C1142%2C1143%2C0114%2C0115%2C0116%2C0118%2C0119%2C0122&assetTypeCodes=AUM%2CTNA&identifierType=PORT&currencyCodes=USD"
        )
        fund_valuation_etl.FUND_MANAGED_ASSETS_API = None

    def test_run_for(self):
        # run_for should return a single period "D" for daily run, if the date is not end of month
        # of the date doesn't fall in 1,2,3,4
        (
            periods_1,
            daily_exceptions_date_1,
            monthly_exceptions_date_1,
        ) = fund_valuation_etl.run_for(":2022-5-25", "N")
        assert periods_1 == [("2022-5-25", "D")]
        assert daily_exceptions_date_1 == "'" + "2022-5-25" + "'"
        assert monthly_exceptions_date_1 == None

        # run_for should return a single period "D" for current days daily run and a single period "M" for monthly,
        # if the date is end of month or the date falls in 1,2,3,4
        (
            periods_2,
            daily_exceptions_date_2,
            monthly_exceptions_date_2,
        ) = fund_valuation_etl.run_for(":2022-5-31", "N")
        assert periods_2 == [
            ("2022-5-31", "D"),
            ("2022-05-31", "M"),
        ]
        assert daily_exceptions_date_2 == "'" + "2022-5-31" + "'"
        assert monthly_exceptions_date_2 == "'" + "2022-05-31" + "'"
        (
            periods_3,
            daily_exceptions_date_3,
            monthly_exceptions_date_3,
        ) = fund_valuation_etl.run_for(":2022-6-1", "N")
        print(
            "###  periods_3: {0} daily_exception_date: {1} monthly_exception_Date {2}".format(
                periods_3, daily_exceptions_date_3, monthly_exceptions_date_3
            )
        )
        assert periods_3 == [
            ("2022-6-1", "D"),
            ("2022-05-31", "M"),
        ]
        assert daily_exceptions_date_3 == "'" + "2022-6-1" + "'"
        assert monthly_exceptions_date_3 == "'" + "2022-05-31" + "'"
        (
            periods_4,
            daily_exceptions_date_4,
            monthly_exceptions_date_4,
        ) = fund_valuation_etl.run_for(":2022-6-4", "N")
        assert periods_4 == [
            ("2022-6-4", "D"),
            ("2022-05-31", "M"),
        ]
        assert daily_exceptions_date_4 == "'" + "2022-6-4" + "'"
        assert monthly_exceptions_date_4 == "'" + "2022-05-31" + "'"
        todays_date = datetime.now()
        todays_date_str = todays_date.strftime(":%Y-%m-%d")
        if (
            todays_date.day <= 4
            or calendar.monthrange(todays_date.year, todays_date.month)[1]
            == todays_date.day
        ):
            (
                periods_5,
                daily_exceptions_date_5,
                monthly_exceptions_date_5,
            ) = fund_valuation_etl.run_for(todays_date_str, "N")
            print(
                "###  periods_5: {0} daily_exception_date: {1} monthly_exception_Date {2}".format(
                    periods_5, daily_exceptions_date_5, monthly_exceptions_date_5
                )
            )
            # assert periods_5 == [
            # (todays_date_str, "D"),
            # ]
            # assert daily_exceptions_date_5 == todays_date_str
            # assert monthly_exceptions_date_5 == None
        else:
            (
                periods_5,
                daily_exceptions_date_5,
                monthly_exceptions_date_5,
            ) = fund_valuation_etl.run_for(todays_date_str, "N")
            # assert periods_5 == [(todays_date_str, "D")]
            # assert daily_exceptions_date_5 == todays_date_str
            # assert monthly_exceptions_date_5 == None
            print(
                "###  periods_5: {0} daily_exception_date: {1} monthly_exception_Date {2}".format(
                    periods_5, daily_exceptions_date_5, monthly_exceptions_date_5
                )
            )

    def test_extract(self):
        with open(f"{TEST_DATA_DIR}/port-ids-share-class.json", "r") as f:
            share_class_port_ids = json.load(f)
        share_class_params = ShareClassParams(
            effective_date="2020-12-31",
            time_period_code="M",
            asset_type_codes="AUM,TNA",
            identifier_type="PORT",
            port_ids=share_class_port_ids,
            currencyCodes="USD",
        )
        with open(f"{TEST_DATA_DIR}/advisors-api-response.json", "r") as f:
            api_response = json.load(f)

        class MockResponse:
            def __init__(self, json_data, status_code):
                self.json_data = json_data
                self.status_code = status_code

            def json(self):
                return self.json_data

        fund_valuation_etl.requests.get = MagicMock(
            return_value=MockResponse(
                api_response,
                200,
            )
        )

        fund_valuation_etl.prepare_url = MagicMock(
            return_value="https://valuation-analytics.polaris.gise.c1.vanguard.com/fund-managed-assets?timePeriodCode=M&effectiveDate=2020-12-31&limit=100&offset=0&portIds=0807%2C4181%2C0730%2C0970%2C0093%2C4158%2C0094%2C0101%2C0102%2C0103%2C1120%2C1677%2C1142%2C1143%2C0114%2C0115%2C0116%2C0118%2C0119%2C0122&assetTypeCodes=AUM%2CTNA&identifierType=PORT&currencyCodes=USD"
        )

        headers = MagicMock()
        records = fund_valuation_etl.extract(share_class_params, headers)
        print(str(records))
        outstanding_shares_record = fund_valuation_etl.DBRecord(
            effective_date=datetime(2021, 6, 30).date(),
            time_period_code="M",
            port_id="AAAV",
            currency="USD",
            TNA=Decimal("16413919219.59000"),
            NAV=Decimal("89.54660000"),
            AUM=None,
            outstanding_shares=Decimal("183300278.86"),
        )
        tna_record = fund_valuation_etl.DBRecord(
            effective_date=datetime(2021, 6, 30).date(),
            time_period_code="M",
            port_id="ABDJ",
            currency="USD",
            TNA=Decimal("59587012.32"),
            NAV=Decimal("5.28320000"),
            AUM=None,
            outstanding_shares=Decimal("11278653.86400"),
        )
        assert outstanding_shares_record in records
        assert tna_record in records

    def test_transform(self):
        spark = self.__class__._spark
        tna_nav_aum_df = spark.read.csv(
            f"{TEST_DATA_DIR}/test_transfom_tna_nav_aum.csv", header=True
        )
        tna_nav_aum_df.show()
        outstanding_shares_df = spark.read.csv(
            f"{TEST_DATA_DIR}/test_transform_outstanding_shares.csv", header=True
        )
        outstanding_shares_df.show()
        df = fund_valuation_etl.transform(tna_nav_aum_df, outstanding_shares_df)
        df = df.sort("port_id", "effective_date", "time_period_code", "currency")
        df.show()

        assert df.first().port_id == "AAAA"
        assert df.first().time_period_code == "D"
        assert df.first().currency == "ASD"
        assert df.first().TNA == "200"
        assert df.first().NAV == "20000"
        assert df.first().AUM == "200000"
        assert df.first().outstanding_shares == "2000000"
        assert df.first().effective_date == "5/30/2022"
        # df.coalesce(1).write.option("header", True).csv(
        #     f"{TEST_DATA_DIR}/test_data_quality_checks.csv"
        # )

    def test_primary_data_quality_checks_currency_null(self):
        spark = self.__class__._spark
        fund_valuation_df = spark.read.csv(
            f"{TEST_DATA_DIR}/test_data_quality_currency_null.csv", header=True
        )
        print(fund_valuation_df.count())
        fund_valuation_df.show()

        (
            fund_valuation_df,
            fund_valuation_exception_df,
        ) = fund_valuation_etl.run_primary_data_quality_checks(fund_valuation_df)
        fund_valuation_df.show()
        fund_valuation_exception_df.show()
        fund_valuation_df = fund_valuation_df.filter(
            fund_valuation_df.currency.isNull()
        )
        assert fund_valuation_df.count() == 0
        assert fund_valuation_exception_df.count() == 1

    def test_data_quality_checks_all(self):
        spark = self.__class__._spark
        fund_valuation_df = spark.read.csv(
            f"{TEST_DATA_DIR}/test_data_quality_all.csv", header=True
        )
        print(fund_valuation_df.count())
        fund_valuation_df.show()
        fund_valuation_count = fund_valuation_df.count()
        (
            fund_valuation_df,
            fund_valuation_exception_df,
        ) = fund_valuation_etl.run_primary_data_quality_checks(fund_valuation_df)
        # there is one row with null currency.
        assert fund_valuation_exception_df.count() == 1
        assert fund_valuation_df.count() == 23
        fund_valuation_null_currency_df = fund_valuation_df.filter(
            fund_valuation_df.currency.isNull()
        )
        assert fund_valuation_null_currency_df.count() == 0

        # testing secondary data quality checks
        (
            temp_fund_valuation_df,
            temp_fund_valuation_exception_df,
        ) = fund_valuation_etl.run_data_quality_checks(
            fund_valuation_df,
            active_port_ids=["ACTIVE", "ACTIVE_USD"],
            usd_base_currency_active_port_ids=["ACTIVE_USD"],
            active_parent_port_ids=["ACTIVEPARENT"],
            multi_advised_port_ids=["MULTIADVISED"],
        )
        temp_fund_valuation_df.show()
        temp_fund_valuation_exception_df.show()
        assert temp_fund_valuation_df.count() == 13
        assert temp_fund_valuation_exception_df.count() == 10


if __name__ == "__main__":
    unittest.main()
