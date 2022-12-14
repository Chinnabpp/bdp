import os
from unittest.mock import Mock

import pytest

from etl.scripts import maa_rimes_etl

dirname = os.path.dirname
TEST_DATA_DIR = os.path.join(dirname(os.path.abspath(__file__)), "resources")
FUND_JSON_FILE = os.path.join(TEST_DATA_DIR, "fund_test.json")


def test_transform_happy_path(spark, spark_context, logger):
    maa_rimes_etl.logger = logger
    maa_rimes_etl.env = "local"
    maa_rimes_etl.sc = spark_context
    maa_rimes_etl.spark = spark

    rimes_df = spark.read.option("multiline", True).json(
        os.path.join(TEST_DATA_DIR, "rimes_data_happy_path.json")
    )
    security_identifier_df = spark.read.csv(
        os.path.join(TEST_DATA_DIR, "security_identifier_happy_path.csv"),
        header=True,
    )
    security_identifier_histo_df = Mock()

    transformed_df = maa_rimes_etl.transform(
        rimes_df,
        security_identifier_df,
        security_identifier_histo_df,
        "staging_identifier",
    )

    transformed_df_dict = transformed_df.toPandas().to_dict()
    assert transformed_df.count() == 10
    assert transformed_df_dict["msec_id"][0] == "NX_V220"
    assert transformed_df_dict["exchange"][0] == "XNYS"
    assert transformed_df_dict["provider_weight"][0] == 0.000574174220494225
    assert transformed_df_dict["weight"][0] == 0.000574174220494225
    assert transformed_df_dict["index_id"][0] == "RU1000GGRUSD0"
    assert transformed_df_dict["msec_id"][0] == "NX_V220"
    assert transformed_df_dict["number_of_shares"][0] == 27829000.0
    assert transformed_df_dict["publication_type"][0] == "EOD"
    assert transformed_df_dict["file_key"][0] == "staging_identifier"
    assert transformed_df_dict["number_of_shares"][0] == 27829000.0
    # assert(transformed_df_dict['effective_date'][0] == datetime.datetime(2021,9,3))


def test_transform_happy_path_with_nasdaq(spark, spark_context, logger):
    maa_rimes_etl.logger = logger
    maa_rimes_etl.env = "local"
    maa_rimes_etl.sc = spark_context
    maa_rimes_etl.spark = spark

    rimes_df = spark.read.option("multiline", True).json(
        os.path.join(TEST_DATA_DIR, "rimes_data_happy_path_nasdaq.json")
    )
    rimes_df.show()
    security_identifier_df = spark.read.csv(
        os.path.join(TEST_DATA_DIR, "security_identifier_happy_path_nasdaq.csv"),
        header=True,
    )
    security_identifier_df.show()
    security_identifier_histo_df = Mock()

    transformed_df = maa_rimes_etl.transform(
        rimes_df,
        security_identifier_df,
        security_identifier_histo_df,
        "staging_identifier",
    )

    transformed_df_dict = transformed_df.toPandas().to_dict()
    assert transformed_df.count() == 10


def test_transform_missing_msec_ids(spark, spark_context, logger):
    maa_rimes_etl.logger = logger
    maa_rimes_etl.env = "local"
    maa_rimes_etl.sc = spark_context
    maa_rimes_etl.spark = spark

    rimes_df = spark.read.option("multiline", True).json(
        os.path.join(TEST_DATA_DIR, "rimes_data_happy_path_nasdaq.json")
    )
    rimes_df.show()
    security_identifier_df = spark.read.csv(
        os.path.join(TEST_DATA_DIR, "security_identifier_missing_msec_nasdaq.csv"),
        header=True,
    )
    security_identifier_df.show()
    security_identifier_histo_df = Mock()
    with pytest.raises(ValueError):
        transformed_df = maa_rimes_etl.transform(
            rimes_df,
            security_identifier_df,
            security_identifier_histo_df,
            "staging_identifier",
        )


def test_transform_duplicate_security_combinations(spark, spark_context, logger):
    maa_rimes_etl.logger = logger
    maa_rimes_etl.env = "local"
    maa_rimes_etl.sc = spark_context
    maa_rimes_etl.spark = spark

    rimes_df = spark.read.option("multiline", True).json(
        os.path.join(TEST_DATA_DIR, "rimes_data_happy_path_nasdaq.json")
    )
    rimes_df.show()
    security_identifier_df = spark.read.csv(
        os.path.join(TEST_DATA_DIR, "security_identifier_duplicates_nasdaq.csv"),
        header=True,
    )
    security_identifier_df.show()
    security_identifier_histo_df = spark.read.csv(
        os.path.join(TEST_DATA_DIR, "security_identifier_histo.csv"),
        header=True,
    )
    security_identifier_histo_df.show()
    transformed_df = maa_rimes_etl.transform(
        rimes_df,
        security_identifier_df,
        security_identifier_histo_df,
        "staging_identifier",
    )
    assert transformed_df.count() == 10


def test_data_quality_check_weight_sum_eod_happy_path(spark, spark_context, logger):
    maa_rimes_etl.logger = logger
    maa_rimes_etl.env = "local"
    maa_rimes_etl.sc = spark_context
    maa_rimes_etl.spark = spark

    transformed_rimes_df = spark.read.csv(
        os.path.join(TEST_DATA_DIR, "data_quality_weight_sum_happy_path_EOD.csv"),
        header=True,
    )

    maa_rimes_etl.run_data_quality_checks(transformed_rimes_df)


def test_data_quality_check_weight_sum_eod_error(spark, spark_context, logger):
    maa_rimes_etl.logger = logger
    maa_rimes_etl.env = "local"
    maa_rimes_etl.sc = spark_context
    maa_rimes_etl.spark = spark

    transformed_rimes_df = spark.read.csv(
        os.path.join(TEST_DATA_DIR, "data_quality_weight_sum_error_EOD.csv"),
        header=True,
    )

    with pytest.raises(ValueError):
        maa_rimes_etl.run_data_quality_checks(transformed_rimes_df)


def test_data_quality_check_weight_sum_sod_happy_path(spark, spark_context, logger):
    maa_rimes_etl.logger = logger
    maa_rimes_etl.env = "local"
    maa_rimes_etl.sc = spark_context
    maa_rimes_etl.spark = spark

    transformed_rimes_df = spark.read.csv(
        os.path.join(TEST_DATA_DIR, "data_quality_weight_sum_happy_path_SOD.csv"),
        header=True,
    )

    maa_rimes_etl.run_data_quality_checks(transformed_rimes_df)


def test_data_quality_check_weight_sum_sod_error(spark, spark_context, logger):
    maa_rimes_etl.logger = logger
    maa_rimes_etl.env = "local"
    maa_rimes_etl.sc = spark_context
    maa_rimes_etl.spark = spark

    transformed_rimes_df = spark.read.csv(
        os.path.join(TEST_DATA_DIR, "data_quality_weight_sum_error_SOD.csv"),
        header=True,
    )

    with pytest.raises(ValueError):
        maa_rimes_etl.run_data_quality_checks(transformed_rimes_df)
