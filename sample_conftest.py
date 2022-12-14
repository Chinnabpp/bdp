import logging
import sys
from logging import getLogger

from pytest import fixture

from pyspark import SparkContext
from pyspark.sql import SparkSession, SQLContext

LOGGING_WARN = 30


@fixture(scope="session")
def spark_context():
    # -- init --
    logger = getLogger("py4j")
    original_effective_level = logger.getEffectiveLevel()

    # -- enter --
    logger.setLevel(LOGGING_WARN)
    sc = SparkContext.getOrCreate()
    yield sc

    # -- exit --
    sc.stop()
    logger.setLevel(original_effective_level)


@fixture
def spark(spark_context):
    yield SparkSession(spark_context)


@fixture
def sql(spark_context):
    yield SQLContext(spark_context)


@fixture
def logger():
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler(stream=sys.stdout)
    handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    yield logger
