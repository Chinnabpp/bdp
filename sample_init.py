from os.path import abspath, dirname, join
from sys import modules, path
from unittest.mock import Mock

try:
    from findspark import init
except ModuleNotFoundError:
    pass
else:
    init()
modules["utilities"] = Mock()
modules["awsglue.utils"] = Mock()
modules["awsglue.utils"].getResolvedOptions.return_value = {
    "env": "eng",
    "trigger_key": "test/eod/test.avro",
}
modules["awsglue.transforms"] = Mock()
modules["awsglue.dynamicframe"] = Mock()
modules["awsglue.context"] = Mock()
modules["awsglue.job"] = Mock()
modules["logging"] = Mock()
modules["pg8000"] = Mock()
modules["pg8000.dbapi"] = Mock()
