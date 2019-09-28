import os

from airtunnel.assetio import BaseDataAssetIO, PandasDataAssetIO
from airtunnel.data_asset import BaseDataAsset, SQLDataAsset, PandasDataAsset

if "ENV" in os.environ:
    # env is set by the environment.
    env = os.environ["ENV"].strip()
else:
    # default env: only production
    env = ""

if env == "":
    DAG_PREFIX = "prod_d_"
    TASK_PREFIX = "prod_t_"
    SCHEMA_PREFIX = ""
elif env == "DEV":
    DAG_PREFIX = "dev_d_"
    TASK_PREFIX = "dev_t_"
    SCHEMA_PREFIX = "DEV_"
else:
    raise NotImplementedError("Extend code to cover environment %s" % env)

__all__ = [
    env,
    DAG_PREFIX,
    TASK_PREFIX,
    SCHEMA_PREFIX,
    BaseDataAsset,
    SQLDataAsset,
    PandasDataAsset,
    BaseDataAssetIO,
    PandasDataAssetIO,
]
