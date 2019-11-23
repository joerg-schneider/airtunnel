""" Airtunnel operators for transformation tasks. """

import logging

from airflow.operators import BaseOperator
from airflow.utils.decorators import apply_defaults

import airtunnel
import airtunnel.operators
from airtunnel.data_asset import PandasDataAsset
from airtunnel.data_asset import PySparkDataAsset
from airtunnel.data_asset import SQLDataAsset

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class PandasTransformationOperator(BaseOperator):
    """
    Airtunnel's transformation operator for PandasDataAssets, calling ``rebuild_for_store()`` on them.
    """

    ui_color = airtunnel.operators.Colours.transformation

    @apply_defaults
    def __init__(self, asset: PandasDataAsset, *args, **kwargs):
        self.asset = asset

        if "task_id" not in kwargs:
            kwargs["task_id"] = asset.name + "_" + "transform"

        super().__init__(*args, **kwargs)

    def execute(self, context):
        """ Execute this operator using Airflow. """
        self.asset.rebuild_for_store(airflow_context=context)


class PySparkTransformationOperator(BaseOperator):
    """
    Airtunnel's transformation operator for PySparkDataAssets, calling ``rebuild_for_store()`` on them.
    """

    ui_color = airtunnel.operators.Colours.transformation

    @apply_defaults
    def __init__(self, asset: PySparkDataAsset, *args, **kwargs):
        self.asset = asset

        if "task_id" not in kwargs:
            kwargs["task_id"] = asset.name + "_" + "transform"

        super().__init__(*args, **kwargs)

    def execute(self, context):
        """ Execute this operator using Airflow. """
        self.asset.rebuild_for_store(airflow_context=context)


class SQLTransformationOperator(BaseOperator):
    """
    Airtunnel's transformation operator for SQLDataAssets, calling ``rebuild_for_store()`` on them.

    Can be customized with static and dynamic parameters from the DAG definition/creation time, i.e. using the
    operator's constructor.

    """

    ui_color = airtunnel.operators.Colours.transformation

    @apply_defaults
    def __init__(
        self,
        asset: SQLDataAsset,
        parameters: dict = None,
        dynamic_parameters: callable([[None], dict]) = None,
        *args,
        **kwargs
    ):
        self.asset = asset

        self._parameters = {} if parameters is None else parameters
        self._dynamic_parameters = dynamic_parameters

        if "task_id" not in kwargs:
            kwargs["task_id"] = asset.name + "_" + "transform"

        super().__init__(*args, **kwargs)

    def execute(self, context):
        """ Execute this operator using Airflow. """
        self.asset.rebuild_for_store(
            airflow_context=context,
            parameters=self._parameters,
            dynamic_parameters=self._dynamic_parameters,
        )
