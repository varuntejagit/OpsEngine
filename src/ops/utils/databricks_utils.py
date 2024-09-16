"""Utilities for databricks operations."""

import json
from typing import Any, Tuple

from pyspark.sql import SparkSession


class DatabricksUtils(object):
    """Databricks utilities class."""

    @staticmethod
    def get_db_utils(spark: SparkSession) -> Any:
        """Get db utils on databricks.

        Args:
            spark: spark session.

        Returns:
            Dbutils from databricks.
        """
        dbutils = None
        try:
            if spark.conf.get("spark.databricks.service.client.enabled") == "true":
                from pyspark.dbutils import DBUtils

                if "dbutils" not in locals():
                    dbutils = DBUtils(spark)
            else:
                dbutils = locals().get("dbutils")
        except ImportError:
            import IPython
            dbutils = IPython.get_ipython().user_ns["dbutils"]

        return dbutils

    @staticmethod
    def get_scope_object(dbutils, scope_name):
        try:
            connection_string = dbutils.secrets.get(scope=scope_name, key="connection_string")
            user_name = dbutils.secrets.get(scope=scope_name, key="user_name")
            password = dbutils.secrets.get(scope=scope_name, key="password")
            return {"connection_string": connection_string, user_name: user_name, password: password}
        except Exception as ex:
            raise ex

    @staticmethod
    def get_databricks_job_information(spark: SparkSession) -> Tuple[str, str]:
        """Get notebook context from running acon.

        Args:
            spark: spark session.

        Returns:
            Dict containing databricks notebook context.
        """
        if "local" in spark.getActiveSession().conf.get("spark.app.id"):
            return "local", "local"
        else:
            dbutils = DatabricksUtils.get_db_utils(spark)
            notebook_context = json.loads(
                (
                    dbutils.notebook.entry_point.getDbutils()
                        .notebook()
                        .getContext()
                        .safeToJson()
                )
            )

            return notebook_context["attributes"].get("orgId"), notebook_context[
                "attributes"
            ].get("jobName")
