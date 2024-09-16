from pyspark.sql import SparkSession

from ops.utils.databricks_utils import DatabricksUtils


class JDBCUtils:
    @classmethod
    def get_database_credentials(cls, spark: SparkSession, df_options: dict, platform: str):
        try:
            return cls.get_local_credentials_from_config(df_options) if platform == "local" else \
                cls.get_jdbc_credentials_from_scope(spark, df_options) if (platform == "databricks") else None
        except Exception as ex:
            raise ex

    @classmethod
    def get_local_credentials_from_config(cls, df_options: dict):
        try:
            return {"user": df_options["user"], "password": df_options["password"]}
        except Exception as ex:
            raise ex

    @classmethod
    def get_jdbc_credentials_from_scope(cls, spark, df_options: dict):
        try:
            scope_name = df_options["scope_name"]
            dbutils = DatabricksUtils().get_db_utils(spark)
            user_name = dbutils.secrets.get(scope=scope_name, key="user_name")
            password = dbutils.secrets.get(scope=scope_name, key="password")
            return {"user_name": user_name, "password": password}
        except Exception as ex:
            raise ex
