import logging

import dataiku
from dataiku.base.spark_like import SparkLike
from dataiku.base.sql_dialect import SparkLikeDialect

try:
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, lit, date_format, to_json, unhex
    from pyspark.sql.functions import base64 as spark_base64  # avoid shadowing stdlib base64
except ImportError as e:
    raise Exception(
        "Unable to import PySpark libraries. "
        "Make sure pyspark>=3.4 is installed in your code-env. Cause: " + str(e)
    )

from .utils import build_spark_connect_url, parse_spark_connect_endpoint


class DkuCDPConnectDialect(SparkLikeDialect):
    """
    Spark SQL dialect for CDP Spark Connect.
    Mirrors DkuDBConnectDialect from dbconnect.py.
    """

    def __init__(self):
        SparkLikeDialect.__init__(self)

    def _get_to_dss_types_map(self):
        if self._to_dss_types_map is None:
            self._to_dss_types_map = {
                'ArrayType':           'string',
                'BinaryType':          'string',
                'BooleanType':         'boolean',
                'ByteType':            'tinyint',
                'DateType':            'dateonly',
                'DayTimeIntervalType': 'string',
                'DecimalType':         'double',
                'DoubleType':          'double',
                'FloatType':           'float',
                'IntegerType':         'int',
                'LongType':            'bigint',
                'MapType':             'string',
                'NullType':            'string',
                'ShortType':           'smallint',
                'StringType':          'string',
                'StructType':          'string',
                'TimestampNTZType':    'datetimenotz',
                'TimestampType':       'date',
                'UserDefinedType':     'string',
            }
        return self._to_dss_types_map

    def allow_empty_schema_after_catalog(self):
        return False

    def identifier_quote_char(self):
        return '`'

    def _column_name_to_sql_column(self, identifier):
        return col(self.quote_identifier(identifier))

    def _python_literal_to_sql_literal(self, value, column_type, original_type=None):
        if original_type is not None and original_type.lower() == 'binary':
            # value is hex string from DSS UI; unhex it to match binary column
            return unhex(lit(value))
        return lit(value)

    def _get_components_from_df_schema(self, df_schema):
        fields = {}
        for field in df_schema.fields:
            col_name = self.unquote_identifier(field.name)
            fields[col_name] = {"name": col_name, "datatype": field.dataType}
        return (df_schema.names, fields)

    def _get_datatype_name_from_df_datatype(self, datatype):
        return datatype.__class__.__name__


class DkuCDPConnect(SparkLike):
    """
    Handle to create CDP Spark Connect sessions from DSS datasets or connections.
    Mirrors DkuDBConnect(SparkLike) for Cloudera Data Platform (CDP).

    Requires pyspark>=3.4 in the code-env (Spark Connect client support).

    Supported authType values in the DSS connection:
        - PERSONAL_ACCESS_TOKEN  (Knox token or CDP API access token)
        - BASIC                  (CDP workload username + password)
    """

    def __init__(self):
        SparkLike.__init__(self)
        self._dialect = DkuCDPConnectDialect()
        self._connection_type = "SparkConnect"

    # --- Mirrors DkuDBConnect._get_config_for_personal_access_token ---
    def _build_url_for_token(self, host, port, token, use_ssl=True):
        """Build sc:// URL using a CDP Knox / workload token."""
        return build_spark_connect_url(host, port, token=token, use_ssl=use_ssl)

    # --- Mirrors DkuDBConnect._get_config_for_personal_access_token (basic variant) ---
    def _build_url_for_basic_auth(self, host, port, username, password, use_ssl=True):
        """Build sc:// URL passing CDP workload credentials as URL params."""
        return build_spark_connect_url(
            host, port,
            use_ssl=use_ssl,
            extra_params={"user": username, "password": password}
        )

    # --- Mirrors DkuDBConnect._create_session ---
    def _create_session(self, connection_name, connection_info, project_key=None):
        connection_params = connection_info["resolvedParams"]

        host    = connection_params["host"]
        port    = int(connection_params.get("port", 15002))
        use_ssl = connection_params.get("useSSL", True)
        auth_type = connection_params.get("authType", "PERSONAL_ACCESS_TOKEN")

        if auth_type == "PERSONAL_ACCESS_TOKEN":
            resolved = connection_info.get("resolvedBasicCredential")
            token = (
                resolved["password"]
                if resolved is not None
                else connection_params.get("pwd") or connection_params.get("password")
            )
            if not token:
                raise Exception("Cannot find access token in connection settings")
            sc_url = self._build_url_for_token(host, port, token, use_ssl=use_ssl)

        elif auth_type == "BASIC":
            resolved = connection_info.get("resolvedBasicCredential")
            if resolved is not None:
                username = resolved.get("user") or resolved.get("username")
                password = resolved["password"]
            else:
                username = connection_params.get("user")
                password = connection_params.get("pwd") or connection_params.get("password")
            if not username or not password:
                raise Exception("Cannot find username/password in connection settings")
            sc_url = self._build_url_for_basic_auth(host, port, username, password, use_ssl=use_ssl)

        else:
            raise Exception("Auth type not supported: " + auth_type)

        logging.info("Connecting to CDP Spark Connect: %s:%d", host, port)
        session = SparkSession.builder.remote(sc_url).getOrCreate()

        # Execute post-connect statements if any — mirrors DkuDBConnect._create_session
        for statement in connection_params.get("postConnectStatementsExpandedAndSplit", []):
            logging.info("Executing post-connect statement: %s", statement)
            session.sql(statement).show()
            logging.info("Post-connect statement done")

        session.dss_connection_name = connection_name  # dynamic attribute, mirrors DkuDBConnect
        return session

    # --- Mirrors DkuDBConnect._cast_to_target_types ---
    def _cast_to_target_types(self, df, dss_schema, qualified_table_id):
        column_names, column_fields = self._dialect._get_components_from_df_schema(df.schema)
        try:
            tdf = df.sparkSession.sql("SELECT * FROM %s" % qualified_table_id)
            _, target_fields = self._dialect._get_components_from_df_schema(tdf.schema)
            for column_name in column_names:
                field  = column_fields[column_name]
                target = target_fields.get(column_name)
                if target is None:
                    continue
                src = self._dialect._get_datatype_name_from_df_datatype(field["datatype"])
                tgt = self._dialect._get_datatype_name_from_df_datatype(target["datatype"])

                if src == 'BinaryType' and tgt == 'StringType':
                    df = df.withColumn(column_name, spark_base64(col(column_name)))
                if src in ('TimestampType', 'TimestampNTZType') and tgt == 'StringType':
                    df = df.withColumn(column_name, date_format(col(column_name), lit('yyyy-MM-dd HH:mm:ss.SSS')))
                if src == 'DateType' and tgt == 'StringType':
                    df = df.withColumn(column_name, date_format(col(column_name), lit('yyyy-MM-dd')))
                if src == 'DayTimeIntervalType' and tgt == 'StringType':
                    df = df.withColumn(column_name, col(column_name).cast("string"))
                if src in ('ArrayType', 'MapType', 'StructType') and tgt == 'StringType':
                    df = df.withColumn(column_name, to_json(col(column_name)))
                if src == 'DecimalType' and tgt == 'DoubleType':
                    df = df.withColumn(column_name, col(column_name).cast('double'))
                if src in ('ByteType', 'ShortType', 'IntegerType') and tgt == 'LongType':
                    df = df.withColumn(column_name, col(column_name).cast('bigint'))
                if src in ('ByteType', 'ShortType') and tgt == 'IntegerType':
                    df = df.withColumn(column_name, col(column_name).cast('int'))
                if src == 'ByteType' and tgt == 'ShortType':
                    df = df.withColumn(column_name, col(column_name).cast('short'))
                if src == 'StringType' and tgt == 'TimestampType':
                    df = df.withColumn(column_name, col(column_name).cast('timestamp'))
        except Exception as e:
            logging.warning("Unable to check output schema, inserting as is: %s", str(e))
        return df

    def _check_dataframe_type(self, df):
        if not df.__class__.__module__.startswith("pyspark."):
            raise ValueError(
                "Dataframe is not a PySpark Connect dataframe. "
                "Use dataset.write_dataframe() instead."
            )

    def _do_with_column(self, df, column_name, column_value):
        return df.withColumn(column_name, column_value)

    # --- Mirrors DkuDBConnect._get_table_schema / _get_table_catalog ---
    def _get_table_schema(self, schema, connection_params):
        if schema and schema.strip():
            return schema
        return self._get_connection_param(connection_params, "db", "db")

    def _get_table_catalog(self, catalog, connection_params):
        if catalog and catalog.strip():
            return catalog
        return self._get_connection_param(connection_params, "defaultCatalog", "catalog")