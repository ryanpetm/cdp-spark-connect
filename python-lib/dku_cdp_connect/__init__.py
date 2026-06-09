import logging
import os
import shutil
import tempfile

import dataiku
from dataiku.base.spark_like import SparkLike
from dataiku.base.sql_dialect import SparkLikeDialect
from dku_cdp_connect.utils import resolve_from_dss_secrets

# PySpark — CDE-provided, version-matched to the cluster's Spark runtime.
# Install in code-env: pip install /path/to/pyspark-<cde-version>.tar.gz
try:
    from pyspark.sql import SparkSession  # noqa: F401
    from pyspark.sql.functions import col, lit, date_format, to_json, unhex
    from pyspark.sql.functions import base64 as spark_base64
    _PYSPARK_AVAILABLE = True
except ImportError:
    _PYSPARK_AVAILABLE = False

# CDE Python package — provides CDESparkConnectSession.
# Install in code-env: pip install /path/to/cdeconnect-<cdeversion>.tar.gz
try:
    from cde import CDESparkConnectSession
    _CDE_NATIVE_AVAILABLE = True
except ImportError:
    _CDE_NATIVE_AVAILABLE = False


class DkuCDPConnectDialect(SparkLikeDialect):
    """Spark SQL dialect for CDP Spark Connect."""

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
    Handle to create CDP Spark Connect sessions from DSS connections.

    Requires two CDE-provided tarballs installed in the code-env:
        pip install /path/to/pyspark-<version>-cde.tar.gz
        pip install /path/to/cde-<version>.tar.gz

    DSS connection params:
        host             - CDE Virtual Cluster endpoint URL (vcluster-endpoint)
        cdpEndpoint      - CDP control plane URL (default: https://console.us-west-1.cdp.cloudera.com)
        cdpAccessKeyId   - CDP access key id
        cdpPrivateKey    - CDP private key
        sessionName      - optional; defaults to 'dss-<connectionName>'
        db               - optional default database/schema
        defaultCatalog   - optional default catalog
    """

    def __init__(self):
        SparkLike.__init__(self)
        self._dialect = DkuCDPConnectDialect()
        self._connection_type = "SparkConnect"

    # -------------------------------------------------------------------------
    # Internal: guard helpers
    # -------------------------------------------------------------------------

    def _require_pyspark(self):
        if not _PYSPARK_AVAILABLE:
            raise Exception(
                "PySpark is not installed. Install the CDE-provided PySpark tarball "
                "(pyspark-<version>-cde.tar.gz) in your code-env:\n"
                "  pip install /path/to/pyspark-<version>-cde.tar.gz"
            )

    def _require_cde_package(self):
        if not _CDE_NATIVE_AVAILABLE:
            raise Exception(
                "The CDE Python package is not installed. Install the CDE-provided tarball "
                "(cde-<version>.tar.gz) in your code-env:\n"
                "  pip install /path/to/cde-<version>.tar.gz"
            )

    # -------------------------------------------------------------------------
    # Session factory
    # -------------------------------------------------------------------------


    def _create_session(self, connection_name, connection_info, project_key=None):
        self._require_pyspark()
        self._require_cde_package()

        connection_params = connection_info["resolvedParams"]
        #  fill missing params from DSS user secrets before use
        connection_params = resolve_from_dss_secrets(connection_params)

        cdp_endpoint      = connection_params.get("cdpEndpoint", "https://console.us-west-1.cdp.cloudera.com")
        vcluster_endpoint = connection_params["host"]
        key_id            = connection_params["cdpAccessKeyId"]
        private_key       = connection_params["cdpPrivateKey"]
        session_name      = connection_params.get("sessionName") or "dss-{}".format(connection_name)

        # Write temp CDE config files from DSS connection params.
        # Both files are deleted immediately after CDESparkConnectSession.get()
        # returns — the session is fully authenticated by that point.
        tmp_dir = tempfile.mkdtemp(prefix="dss_cde_")
        try:
            creds_path  = os.path.join(tmp_dir, "credentials")
            config_path = os.path.join(tmp_dir, "config.yaml")

            with open(creds_path, "w") as f:
                f.write("[default]\n")
                f.write("cdp_access_key_id = {}\n".format(key_id))
                f.write("cdp_private_key = {}\n".format(private_key))

            with open(config_path, "w") as f:
                f.write("cdp-endpoint: {}\n".format(cdp_endpoint))
                f.write("credentials-file: {}\n".format(creds_path))
                f.write("vcluster-endpoint: {}\n".format(vcluster_endpoint))

            logging.info("Creating/reusing CDE Spark Connect session: %s", session_name)
            session = (
                CDESparkConnectSession.builder
                .sessionName(session_name)
                .cdeConfigLocation(config_path)
                .get()
            )
        finally:
            shutil.rmtree(tmp_dir, ignore_errors=True)

        for statement in connection_params.get("postConnectStatementsExpandedAndSplit", []):
            logging.info("Executing post-connect statement: %s", statement)
            session.sql(statement).show()
            logging.info("Post-connect statement done")

        session.dss_connection_name = connection_name
        return session

    # -------------------------------------------------------------------------
    # DataFrame helpers
    # -------------------------------------------------------------------------

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

    def _get_table_schema(self, schema, connection_params):
        if schema and schema.strip():
            return schema
        return self._get_connection_param(connection_params, "db", "db")

    def _get_table_catalog(self, catalog, connection_params):
        if catalog and catalog.strip():
            return catalog
        return self._get_connection_param(connection_params, "defaultCatalog", "catalog")