import json
import logging
import subprocess
import time

import dataiku  # noqa: F401 — provided by DSS runtime; not in local dev env
from dataiku.base.spark_like import SparkLike
from dataiku.base.sql_dialect import SparkLikeDialect

# Tarball 1: PySpark — CDE-provided, version-matched to the cluster's Spark runtime.
# Install in code-env: pip install /path/to/pyspark-<version>-cde.tar.gz
try:
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, lit, date_format, to_json, unhex
    from pyspark.sql.functions import base64 as spark_base64
    _PYSPARK_AVAILABLE = True
except ImportError:
    _PYSPARK_AVAILABLE = False

# Tarball 2: CDE Python package — separate tarball from CDE, provides CDESparkConnectSession.
# Install in code-env: pip install /path/to/cde-<version>.tar.gz
# This is independent of the PySpark tarball above.
try:
    from cde import CDESparkConnectSession
    _CDE_NATIVE_AVAILABLE = True
except ImportError:
    _CDE_NATIVE_AVAILABLE = False


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

    CDE supplies two separate tarballs that must both be installed in the code-env:
        1. pyspark-<version>-cde.tar.gz  — version-matched PySpark client
        2. cde-<version>.tar.gz          — CDE Python package (CDESparkConnectSession)

    Supported authType values in the DSS connection:
        - PERSONAL_ACCESS_TOKEN  (Knox token or CDP API access token)
          Requires: pyspark tarball only
        - BASIC                  (CDP workload username + password)
          Requires: pyspark tarball only
        - CDE_SESSION            (CDE-managed session via CDESparkConnectSession)
          Requires: BOTH tarballs
        - CDE_CLI                (CDE-managed session via 'cde' CLI binary)
          Requires: pyspark tarball + cde CLI on PATH
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
                "  pip install /path/to/pyspark-<version>-cde.tar.gz\n"
                "The PySpark tarball is separate from the CDE Python package tarball."
            )

    def _require_cde_package(self):
        if not _CDE_NATIVE_AVAILABLE:
            raise Exception(
                "The CDE Python package is not installed. Install the CDE-provided tarball "
                "(cde-<version>.tar.gz) in your code-env:\n"
                "  pip install /path/to/cde-<version>.tar.gz\n"
                "Note: the CDE package tarball is separate from the PySpark tarball. "
                "Both must be installed for authType=CDE_SESSION."
            )

    # -------------------------------------------------------------------------
    # URL builders (PERSONAL_ACCESS_TOKEN / BASIC)
    # -------------------------------------------------------------------------

    def _build_url_for_token(self, host, port, token, use_ssl=True):
        """Build sc:// URL using a CDP Knox / workload token."""
        return build_spark_connect_url(host, port, token=token, use_ssl=use_ssl)

    def _build_url_for_basic_auth(self, host, port, username, password, use_ssl=True):
        """Build sc:// URL passing CDP workload credentials as URL params."""
        return build_spark_connect_url(
            host, port,
            use_ssl=use_ssl,
            extra_params={"user": username, "password": password}
        )

    # -------------------------------------------------------------------------
    # CDE CLI helpers (CDE_CLI auth type)
    # -------------------------------------------------------------------------

    def _run_cde_cli(self, args, vcluster_endpoint=None, check=True):
        """Run a cde CLI command and return parsed JSON or raw stdout."""
        cmd = ["cde"] + args
        if vcluster_endpoint:
            cmd += ["--vcluster-endpoint", vcluster_endpoint]
        logging.debug("CDE CLI: %s", " ".join(cmd))
        result = subprocess.run(cmd, capture_output=True, text=True, check=check)
        if result.returncode != 0:
            raise Exception("CDE CLI failed: " + result.stderr.strip())
        try:
            return json.loads(result.stdout)
        except json.JSONDecodeError:
            return result.stdout

    def _wait_for_cde_session(self, session_name, vcluster_endpoint, timeout=300, poll_interval=10):
        """Poll until CDE session reaches 'available' status."""
        deadline = time.time() + timeout
        while time.time() < deadline:
            info = self._run_cde_cli(
                ["session", "describe", "--name", session_name],
                vcluster_endpoint=vcluster_endpoint
            )
            status = info.get("status", "unknown")
            logging.info("CDE session '%s' status: %s", session_name, status)
            if status == "available":
                return info
            if status in ("killed", "error", "failed", "timedout"):
                raise Exception(
                    "CDE session '{}' failed with status: {}".format(session_name, status)
                )
            time.sleep(poll_interval)
        raise Exception(
            "Timed out after {}s waiting for CDE session '{}'".format(timeout, session_name)
        )

    def _get_sc_url_from_session(self, session_info, session_name):
        """Extract Spark Connect URL from CDE session describe output."""
        for path in [
            ["sparkConnectUrl"],
            ["sparkConnect", "url"],
            ["sparkConnect", "remoteUrl"],
            ["endpoints", "sparkConnect"],
        ]:
            node = session_info
            for key in path:
                node = node.get(key) if isinstance(node, dict) else None
                if node is None:
                    break
            if node and isinstance(node, str):
                return node
        raise Exception(
            "Could not find Spark Connect URL in CDE session '{}'. Info: {}".format(
                session_name, json.dumps(session_info, indent=2)
            )
        )

    def _create_session_via_cde_cli(self, connection_name, connection_params):
        """
        Create (or reuse) a Spark Connect session via the CDE CLI binary.

        Requires:
          - CDE PySpark tarball installed in code-env (pyspark-<version>-cde.tar.gz)
          - 'cde' CLI installed and on PATH on the DSS server
          - CDE CLI pre-configured: cde configure --endpoint <vc-endpoint>

        Note: does NOT require the CDE Python package tarball (cde-<version>.tar.gz).

        Connection params used:
          host          - CDE Virtual Cluster endpoint URL
          sessionName   - optional; defaults to 'dss-<connection_name>'
          sparkConfigs  - optional list of 'key=value' Spark conf overrides
        """
        self._require_pyspark()
        vcluster_endpoint = connection_params["host"]
        session_name = connection_params.get("sessionName") or "dss-{}".format(connection_name)

        # Reuse an existing available session if present
        try:
            existing = self._run_cde_cli(
                ["session", "describe", "--name", session_name],
                vcluster_endpoint=vcluster_endpoint,
                check=False
            )
            if isinstance(existing, dict) and existing.get("status") == "available":
                logging.info("Reusing existing CDE session '%s'", session_name)
                sc_url = self._get_sc_url_from_session(existing, session_name)
                session = SparkSession.builder.remote(sc_url).getOrCreate()
                session.dss_connection_name = connection_name
                session._cde_session_name = session_name
                session._cde_vcluster_endpoint = vcluster_endpoint
                return session
        except Exception:
            pass  # session doesn't exist yet — fall through to create

        create_args = ["session", "create", "--name", session_name, "--type", "spark-connect"]
        for cfg in connection_params.get("sparkConfigs", []):
            create_args += ["--conf", cfg]

        logging.info(
            "Creating CDE Spark Connect session '%s' at %s", session_name, vcluster_endpoint
        )
        self._run_cde_cli(create_args, vcluster_endpoint=vcluster_endpoint)

        session_info = self._wait_for_cde_session(session_name, vcluster_endpoint)
        sc_url = self._get_sc_url_from_session(session_info, session_name)

        logging.info("CDE session ready — connecting to: %s", sc_url)
        session = SparkSession.builder.remote(sc_url).getOrCreate()
        session.dss_connection_name = connection_name
        session._cde_session_name = session_name
        session._cde_vcluster_endpoint = vcluster_endpoint
        return session

    def stop_cde_session(self, spark_session):
        """
        Kill the CDE session tied to a SparkSession.
        Call on recipe/notebook teardown to avoid orphaned CDE sessions.
        Only applicable to sessions created via CDE_CLI auth type.
        """
        session_name = getattr(spark_session, "_cde_session_name", None)
        vcluster_endpoint = getattr(spark_session, "_cde_vcluster_endpoint", None)
        if session_name and vcluster_endpoint:
            logging.info("Killing CDE session '%s'", session_name)
            self._run_cde_cli(
                ["session", "kill", "--name", session_name],
                vcluster_endpoint=vcluster_endpoint,
                check=False
            )

    # -------------------------------------------------------------------------
    # Session factory
    # -------------------------------------------------------------------------

    def _create_session(self, connection_name, connection_info, project_key=None):
        connection_params = connection_info["resolvedParams"]

        host      = connection_params["host"]
        port      = int(connection_params.get("port", 15002))
        use_ssl   = connection_params.get("useSSL", True)
        auth_type = connection_params.get("authType", "PERSONAL_ACCESS_TOKEN")

        if auth_type == "PERSONAL_ACCESS_TOKEN":
            # Requires: pyspark-<version>-cde.tar.gz only
            self._require_pyspark()
            resolved = connection_info.get("resolvedBasicCredential")
            token = (
                resolved["password"]
                if resolved is not None
                else connection_params.get("pwd") or connection_params.get("password")
            )
            if not token:
                raise Exception("Cannot find access token in connection settings")
            sc_url = self._build_url_for_token(host, port, token, use_ssl=use_ssl)
            logging.info("Connecting to CDP Spark Connect: %s:%d", host, port)
            session = SparkSession.builder.remote(sc_url).getOrCreate()

        elif auth_type == "BASIC":
            # Requires: pyspark-<version>-cde.tar.gz only
            self._require_pyspark()
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
            logging.info("Connecting to CDP Spark Connect: %s:%d", host, port)
            session = SparkSession.builder.remote(sc_url).getOrCreate()

        elif auth_type == "CDE_SESSION":
            # Requires: BOTH pyspark-<version>-cde.tar.gz AND cde-<version>.tar.gz
            self._require_pyspark()
            self._require_cde_package()
            session_name = connection_params.get("sessionName") or "dss-{}".format(connection_name)
            logging.info("Creating/reusing CDE Spark Connect session: %s", session_name)
            session = CDESparkConnectSession.builder.sessionName(session_name).get()

        elif auth_type == "CDE_CLI":
            # Requires: pyspark-<version>-cde.tar.gz + cde CLI binary on PATH
            # Does NOT require cde-<version>.tar.gz
            session = self._create_session_via_cde_cli(connection_name, connection_params)

        else:
            raise Exception("Auth type not supported: " + auth_type)

        # Post-connect statements (shared across all auth types)
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