import re
import logging


def parse_spark_connect_endpoint(endpoint):
    """
    Parse a CDP Spark Connect endpoint into (host, port).
    Accepts sc://host:port, host:port, or bare host (defaults port 15002).
    """
    if endpoint.startswith("sc://"):
        endpoint = endpoint[5:]
    endpoint = endpoint.split("/")[0].split(";")[0]
    m = re.match(r"^([^:]+)(?::(\d+))?$", endpoint)
    if m is None:
        raise ValueError("Cannot parse CDP Spark Connect endpoint: %s" % endpoint)
    host = m.group(1)
    port = int(m.group(2)) if m.group(2) else 15002
    return host, port


def build_spark_connect_url(host, port=15002, token=None, use_ssl=True, extra_params=None):
    """
    Build a sc:// Spark Connect URL for CDP.

    :param str host:         Spark Connect server host
    :param int port:         Spark Connect server port (default 15002)
    :param str token:        Optional Bearer/Knox token for auth
    :param bool use_ssl:     Whether to enable SSL/TLS (default True)
    :param dict extra_params: Additional key=value params appended to the URL
    :return: sc:// URL string
    """
    url = "sc://%s:%d" % (host, port)
    params = []
    if use_ssl:
        params.append("use_ssl=true")
    if token:
        params.append("token=%s" % token)
    if extra_params:
        for k, v in extra_params.items():
            params.append("%s=%s" % (k, v))
    if params:
        url += "/;" + ";".join(params)
    return url