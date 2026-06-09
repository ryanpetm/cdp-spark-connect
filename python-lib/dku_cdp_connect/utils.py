import logging

_SECRET_KEY_MAP = {
    "Virtual Cluster Endpoint": "vclusterEndpoint",
    "CDP Control Plane URL":    "cdpEndpoint",
    "CDP Access Key ID":        "cdpAccessKeyId",
    "CDP Private Key":          "cdpPrivateKey",
}

_REQUIRED_PARAMS = ("vclusterEndpoint", "cdpAccessKeyId", "cdpPrivateKey", "cdpEndpoint")


def resolve_from_dss_secrets():
    """
    Fetch CDE connection credentials from DSS account-level user secrets.
    Returns a dict of resolved params. Raises if required secrets are missing.

    Required keys in My Account → Secrets:
        - Virtual Cluster Endpoint
        - CDP Access Key ID
        - CDP Private Key
        - CDP Control Plane URL
    Optional:
        - CDP Control Plane URL  (defaults to https://console.us-west-1.cdp.cloudera.com)
    """
    try:
        import dataiku
        auth_info = dataiku.api_client().get_auth_info(with_secrets=True)
        secrets = {s["key"]: s["value"] for s in auth_info.get("secrets", [])}
    except Exception as exc:
        raise RuntimeError("Could not fetch DSS user secrets: {}".format(exc))

    params = {}
    for secret_key, param_key in _SECRET_KEY_MAP.items():
        if secret_key in secrets:
            params[param_key] = secrets[secret_key]

    missing_secrets = [k for k, v in _SECRET_KEY_MAP.items() if v in _REQUIRED_PARAMS and not params.get(v)]
    if missing_secrets:
        raise RuntimeError(
            "Missing required DSS user secrets. "
            "Set the following in My Account → Secrets: {}".format(missing_secrets)
        )

    return params