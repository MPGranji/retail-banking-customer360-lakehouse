import os


SECRET_KEY = os.environ.get("SUPERSET_SECRET_KEY")
if not SECRET_KEY:
    raise RuntimeError("SUPERSET_SECRET_KEY must be configured")

SQLALCHEMY_DATABASE_URI = os.environ.get(
    "SUPERSET_METADATA_DB_URI",
    "sqlite:////app/superset_home/superset.db",
)

SUPERSET_WEBSERVER_PORT = int(os.environ.get("SUPERSET_WEBSERVER_PORT", "8088"))
SUPERSET_WEBSERVER_TIMEOUT = 120
SQLLAB_TIMEOUT = 120
SQLLAB_ASYNC_TIME_LIMIT_SEC = 120

TALISMAN_ENABLED = False
WTF_CSRF_ENABLED = True
ENABLE_PROXY_FIX = True

FEATURE_FLAGS = {
    "DASHBOARD_NATIVE_FILTERS": True,
    # Keep datasource YAML import available for this local demo bootstrap.
    "VERSIONED_EXPORT": False,
}

PREVENT_UNSAFE_DB_CONNECTIONS = False
SQLALCHEMY_TRACK_MODIFICATIONS = False
