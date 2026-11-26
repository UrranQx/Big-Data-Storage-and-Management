"""Local analytics configuration, credentials supplied by Compose."""

import os

from sqlalchemy.engine import URL

SECRET_KEY = os.environ["SUPERSET_SECRET_KEY"]
SQLALCHEMY_DATABASE_URI = URL.create(
    "postgresql+psycopg2",
    username="superset",
    password=os.environ["SUPERSET_DB_PASSWORD"],
    host="postgres",
    database="superset",
).render_as_string(hide_password=False)
FEATURE_FLAGS = {"ENABLE_TEMPLATE_PROCESSING": True}
WTF_CSRF_ENABLED = True
ROW_LIMIT = 10000
SQL_MAX_ROW = 100000
SUPERSET_WEBSERVER_TIMEOUT = 120
SQLLAB_TIMEOUT = 120
SQLALCHEMY_TRACK_MODIFICATIONS = False
ENABLE_PROXY_FIX = False
LANGUAGES = {
    "en": {"flag": "us", "name": "English"},
    "ru": {"flag": "ru", "name": "Русский"},
}
APP_NAME = "Speedrun Analytics"
