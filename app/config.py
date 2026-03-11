import os
class Config:
    SQLALCHEMY_DATABASE_URI = os.getenv("SQLALCHEMY_DATABASE_URI")
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    SECRET_KEY = os.getenv("SECRET_KEY")

    ENVISION_BASE = os.getenv("ENVISION_BASE")
    ENVISION_TEST = os.getenv("ENVISION_TEST")
    ENVISION_ENV = os.getenv("ENVISION_ENV")
    ENVISION_USE_TEST = os.getenv("ENVISION_USE_TEST")
    ENVISION_USER = os.getenv("ENVISION_USER")   # e.g. "OJB"
    ENVISION_PASS = os.getenv("ENVISION_PASS")   # e.g. "********"

    APG_POST_URL = os.getenv("APG_POST_URL")
    APG_BEARER_TOKEN = os.getenv("APG_BEARER_TOKEN")
    IMPORT_LOOKBACK_HOURS = int(os.getenv("IMPORT_LOOKBACK_HOURS", "24"))
    DCS_API_BASE = os.getenv("DCS_API_BASE")
    PROD_DCS_API_BASE = os.getenv("PROD_DCS_API_BASE")
    DCS_API_FLIGHTS_PATH = os.getenv("DCS_API_FLIGHTS_PATH")
    DCS_API_KEY = os.getenv("DCS_API_KEY")
    PROD_DCS_API_KEY = os.getenv("PROD_DCS_API_KEY")
    DCS_DEFAULT_AIRLINE = os.getenv("DCS_DEFAULT_AIRLINE")
