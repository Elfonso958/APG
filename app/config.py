import os
class Config:
    SQLALCHEMY_DATABASE_URI = os.getenv("DATABASE_URL", "sqlite:///apg_importer.db")
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    SECRET_KEY = os.getenv("SECRET_KEY", "dev-key")
    SOURCE_API_BASE = os.getenv("SOURCE_API_BASE", "https://example.com")
    SOURCE_API_TOKEN = os.getenv("SOURCE_API_TOKEN", "")
    APG_POST_URL = os.getenv("APG_POST_URL", "https://apg.example.com/v1/import/flights")
    APG_BEARER_TOKEN = os.getenv("APG_BEARER_TOKEN", "")
    IMPORT_LOOKBACK_HOURS = int(os.getenv("IMPORT_LOOKBACK_HOURS", "24"))
