import os
class Config:
    SQLALCHEMY_DATABASE_URI = os.getenv("DATABASE_URL", "sqlite:///apg_importer.db")
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    SECRET_KEY = os.getenv("SECRET_KEY", "dev-key")

    ENVISION_BASE = os.getenv("ENVISION_BASE", "https://<envision-host>/v1")
    ENVISION_USER = os.getenv("ENVISION_USER", "")   # e.g. "OJB"
    ENVISION_PASS = os.getenv("ENVISION_PASS", "")   # e.g. "********"

    APG_POST_URL = os.getenv("APG_POST_URL", "https://apg.example.com/v1/import/flights")
    APG_BEARER_TOKEN = os.getenv("APG_BEARER_TOKEN", "")
    IMPORT_LOOKBACK_HOURS = int(os.getenv("IMPORT_LOOKBACK_HOURS", "24"))
    DCS_API_BASE = os.getenv("DCS_API_BASE", "https://release.ttinteractive.com/Zenith/TTI.Partners")
    PROD_DCS_API_BASE = os.getenv("DCS_API_BASE", "https://pacific.ttinteractive.com/Zenith/TTI.Partners")
    DCS_API_FLIGHTS_PATH = os.getenv("DCS_API_FLIGHTS_PATH", "/api/DCS/FullPassengerList")  # adjust to the actual Swagger path if different
    DCS_API_KEY = os.getenv("DCS_API_KEY", "_JEAAAAB607RfBiBZY3vWAH5_EhngbMJtve4vFFXnqhJuqJcJcVgtGKGkkp9yMwBCBn38SDCA_U_U")  # put the UAT key here
    PROD_DCS_API_KEY = os.getenv("DCS_API_KEY", "_JEAAAAPRmnXnYshbiY03_EOCc0KaW_R21QBiK0iDjORJ39FDjdt_Eld94E1gbBlyTF0ryG_En8w_U_U")  # put the UAT key here    
    DCS_DEFAULT_AIRLINE = os.getenv("DCS_DEFAULT_AIRLINE", "")
