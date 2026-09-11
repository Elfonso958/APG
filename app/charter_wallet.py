"""Wallet pass helpers for ACCharters boarding passes."""
from __future__ import annotations

import base64
import hashlib
import json
import os
import shutil
import subprocess
import tempfile
import zipfile
from datetime import datetime
from pathlib import Path

from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding
from flask import current_app
from itsdangerous import BadSignature, URLSafeTimedSerializer


def _b64(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).rstrip(b"=").decode("ascii")


def make_wallet_token(flight_id: str, passenger_id: str) -> str:
    return URLSafeTimedSerializer(current_app.secret_key, salt="accharters-wallet-pass").dumps({"f": flight_id, "p": passenger_id})


def parse_wallet_token(token: str) -> dict:
    return URLSafeTimedSerializer(current_app.secret_key, salt="accharters-wallet-pass").loads(token, max_age=60 * 60 * 24 * 30)


def google_wallet_link(*, issuer_id: str, service_account_file: str, origin: str, flight_id: str, passenger: dict, flight_no: str, dep: str, ades: str, gate: str, departure: str, logo_url: str) -> str | None:
    if not issuer_id or not os.path.isfile(service_account_file):
        return None
    with open(service_account_file, encoding="utf-8") as stream:
        account = json.load(stream)
    object_id = f"{issuer_id}.charter_{flight_id}_{passenger['PassengerId']}".replace("-", "_")
    class_id = f"{issuer_id}.accharters_charter_boarding"
    barcode = f"ACCI|{flight_id}|{passenger['PassengerId']}|{passenger.get('Seat') or ''}"
    title = f"{dep} → {ades}"
    payload = {
        "iss": account["client_email"], "aud": "google", "typ": "savetowallet", "origins": [origin.rstrip("/")],
        "payload": {"genericClasses": [{"id": class_id, "issuerName": "ACCharters", "reviewStatus": "UNDER_REVIEW", "logo": {"sourceUri": {"uri": logo_url}}, "hexBackgroundColor": "#075c74"}], "genericObjects": [{"id": object_id, "classId": class_id, "state": "ACTIVE", "cardTitle": {"defaultValue": {"language": "en-NZ", "value": "ACCharters"}}, "header": {"defaultValue": {"language": "en-NZ", "value": title}}, "subheader": {"defaultValue": {"language": "en-NZ", "value": flight_no or "Charter flight"}}, "barcode": {"type": "QR_CODE", "value": barcode, "alternateText": passenger.get("Seat") or "GATE"}, "textModulesData": [{"header": "Passenger", "body": f"{passenger.get('GivenName','')} {passenger.get('Surname','')}".strip()}, {"header": "Seat · Gate", "body": f"{passenger.get('Seat') or 'GATE'} · {gate or 'AS DIRECTED'}"}, {"header": "Departure", "body": departure}]}]},
    }
    header = {"alg": "RS256", "typ": "JWT"}
    signing_input = f"{_b64(json.dumps(header, separators=(',', ':')).encode())}.{_b64(json.dumps(payload, separators=(',', ':')).encode())}".encode()
    key = serialization.load_pem_private_key(account["private_key"].encode(), password=None)
    signature = key.sign(signing_input, padding.PKCS1v15(), hashes.SHA256())
    return f"https://pay.google.com/gp/v/save/{signing_input.decode()}.{_b64(signature)}"


def apple_wallet_pass(*, flight_id: str, passenger: dict, flight_no: str, dep: str, ades: str, gate: str, departure: str) -> bytes:
    base = Path(os.getenv("CHARTER_WALLET_SECRETS_DIR", "/opt/apg-importer/wallet-secrets"))
    cert, key, intermediate = base / "accharters-wallet.cer", base / "accharters-wallet.key", base / "apple-wwdr-g4.cer"
    if not all(path.is_file() for path in (cert, key, intermediate)):
        raise RuntimeError("Apple Wallet signing is not configured")
    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory)
        logo = Path(current_app.root_path).parent / "ACCharter Main Logo.png"
        try:
            from PIL import Image
            image = Image.open(logo).convert("RGBA")
            image.thumbnail((320, 100))
            image.save(root / "logo.png")
            image.resize((29, 29)).save(root / "icon.png")
        except Exception:
            shutil.copyfile(logo, root / "logo.png")
            shutil.copyfile(logo, root / "icon.png")
        pass_json = {"formatVersion": 1, "passTypeIdentifier": "pass.co.nz.accharters.boarding", "serialNumber": f"{flight_id}-{passenger['PassengerId']}", "teamIdentifier": "HZGY9VQVC8", "organizationName": "ACCharters", "description": "ACCharters boarding pass", "logoText": "ACCharters", "backgroundColor": "rgb(7, 92, 116)", "foregroundColor": "rgb(255, 255, 255)", "labelColor": "rgb(255, 255, 255)", "boardingPass": {"transitType": "PKTransitTypeAir", "primaryFields": [{"key": "route", "label": "ROUTE", "value": f"{dep}  →  {ades}"}], "secondaryFields": [{"key": "passenger", "label": "PASSENGER", "value": f"{passenger.get('GivenName','')} {passenger.get('Surname','')}".strip()}, {"key": "flight", "label": "FLIGHT", "value": flight_no or "CHARTER"}], "auxiliaryFields": [{"key": "seat", "label": "SEAT", "value": passenger.get("Seat") or "GATE"}, {"key": "gate", "label": "GATE", "value": gate or "AS DIRECTED"}], "backFields": [{"key": "departure", "label": "DEPARTURE", "value": departure}]}, "barcode": {"format": "PKBarcodeFormatQR", "message": f"ACCI|{flight_id}|{passenger['PassengerId']}|{passenger.get('Seat') or ''}", "messageEncoding": "iso-8859-1", "altText": passenger.get("Seat") or "GATE"}}
        (root / "pass.json").write_text(json.dumps(pass_json, separators=(",", ":")), encoding="utf-8")
        manifest = {item.name: hashlib.sha1(item.read_bytes()).hexdigest() for item in root.iterdir() if item.is_file()}
        (root / "manifest.json").write_text(json.dumps(manifest, separators=(",", ":")), encoding="utf-8")
        subprocess.run(["openssl", "smime", "-binary", "-sign", "-certfile", str(intermediate), "-signer", str(cert), "-inkey", str(key), "-in", str(root / "manifest.json"), "-out", str(root / "signature"), "-outform", "DER", "-nodetach"], check=True, capture_output=True)
        output = root / "boarding-pass.pkpass"
        with zipfile.ZipFile(output, "w", zipfile.ZIP_DEFLATED) as archive:
            for item in root.iterdir():
                if item.name != output.name:
                    archive.write(item, item.name)
        return output.read_bytes()
