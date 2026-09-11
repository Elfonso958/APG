import os
import unittest
from unittest.mock import patch

from app import create_app, db
from app.config import Config
from app.models import AppUser, EmailSettings


class ApgAccountTest(unittest.TestCase):
    def setUp(self):
        Config.SQLALCHEMY_DATABASE_URI = "sqlite:///:memory:"
        self.app = create_app()
        self.app.config.update(TESTING=True)
        self.ctx = self.app.app_context()
        self.ctx.push()
        db.drop_all()
        db.create_all()
        self.client = self.app.test_client()

    def tearDown(self):
        db.session.remove()
        db.drop_all()
        self.ctx.pop()

    def csrf_token(self):
        with self.client.session_transaction() as session:
            return session["apg_csrf_token"]

    def test_initial_admin_can_sign_in_and_manage_accounts_and_email_settings(self):
        with patch.dict(os.environ, {
            "APG_INITIAL_ADMIN_EMAIL": "admin@example.test",
            "APG_INITIAL_ADMIN_PASSWORD": "temporary-test-password",
            "APG_INITIAL_ADMIN_NAME": "Test Admin",
        }, clear=False):
            login_page = self.client.get("/account/login")
        self.assertEqual(login_page.status_code, 200)
        self.assertEqual(AppUser.query.count(), 1)

        signed_in = self.client.post("/account/login", data={
            "csrf_token": self.csrf_token(),
            "email": "admin@example.test",
            "password": "temporary-test-password",
            "next": "/admin/users",
        })
        self.assertEqual(signed_in.status_code, 302)
        self.assertIn("/admin/users", signed_in.location)

        users_page = self.client.get("/admin/users")
        self.assertEqual(users_page.status_code, 200)
        self.assertIn(b"APG users", users_page.data)

        created = self.client.post("/admin/users", data={
            "csrf_token": self.csrf_token(),
            "action": "create",
            "display_name": "Operations Agent",
            "email": "agent@example.test",
            "password": "agent-password-123",
        })
        self.assertEqual(created.status_code, 302)
        agent = AppUser.query.filter_by(email="agent@example.test").first()
        self.assertIsNotNone(agent)
        self.assertFalse(agent.is_admin)

        saved_email_settings = self.client.post("/admin/email-settings", data={
            "csrf_token": self.csrf_token(),
            "charter_closure_emails_enabled": "1",
            "flight_operations_email": "flightops@example.test, duty@example.test",
        })
        self.assertEqual(saved_email_settings.status_code, 302)
        settings = db.session.get(EmailSettings, 1)
        self.assertEqual(settings.flight_operations_email, "flightops@example.test, duty@example.test")

    def test_admin_pages_require_a_local_apg_login(self):
        response = self.client.get("/admin/users")
        self.assertEqual(response.status_code, 302)
        self.assertIn("/account/login", response.location)

