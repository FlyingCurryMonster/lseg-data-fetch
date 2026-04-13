"""
Direct OAuth token management for LSEG/Refinitiv APIs.

Authenticates via password grant, refreshes via refresh_token grant,
falls back to full re-auth, and proactively refreshes before expiry.
No lseg.data SDK dependency.
"""

import os
import time
import threading

import requests
from dotenv import load_dotenv

load_dotenv()

AUTH_URL = "https://api.refinitiv.com/auth/oauth2/v1/token"


class TokenManager:
    """Direct OAuth token management — no lseg.data SDK dependency."""

    def __init__(self):
        self._app_key = os.getenv("DSWS_APPKEY")
        self._username = os.getenv("DSWS_USERNAME")
        self._password = os.getenv("DSWS_PASSWORD")
        self._token = None
        self._refresh_token_str = None
        self._token_expiry = 0
        self._lock = threading.Lock()
        self._consecutive_401s = 0
        self.authenticate()

    def authenticate(self):
        """Get initial token via password grant."""
        resp = requests.post(AUTH_URL, data={
            "grant_type": "password",
            "username": self._username,
            "password": self._password,
            "client_id": self._app_key,
            "scope": "trapi",
            "takeExclusiveSignOnControl": "true",
        }, headers={"Content-Type": "application/x-www-form-urlencoded"})
        resp.raise_for_status()
        data = resp.json()
        self._token = data["access_token"]
        self._refresh_token_str = data.get("refresh_token")
        self._token_expiry = time.time() + int(data.get("expires_in", 300)) - 30
        self._consecutive_401s = 0
        print(f"  [auth] Authenticated (token expires in {data.get('expires_in', '?')}s)",
              flush=True)

    def refresh(self):
        """Refresh token via refresh_token grant, fall back to full re-auth."""
        with self._lock:
            if self._refresh_token_str:
                try:
                    resp = requests.post(AUTH_URL, data={
                        "grant_type": "refresh_token",
                        "refresh_token": self._refresh_token_str,
                        "client_id": self._app_key,
                    }, headers={"Content-Type": "application/x-www-form-urlencoded"})
                    if resp.status_code == 200:
                        data = resp.json()
                        self._token = data["access_token"]
                        self._refresh_token_str = data.get("refresh_token",
                                                           self._refresh_token_str)
                        self._token_expiry = (time.time()
                                              + int(data.get("expires_in", 300)) - 30)
                        self._consecutive_401s = 0
                        print(f"  [auth] Token refreshed "
                              f"(expires in {data.get('expires_in', '?')}s)",
                              flush=True)
                        return
                except Exception:
                    pass
            # Fallback: full re-auth
            self.authenticate()

    def headers(self):
        """Return auth headers, proactively refreshing if near expiry."""
        if time.time() > self._token_expiry:
            self.refresh()
        return {
            "Authorization": f"Bearer {self._token}",
            "Content-Type": "application/json",
        }

    def on_401(self):
        """Called on 401 response — refresh and track consecutive failures."""
        with self._lock:
            self._consecutive_401s += 1
        self.refresh()

    @property
    def consecutive_401s(self):
        return self._consecutive_401s
