import json
import os
import secrets
import threading
import time
import hashlib
from typing import Any, Dict, List, Optional


class AuthStore:
    def __init__(self, user_store_path: str, admin_user: str, admin_password: str, token_ttl: int = 86400):
        self.user_store_path = user_store_path
        self.admin_user = admin_user
        self.admin_password = admin_password
        self.token_ttl = int(token_ttl)
        self._lock = threading.Lock()
        self._tokens: Dict[str, Dict[str, Any]] = {}
        self._ensure_store()

    def _ensure_store(self) -> None:
        directory = os.path.dirname(self.user_store_path)
        if directory:
            os.makedirs(directory, exist_ok=True)
        if not os.path.exists(self.user_store_path):
            self._save({"users": []})

    def _load(self) -> Dict[str, Any]:
        with self._lock:
            if not os.path.exists(self.user_store_path):
                return {"users": []}
            with open(self.user_store_path, "r", encoding="utf-8") as f:
                return json.load(f)

    def _save(self, data: Dict[str, Any]) -> None:
        with self._lock:
            with open(self.user_store_path, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)

    @staticmethod
    def _hash_password(password: str, salt: str) -> str:
        payload = f"{salt}:{password}".encode("utf-8")
        return hashlib.sha256(payload).hexdigest()

    def _prune_tokens(self) -> None:
        now = int(time.time())
        expired = [t for t, meta in self._tokens.items() if now - meta["issued_at"] > self.token_ttl]
        for token in expired:
            self._tokens.pop(token, None)

    def login(self, username: str, password: str) -> Optional[Dict[str, Any]]:
        if username == self.admin_user and password == self.admin_password:
            return {"username": username, "role": "admin"}
        data = self._load()
        for user in data.get("users", []):
            if user.get("username") != username:
                continue
            salt = user.get("salt", "")
            hashed = user.get("password_hash", "")
            if hashed == self._hash_password(password, salt):
                return {"username": username, "role": user.get("role", "user")}
        return None

    def issue_token(self, username: str, role: str) -> str:
        self._prune_tokens()
        token = secrets.token_hex(24)
        self._tokens[token] = {"username": username, "role": role, "issued_at": int(time.time())}
        return token

    def verify_token(self, token: str) -> Optional[Dict[str, Any]]:
        self._prune_tokens()
        return self._tokens.get(token)

    def list_users(self) -> List[Dict[str, Any]]:
        data = self._load()
        users = []
        for user in data.get("users", []):
            users.append({"username": user.get("username"), "role": user.get("role", "user")})
        return users

    def add_user(self, username: str, password: str, role: str = "user") -> None:
        data = self._load()
        users = data.get("users", [])
        for user in users:
            if user.get("username") == username:
                raise ValueError("User already exists")
        salt = secrets.token_hex(8)
        users.append(
            {
                "username": username,
                "role": role,
                "salt": salt,
                "password_hash": self._hash_password(password, salt),
            }
        )
        data["users"] = users
        self._save(data)

    def delete_user(self, username: str) -> None:
        data = self._load()
        users = [u for u in data.get("users", []) if u.get("username") != username]
        data["users"] = users
        self._save(data)
