from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from typing import Any

import pymongo
import streamlit as st
from passlib.context import CryptContext
from pymongo import ASCENDING, DESCENDING
from pymongo.collection import Collection
from pymongo.database import Database
from pymongo.errors import DuplicateKeyError, PyMongoError

from core.configuration import (
    ACTIVITY_LOGS_COLLECTION,
    FLOWCHARTS_COLLECTION,
    FLOWCHART_VERSIONS_COLLECTION,
    FLOWCHART_DRAFTS_COLLECTION,
    FLOWCHART_COMMENTS_COLLECTION,
    FLOWCHART_APPROVALS_COLLECTION,
    FLOWCHART_TEMPLATES_COLLECTION,
    FLOWCHART_PRESENCE_COLLECTION,
    MONGO_DB_NAME,
    USERS_COLLECTION,
    VALID_USER_ROLES,
)

log = logging.getLogger("produto_tools.database")
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _secret(*names: str, default: Any = None) -> Any:
    """Lê primeiro os Secrets do Streamlit e depois variáveis de ambiente."""
    for name in names:
        try:
            value = st.secrets.get(name)
        except Exception:
            value = None
        if value not in (None, ""):
            return value
        value = os.getenv(name)
        if value not in (None, ""):
            return value
    return default


@st.cache_resource(show_spinner="Conectando ao MongoDB...")
def get_mongo_client() -> pymongo.MongoClient | None:
    connection_string = _secret("MONGO_CONNECTION_STRING", "mongo_connection_string")
    if not connection_string:
        log.error("MONGO_CONNECTION_STRING não foi configurada.")
        return None

    try:
        client = pymongo.MongoClient(
            str(connection_string),
            serverSelectionTimeoutMS=8_000,
            connectTimeoutMS=8_000,
            socketTimeoutMS=20_000,
            maxPoolSize=20,
            minPoolSize=0,
            retryWrites=True,
            appname="produto-tools",
            tz_aware=True,
        )
        client.admin.command("ping")
        return client
    except PyMongoError:
        log.exception("Falha ao conectar ao MongoDB Atlas.")
        return None


@st.cache_resource
def get_database() -> Database | None:
    client = get_mongo_client()
    if client is None:
        return None
    database_name = str(_secret("MONGO_DB_NAME", default=MONGO_DB_NAME)).strip() or MONGO_DB_NAME
    return client[database_name]


@st.cache_resource
def get_collection(collection_name: str) -> Collection | None:
    database = get_database()
    return database[collection_name] if database is not None else None


@st.cache_resource
def initialize_database() -> bool:
    """Cria somente índices; não altera usuários existentes do simulador."""
    database = get_database()
    if database is None:
        return False

    try:
        database[USERS_COLLECTION].create_index(
            [("username", ASCENDING)], unique=True, name="uq_users_username"
        )
        database[USERS_COLLECTION].create_index(
            [("email", ASCENDING)], sparse=True, name="ix_users_email"
        )
        database[FLOWCHARTS_COLLECTION].create_index(
            [("owner_username", ASCENDING), ("updated_at", DESCENDING)],
            name="ix_pt_flows_owner_updated",
        )
        database[FLOWCHARTS_COLLECTION].create_index(
            [("status", ASCENDING), ("updated_at", DESCENDING)],
            name="ix_pt_flows_status_updated",
        )
        database[FLOWCHART_VERSIONS_COLLECTION].create_index(
            [("flowchart_id", ASCENDING), ("version", DESCENDING)],
            unique=True,
            name="uq_pt_flow_versions",
        )
        database[FLOWCHART_DRAFTS_COLLECTION].create_index(
            [("flowchart_id", ASCENDING), ("username", ASCENDING)],
            unique=True,
            name="uq_pt_flow_drafts",
        )
        database[FLOWCHART_DRAFTS_COLLECTION].create_index(
            [("updated_at", DESCENDING)], name="ix_pt_drafts_updated"
        )
        database[FLOWCHART_COMMENTS_COLLECTION].create_index(
            [("flowchart_id", ASCENDING), ("resolved", ASCENDING), ("created_at", DESCENDING)],
            name="ix_pt_comments_flow_status",
        )
        database[FLOWCHART_APPROVALS_COLLECTION].create_index(
            [("flowchart_id", ASCENDING), ("created_at", DESCENDING)],
            name="ix_pt_approvals_flow",
        )
        database[FLOWCHART_TEMPLATES_COLLECTION].create_index(
            [("owner_username", ASCENDING), ("name", ASCENDING)],
            name="ix_pt_templates_owner_name",
        )
        database[FLOWCHART_PRESENCE_COLLECTION].create_index(
            [("flowchart_id", ASCENDING), ("last_seen", DESCENDING)],
            name="ix_pt_presence_flow",
        )
        database[FLOWCHART_PRESENCE_COLLECTION].create_index(
            [("expires_at", ASCENDING)], expireAfterSeconds=0, name="ttl_pt_presence"
        )
        database[ACTIVITY_LOGS_COLLECTION].create_index(
            [("timestamp", DESCENDING)], name="ix_logs_timestamp"
        )
        return True
    except PyMongoError:
        log.exception("Falha ao criar índices do MongoDB.")
        return False


def get_users_collection() -> Collection | None:
    return get_collection(USERS_COLLECTION)


def verify_password(plain_password: str, hashed_password: str) -> bool:
    try:
        return pwd_context.verify(plain_password, hashed_password)
    except (ValueError, TypeError):
        return False


def get_password_hash(password: str) -> str:
    return pwd_context.hash(password)


def fetch_all_users_for_auth() -> dict[str, dict[str, dict[str, str]]]:
    """Retorna o formato exigido pelo streamlit-authenticator.

    O schema é idêntico ao Simulador-Telemetria:
    username, name, email, hashed_password, role e active.
    """
    users_collection = get_users_collection()
    credentials: dict[str, dict[str, dict[str, str]]] = {"usernames": {}}
    if users_collection is None:
        return credentials

    projection = {
        "_id": 0,
        "username": 1,
        "name": 1,
        "email": 1,
        "hashed_password": 1,
        "role": 1,
        "active": 1,
    }
    try:
        cursor = users_collection.find({"active": {"$ne": False}}, projection)
        for user in cursor:
            username = str(user.get("username") or "").strip().lower()
            hashed_password = user.get("hashed_password")
            if username and hashed_password:
                credentials["usernames"][username] = {
                    "name": str(user.get("name") or username),
                    "email": str(user.get("email") or ""),
                    "password": str(hashed_password),
                    "role": str(user.get("role") or "user"),
                }
    except PyMongoError:
        log.exception("Falha ao carregar usuários para autenticação.")
    return credentials


def get_user(username: str) -> dict[str, Any] | None:
    users_collection = get_users_collection()
    if users_collection is None:
        return None
    normalized = str(username or "").strip().lower()
    if not normalized:
        return None
    try:
        return users_collection.find_one({"username": normalized}, {"hashed_password": 0})
    except PyMongoError:
        log.exception("Falha ao consultar usuário.")
        return None


def get_user_profile(username: str) -> dict[str, Any] | None:
    user = get_user(username)
    if not user or user.get("active") is False:
        return None
    return {
        "username": str(user.get("username") or "").strip().lower(),
        "name": str(user.get("name") or user.get("username") or "Usuário"),
        "email": str(user.get("email") or ""),
        "role": str(user.get("role") or "user"),
        "active": user.get("active") is not False,
    }


def get_user_role(username: str) -> str | None:
    profile = get_user_profile(username)
    return str(profile["role"]) if profile else None


def get_all_users() -> list[dict[str, Any]]:
    users_collection = get_users_collection()
    if users_collection is None:
        return []
    try:
        return list(
            users_collection.find({}, {"hashed_password": 0}).sort("name", ASCENDING)
        )
    except PyMongoError:
        log.exception("Falha ao listar usuários.")
        return []


def add_user(username: str, name: str, email: str, password: str, role: str) -> bool:
    users_collection = get_users_collection()
    if users_collection is None:
        return False

    normalized_username = str(username or "").strip().lower()
    normalized_email = str(email or "").strip().lower()
    clean_name = str(name or "").strip()
    clean_role = role if role in VALID_USER_ROLES else "user"
    if not normalized_username or not clean_name or not normalized_email or len(password) < 8:
        return False

    now = utc_now()
    try:
        users_collection.insert_one(
            {
                "username": normalized_username,
                "name": clean_name,
                "email": normalized_email,
                "hashed_password": get_password_hash(password),
                "role": clean_role,
                "active": True,
                "created_at": now,
                "updated_at": now,
            }
        )
        return True
    except DuplicateKeyError:
        return False
    except PyMongoError:
        log.exception("Falha ao cadastrar usuário.")
        return False


def update_user(
    username: str,
    new_name: str,
    new_email: str,
    new_role: str,
    active: bool = True,
) -> bool:
    users_collection = get_users_collection()
    if users_collection is None:
        return False
    role = new_role if new_role in VALID_USER_ROLES else "user"
    normalized_username = str(username or "").strip().lower()
    try:
        existing = users_collection.find_one({"username": normalized_username}, {"role": 1, "active": 1})
        if not existing:
            return False
        removing_last_admin = (
            existing.get("role") == "admin"
            and (role != "admin" or not active)
            and users_collection.count_documents(
                {"role": "admin", "active": {"$ne": False}}
            ) <= 1
        )
        if removing_last_admin:
            return False
        result = users_collection.update_one(
            {"username": normalized_username},
            {
                "$set": {
                    "name": str(new_name or "").strip(),
                    "email": str(new_email or "").strip().lower(),
                    "role": role,
                    "active": bool(active),
                    "updated_at": utc_now(),
                }
            },
        )
        return result.matched_count > 0
    except PyMongoError:
        log.exception("Falha ao atualizar usuário.")
        return False


def update_user_password(username: str, new_password: str) -> bool:
    users_collection = get_users_collection()
    if users_collection is None or len(new_password) < 8:
        return False
    try:
        result = users_collection.update_one(
            {"username": str(username or "").strip().lower()},
            {
                "$set": {
                    "hashed_password": get_password_hash(new_password),
                    "updated_at": utc_now(),
                }
            },
        )
        return result.matched_count > 0
    except PyMongoError:
        log.exception("Falha ao atualizar senha.")
        return False


def delete_user(username: str) -> bool:
    users_collection = get_users_collection()
    if users_collection is None:
        return False

    normalized = str(username or "").strip().lower()
    try:
        user = users_collection.find_one({"username": normalized}, {"role": 1})
        if not user:
            return False
        if (
            user.get("role") == "admin"
            and users_collection.count_documents(
                {"role": "admin", "active": {"$ne": False}}
            )
            <= 1
        ):
            return False
        return users_collection.delete_one({"username": normalized}).deleted_count > 0
    except PyMongoError:
        log.exception("Falha ao excluir usuário.")
        return False


def add_log(user: str, action: str, details: Any = None) -> bool:
    collection = get_collection(ACTIVITY_LOGS_COLLECTION)
    if collection is None:
        return False
    try:
        collection.insert_one(
            {
                "timestamp": utc_now(),
                "user": str(user or "sistema"),
                "application": "produto_tools",
                "action": str(action),
                "details": details if details is not None else {},
            }
        )
        return True
    except PyMongoError:
        log.exception("Falha ao registrar log.")
        return False


def get_activity_logs(*, limit: int = 200, application: str = "produto_tools") -> list[dict[str, Any]]:
    collection = get_collection(ACTIVITY_LOGS_COLLECTION)
    if collection is None:
        return []
    try:
        return list(
            collection.find(
                {"application": application},
                {"_id": 0},
            ).sort("timestamp", DESCENDING).limit(max(1, min(int(limit), 1000)))
        )
    except PyMongoError:
        log.exception("Falha ao consultar logs de atividade.")
        return []
