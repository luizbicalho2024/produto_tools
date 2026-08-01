from __future__ import annotations

import os

APP_NAME = os.getenv("PRODUTO_TOOLS_APP_NAME", "Produto Tools")
APP_VERSION = "2.1.0"

# O Produto Tools utiliza o mesmo banco e a mesma coleção de usuários do
# Simulador-Telemetria. Os nomes podem ser sobrescritos nos Secrets.
MONGO_DB_NAME = os.getenv("MONGO_DB_NAME", "simulador_db")
USERS_COLLECTION = "users"
FLOWCHARTS_COLLECTION = "produto_tools_flowcharts"
FLOWCHART_VERSIONS_COLLECTION = "produto_tools_flowchart_versions"
ACTIVITY_LOGS_COLLECTION = "activity_logs"

VALID_USER_ROLES = {"user", "head_comercial", "admin"}
ROLE_LABELS = {
    "user": "Usuário",
    "head_comercial": "Head Comercial",
    "admin": "Administrador",
}
