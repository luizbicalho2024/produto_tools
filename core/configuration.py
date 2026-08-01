from __future__ import annotations

import os

APP_NAME = os.getenv("PRODUTO_TOOLS_APP_NAME", "Produto Tools")
APP_VERSION = "3.0.3"

MONGO_DB_NAME = os.getenv("MONGO_DB_NAME", "simulador_db")
USERS_COLLECTION = "users"
ACTIVITY_LOGS_COLLECTION = "activity_logs"
FLOWCHARTS_COLLECTION = "produto_tools_flowcharts"
FLOWCHART_VERSIONS_COLLECTION = "produto_tools_flowchart_versions"
FLOWCHART_DRAFTS_COLLECTION = "produto_tools_flowchart_drafts"
FLOWCHART_COMMENTS_COLLECTION = "produto_tools_flowchart_comments"
FLOWCHART_APPROVALS_COLLECTION = "produto_tools_flowchart_approvals"
FLOWCHART_TEMPLATES_COLLECTION = "produto_tools_flowchart_templates"
FLOWCHART_PRESENCE_COLLECTION = "produto_tools_flowchart_presence"

VALID_USER_ROLES = {"user", "head_comercial", "admin"}
ROLE_LABELS = {
    "user": "Usuário",
    "head_comercial": "Head Comercial",
    "admin": "Administrador",
}

FLOW_ACCESS_LEVELS = ("viewer", "editor", "reviewer", "approver")
FLOW_ACCESS_LABELS = {
    "viewer": "Visualizador",
    "editor": "Editor",
    "reviewer": "Revisor",
    "approver": "Aprovador",
}

WORKFLOW_STATUSES = (
    "draft",
    "in_review",
    "approved",
    "published",
    "archived",
)
WORKFLOW_STATUS_LABELS = {
    "draft": "Rascunho",
    "in_review": "Em revisão",
    "approved": "Aprovado",
    "published": "Publicado",
    "archived": "Arquivado",
}
