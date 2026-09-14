from __future__ import annotations

import re
from pathlib import Path

def read(path: str) -> str:
    return Path(path).read_text(encoding="utf-8")

def write(path: str, text: str) -> None:
    Path(path).write_text(text, encoding="utf-8", newline="\n")

def append_once(path: str, marker: str, block: str) -> None:
    text = read(path)
    if marker not in text:
        if not text.endswith("\n"):
            text += "\n"
        text += "\n" + block.strip() + "\n"
        write(path, text)

# Versão.
path = "core/configuration.py"
text = read(path)
text = re.sub(r'APP_VERSION\s*=\s*"[^"]+"', 'APP_VERSION = "4.0.0"', text, count=1)
write(path, text)

# Dependências runtime.
append_once("requirements.txt", "pyotp>=", """
# Produto Tools 4.0 Enterprise
Authlib>=1.3.2,<2.0
pyotp>=2.9,<3.0
openpyxl>=3.1,<4.0
xlrd>=2.0,<3.0
""")

# Dependências de qualidade.
append_once("requirements-dev.txt", "pytest-cov>=", """
pytest-cov>=5,<7
ruff>=0.9,<1.0
bandit>=1.8,<2.0
pip-audit>=2.8,<3.0
""")

# Configurações opcionais.
append_once(".env.example", "AUTH_MODE=", """
# Autenticacao: local ou oidc
AUTH_MODE=local

# IA externa opcional - endpoint compativel com Chat Completions
AI_API_URL=
AI_API_KEY=
AI_MODEL=
""")

# Senha mínima e soft delete.
path = "database.py"
text = read(path)
text = text.replace("len(password) < 8", "len(password) < 12")
text = text.replace("len(new_password) < 8", "len(new_password) < 12")
old_delete = 'return users_collection.delete_one({"username": normalized}).deleted_count > 0'
new_delete = """result = users_collection.update_one(
            {"username": normalized},
            {"$set": {"active": False, "deleted_at": utc_now(), "updated_at": utc_now()}},
        )
        return result.matched_count > 0"""
if old_delete in text:
    text = text.replace(old_delete, new_delete)
write(path, text)

# UI de acesso alinhada ao soft delete e senha mais forte.
path = "pages/1_Gestao_de_Acesso.py"
text = read(path)
text = text.replace("Mínimo de oito caracteres.", "Mínimo de doze caracteres.")
text = text.replace("len(password) < 8", "len(password) < 12")
text = text.replace("len(new_password) < 8", "len(new_password) < 12")
text = text.replace("A senha deve possuir pelo menos oito caracteres.", "A senha deve possuir pelo menos doze caracteres.")
text = text.replace("A nova senha deve possuir pelo menos oito caracteres.", "A nova senha deve possuir pelo menos doze caracteres.")
text = text.replace(
    'st.warning(f"A exclusão de **@{selected_username}** será permanente e afetará também o Simulador-Telemetria.")',
    'st.warning(f"A desativação de **@{selected_username}** preservará histórico e referências e afetará também o acesso compartilhado.")',
)
text = text.replace('"Excluir usuário"', '"Desativar usuário"')
text = text.replace('"Usuário excluído das duas aplicações."', '"Usuário desativado, com histórico preservado."')
write(path, text)

# Segregação mínima: revisor/aprovador não editam o conteúdo.
path = "services/flow_permissions.py"
text = read(path)
text = re.sub(
    r'def can_edit\(permission: str \| None\) -> bool:\n\s+return permission in \{[^}]+\}',
    'def can_edit(permission: str | None) -> bool:\n    return permission in {"owner", "editor"}',
    text,
    count=1,
)
write(path, text)


# Repositório principal: segregação de edição e regra de quatro-olhos.
path = "services/flowchart_repository.py"
text = read(path)
text = re.sub(
    r'def can_edit\(permission: str \| None\) -> bool:\n\s+return permission in \{[^}]+\}',
    'def can_edit(permission: str | None) -> bool:\n    return permission in {"owner", "editor"}',
    text,
    count=1,
)
four_eyes_anchor = '''    if not permission_fn(permission):
        raise FlowPermissionError("Seu perfil não possui permissão para esta transição.")
'''
four_eyes_block = '''    if not permission_fn(permission):
        raise FlowPermissionError("Seu perfil não possui permissão para esta transição.")
    if action in {"approve", "publish"} and not is_admin:
        last_editor = str(record.get("last_saved_by") or "").strip().lower()
        if last_editor and last_editor == actor.strip().lower():
            raise FlowPermissionError(
                "Regra de quatro-olhos: quem realizou a última edição não pode aprovar/publicar a própria alteração."
            )
'''
if "Regra de quatro-olhos" not in text:
    if four_eyes_anchor not in text:
        raise RuntimeError("Anchor de governança quatro-olhos não encontrado.")
    text = text.replace(four_eyes_anchor, four_eyes_block, 1)

# Emissão de eventos de governança para webhooks cadastrados.
event_anchor = '''    db.add_log(actor, "Alterou status de governança", {"flowchart_id": flowchart_id, "from": current, "to": target, "action": action})
    return {"from": current, "to": target}
'''
event_block = '''    db.add_log(actor, "Alterou status de governança", {"flowchart_id": flowchart_id, "from": current, "to": target, "action": action})
    try:
        from services.event_bus import emit_event
        event_name = {
            "approved": "process.approved",
            "published": "process.published",
            "archived": "process.archived",
            "draft": "process.changes_requested",
            "in_review": "process.review_requested",
        }.get(target, "process.status_changed")
        emit_event(event_name, {"flow_id": str(flowchart_id), "from": current, "to": target, "action": action, "actor": actor})
    except Exception:
        pass
    return {"from": current, "to": target}
'''
if 'emit_event(event_name' not in text:
    if event_anchor not in text:
        raise RuntimeError("Anchor de eventos de governança não encontrado.")
    text = text.replace(event_anchor, event_block, 1)

# Menções em comentários geram notificações.
comment_anchor = '''    _collection(FLOWCHART_COMMENTS_COLLECTION).insert_one({
        "_id": comment_id, "flowchart_id": str(flowchart_id), "target_kind": target_kind,
        "target_id": str(target_id), "content": clean, "author": author.strip().lower(),
        "mentions": sorted({item.strip().lower() for item in (mentions or []) if item.strip()}),
        "resolved": False, "created_at": utc_now(), "updated_at": utc_now(),
    })
    return comment_id
'''
comment_block = '''    clean_mentions = sorted({item.strip().lower() for item in (mentions or []) if item.strip()})
    _collection(FLOWCHART_COMMENTS_COLLECTION).insert_one({
        "_id": comment_id, "flowchart_id": str(flowchart_id), "target_kind": target_kind,
        "target_id": str(target_id), "content": clean, "author": author.strip().lower(),
        "mentions": clean_mentions,
        "resolved": False, "created_at": utc_now(), "updated_at": utc_now(),
    })
    if clean_mentions:
        try:
            from services.enterprise_repository import add_notification
            from services.event_bus import emit_event
            for mentioned in clean_mentions:
                add_notification(
                    mentioned,
                    "Você foi mencionado em um comentário",
                    clean[:500],
                    category="comment",
                    link=f"flow:{flowchart_id}",
                )
            emit_event("comment.mentioned", {"flow_id": str(flowchart_id), "comment_id": comment_id, "author": author, "mentions": clean_mentions})
        except Exception:
            pass
    return comment_id
'''
if "clean_mentions =" not in text:
    if comment_anchor not in text:
        raise RuntimeError("Anchor de comentários/menções não encontrado.")
    text = text.replace(comment_anchor, comment_block, 1)
write(path, text)

# Catálogo do editor em lote para remover consultas N+1.
path = "pages/5_Editor_de_Fluxos.py"
text = read(path)
import_line = "from services.portfolio_service import build_flow_catalog\n"
anchor = "from services.template_library import built_in_templates, clone_template\n"
if import_line not in text:
    if anchor not in text:
        raise RuntimeError("Anchor de importação do Editor não encontrado.")
    text = text.replace(anchor, anchor + import_line, 1)

if "flow_catalog = build_flow_catalog(flows)" not in text:
    pattern = re.compile(
        r'\nflow_catalog = \[\]\nfor item in flows:\n.*?\ncomments_for_editor = serialize_comments',
        re.DOTALL,
    )
    text, count = pattern.subn(
        '\nflow_catalog = build_flow_catalog(flows)\ncomments_for_editor = serialize_comments',
        text,
        count=1,
    )
    if count != 1:
        raise RuntimeError("Bloco N+1 do catálogo do Editor não foi localizado com segurança.")
write(path, text)

# README / changelog.
append_once("README.md", "## Produto Tools 4.0 Enterprise Suite", """
## Produto Tools 4.0 Enterprise Suite

A versão 4.0 adiciona melhoria contínua (SIPOC, 5W2H, FMEA, Ishikawa, Pareto e VSM),
simulação e custos, Process Mining e conformance, BPMN/DMN, execução real com formulários
e tarefas, Change Requests, compliance, catálogo de sistemas, capacidades, Customer Journey,
evidências, portal de processos publicados, copiloto de IA, POP/SOP, webhooks, notificações,
MFA TOTP e SSO/OIDC opcional.

Consulte `docs/ENTERPRISE_SUITE_4.md` para configuração e detalhes.
""")

path = "CHANGELOG.md"
text = read(path)
if "## 4.0.0 — Enterprise Suite" not in text:
    block = """# Changelog

## 4.0.0 — Enterprise Suite

- Nova Central de Melhoria: SIPOC, 5W2H, FMEA/riscos, Ishikawa, 5 Porquês, Pareto e VSM.
- Simulação What-if, custos e cockpit de SLA.
- Process Mining, variantes e conformance checking.
- Importação/exportação BPMN 2.0 e tabelas DMN.
- Form Builder, instâncias e Central de Tarefas.
- Change Requests, compliance, sistemas, capacidades, Customer Journey e evidências.
- Diff visual de versões e Portal de Processos Publicados.
- Copiloto, texto para fluxo, gerador POP/SOP, webhooks e notificações.
- MFA TOTP e autenticação OIDC opcional.
- Consultas em lote na Central e catálogo do Editor para reduzir N+1.
- Soft delete de usuários, senha mínima de 12 caracteres e segregação de edição/revisão.
- CI ampliado com cobertura, lint crítico e verificações de segurança.

"""
    if text.startswith("# Changelog\n"):
        text = block + text[len("# Changelog\n"):]
    else:
        text = block + text
    write(path, text)

print("Patch estrutural 4.0 aplicado com sucesso.")