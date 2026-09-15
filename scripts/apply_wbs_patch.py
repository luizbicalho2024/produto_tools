from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def read(path: str) -> str:
    return (ROOT / path).read_text(encoding="utf-8")


def write(path: str, content: str) -> None:
    (ROOT / path).write_text(content, encoding="utf-8", newline="\n")


def replace_once(text: str, old: str, new: str, label: str) -> str:
    if new in text:
        return text
    if old not in text:
        raise RuntimeError(f"Anchor não encontrado para {label}.")
    return text.replace(old, new, 1)


# core/configuration.py
path = "core/configuration.py"
text = read(path)
text = text.replace('APP_VERSION = "4.0.0"', 'APP_VERSION = "4.1.0"')
if 'WBS_COLLECTION = "produto_tools_wbs"' not in text:
    text = replace_once(
        text,
        'PROJECT_MEMBERS_COLLECTION = "produto_tools_project_members"\n',
        'PROJECT_MEMBERS_COLLECTION = "produto_tools_project_members"\nWBS_COLLECTION = "produto_tools_wbs"\n',
        "WBS_COLLECTION",
    )
write(path, text)

# database.py
path = "database.py"
text = read(path)
if "    WBS_COLLECTION,\n" not in text:
    text = replace_once(
        text,
        "    PROJECT_MEMBERS_COLLECTION,\n",
        "    PROJECT_MEMBERS_COLLECTION,\n    WBS_COLLECTION,\n",
        "import WBS_COLLECTION",
    )
indexes = '''        database[WBS_COLLECTION].create_index(
            [("owner_username", ASCENDING), ("updated_at", DESCENDING)],
            name="ix_pt_wbs_owner_updated",
        )
        database[WBS_COLLECTION].create_index(
            [("project_id", ASCENDING), ("updated_at", DESCENDING)],
            name="ix_pt_wbs_project_updated",
        )
        database[WBS_COLLECTION].create_index(
            [("status", ASCENDING), ("updated_at", DESCENDING)],
            name="ix_pt_wbs_status_updated",
        )
'''
if "ix_pt_wbs_owner_updated" not in text:
    anchor = '        database[ACTIVITY_LOGS_COLLECTION].create_index(\n'
    if anchor not in text:
        raise RuntimeError("Anchor dos índices de activity_logs não encontrado em database.py.")
    text = text.replace(anchor, indexes + anchor, 1)
write(path, text)

# requirements.txt
path = "requirements.txt"
text = read(path)
if "pypdf" not in text.lower():
    suffix = "\n# WBS / EAP 4.1\npypdf>=5.0,<7.0\n"
    text = text.rstrip() + suffix
write(path, text)

# README.md
path = "README.md"
text = read(path)
marker = "## WBS / EAP — Work Breakdown Structure"
if marker not in text:
    text = text.rstrip() + '''\n\n## WBS / EAP — Work Breakdown Structure\n\nA partir da versão **4.1.0**, o Produto Tools possui uma página dedicada à WBS/EAP, com:\n\n- CRUD de WBS e pacotes de trabalho;\n- vínculo com projetos existentes;\n- códigos hierárquicos automáticos (`1`, `1.1`, `1.1.1`...);\n- visualização gráfica, tabela, outline e indicadores;\n- edição por formulário e edição tabular rápida;\n- responsável, status, entregável, datas, duração, progresso, custo, marcos e tags;\n- importação de JSON, XML, Microsoft Project XML, XLS/XLSX, CSV, TSV, TXT, Markdown e PDF textual;\n- exportação para JSON, XML, Excel, CSV, TSV, PDF, Markdown, TXT e pacote ZIP completo;\n- controle de revisão e auditoria no MongoDB.\n\nConsulte `docs/WBS.md` para detalhes.\n'''
write(path, text)

# CHANGELOG.md
path = "CHANGELOG.md"
text = read(path)
if "## 4.1.0" not in text:
    entry = '''## 4.1.0 — WBS / EAP\n\n- nova página exclusiva `WBS — Work Breakdown Structure`;\n- CRUD completo de WBS e pacotes hierárquicos;\n- vínculo da WBS aos projetos existentes;\n- visão gráfica Graphviz, tabela, outline e indicadores;\n- importação JSON/XML/Microsoft Project XML/Excel/CSV/TSV/TXT/Markdown/PDF;\n- exportação JSON/XML/Excel/CSV/TSV/PDF/Markdown/TXT/ZIP;\n- controle de revisão otimista e trilha de auditoria;\n- coleção MongoDB dedicada `produto_tools_wbs`;\n- testes de hierarquia, formatos, exportação e repositório.\n\n'''
    if text.startswith("# "):
        first_break = text.find("\n") + 1
        text = text[:first_break] + "\n" + entry + text[first_break:].lstrip("\n")
    else:
        text = entry + text
write(path, text)

print("Patch WBS 4.1.0 aplicado com sucesso.")
