from __future__ import annotations

from copy import deepcopy
from typing import Any

from schemas.flowchart_schema import demo_flowchart_document, new_flowchart_document


def built_in_templates(owner: str = "") -> list[dict[str, Any]]:
    approval = new_flowchart_document("Aprovação corporativa", owner)
    approval["lanes"] = [
        {"id": "lane_requester", "name": "Solicitante", "owner": "Solicitante", "orientation": "horizontal", "order": 1, "color": "#E8F5F0", "collapsed": False, "enabled": True, "height": 240},
        {"id": "lane_approver", "name": "Aprovador", "owner": "Aprovador", "orientation": "horizontal", "order": 2, "color": "#EAF4FF", "collapsed": False, "enabled": True, "height": 240},
    ]
    approval["nodes"] = [
        {"id": "n1", "type": "start", "laneId": "lane_requester", "position": {"x": 80, "y": 72}, "data": {"label": "Criar solicitação", "description": "Registrar dados e anexos.", "owner": "Solicitante", "enabled": True, "locked": False, "slaMinutes": 30, "tags": ["solicitação"], "level": "executive", "category": "approval", "criticality": "medium"}},
        {"id": "n2", "type": "task", "laneId": "lane_approver", "position": {"x": 370, "y": 315}, "data": {"label": "Analisar solicitação", "description": "Validar critérios e documentação.", "owner": "Aprovador", "enabled": True, "locked": False, "slaMinutes": 240, "tags": ["análise"], "level": "operational", "category": "approval", "criticality": "high"}},
        {"id": "n3", "type": "decision", "laneId": "lane_approver", "position": {"x": 660, "y": 310}, "data": {"label": "Aprovar?", "description": "Decidir aprovação ou devolução.", "owner": "Aprovador", "enabled": True, "locked": False, "slaMinutes": 60, "tags": ["decisão"], "level": "executive", "category": "approval", "criticality": "high"}},
        {"id": "n4", "type": "end", "laneId": "lane_requester", "position": {"x": 970, "y": 65}, "data": {"label": "Solicitação devolvida", "description": "Corrigir e reenviar.", "owner": "Solicitante", "enabled": True, "locked": False, "slaMinutes": None, "tags": ["exceção"], "level": "operational", "category": "exception", "criticality": "medium"}},
        {"id": "n5", "type": "end", "laneId": "lane_approver", "position": {"x": 970, "y": 315}, "data": {"label": "Solicitação aprovada", "description": "Encerrar aprovação.", "owner": "Aprovador", "enabled": True, "locked": False, "slaMinutes": None, "tags": ["sucesso"], "level": "executive", "category": "approval", "criticality": "medium"}},
    ]
    approval["edges"] = [
        {"id": "e1", "source": "n1", "target": "n2", "sourceHandle": "output", "targetHandle": "input", "type": "step", "label": "Enviar", "enabled": True, "condition": ""},
        {"id": "e2", "source": "n2", "target": "n3", "sourceHandle": "output", "targetHandle": "input", "type": "step", "label": "Analisado", "enabled": True, "condition": ""},
        {"id": "e3", "source": "n3", "target": "n5", "sourceHandle": "branch-0", "targetHandle": "input", "type": "step", "label": "Sim", "enabled": True, "condition": "Aprovado"},
        {"id": "e4", "source": "n3", "target": "n4", "sourceHandle": "branch-1", "targetHandle": "input", "type": "step", "label": "Não", "enabled": True, "condition": "Correção necessária"},
    ]

    webhook = new_flowchart_document("Integração com webhook", owner)
    webhook["lanes"] = [
        {"id": "lane_external", "name": "Sistema externo", "owner": "Fornecedor", "orientation": "horizontal", "order": 1, "color": "#F5F7FA", "collapsed": False, "enabled": True, "height": 230},
        {"id": "lane_core", "name": "Aplicação", "owner": "Produto", "orientation": "horizontal", "order": 2, "color": "#E8F5F0", "collapsed": False, "enabled": True, "height": 300},
    ]
    webhook["nodes"] = [
        {"id": "w1", "type": "start", "laneId": "lane_external", "position": {"x": 80, "y": 70}, "data": {"label": "Evento gerado", "description": "Fornecedor gera evento.", "owner": "Fornecedor", "enabled": True, "locked": False, "slaMinutes": None, "tags": ["webhook"], "level": "technical", "category": "integration", "criticality": "high"}},
        {"id": "w2", "type": "api", "laneId": "lane_core", "position": {"x": 360, "y": 300}, "data": {"label": "Receber webhook", "description": "Autenticar e persistir payload bruto.", "owner": "API", "enabled": True, "locked": False, "slaMinutes": 1, "tags": ["api", "idempotência"], "level": "technical", "category": "integration", "criticality": "critical"}},
        {"id": "w3", "type": "decision", "laneId": "lane_core", "position": {"x": 650, "y": 295}, "data": {"label": "Payload válido?", "description": "Validar assinatura e schema.", "owner": "API", "enabled": True, "locked": False, "slaMinutes": 1, "tags": ["validação"], "level": "technical", "category": "integration", "criticality": "critical"}},
        {"id": "w4", "type": "task", "laneId": "lane_core", "position": {"x": 940, "y": 260}, "data": {"label": "Processar evento", "description": "Executar regra idempotente.", "owner": "Worker", "enabled": True, "locked": False, "slaMinutes": 5, "tags": ["fila"], "level": "technical", "category": "integration", "criticality": "high"}},
        {"id": "w5", "type": "end", "laneId": "lane_core", "position": {"x": 1230, "y": 260}, "data": {"label": "Evento processado", "description": "Registrar sucesso.", "owner": "Worker", "enabled": True, "locked": False, "slaMinutes": None, "tags": ["sucesso"], "level": "technical", "category": "integration", "criticality": "medium"}},
        {"id": "w6", "type": "end", "laneId": "lane_core", "position": {"x": 940, "y": 390}, "data": {"label": "Evento rejeitado", "description": "Registrar motivo sem alterar o estado.", "owner": "API", "enabled": True, "locked": False, "slaMinutes": None, "tags": ["erro"], "level": "technical", "category": "exception", "criticality": "high"}},
    ]
    webhook["edges"] = [
        {"id": "we1", "source": "w1", "target": "w2", "sourceHandle": "output", "targetHandle": "input", "type": "step", "label": "HTTP", "enabled": True, "condition": ""},
        {"id": "we2", "source": "w2", "target": "w3", "sourceHandle": "output", "targetHandle": "input", "type": "step", "label": "Validar", "enabled": True, "condition": ""},
        {"id": "we3", "source": "w3", "target": "w4", "sourceHandle": "branch-0", "targetHandle": "input", "type": "step", "label": "Sim", "enabled": True, "condition": "Assinatura e schema válidos"},
        {"id": "we4", "source": "w3", "target": "w6", "sourceHandle": "branch-1", "targetHandle": "input", "type": "step", "label": "Não", "enabled": True, "condition": "Payload inválido"},
        {"id": "we5", "source": "w4", "target": "w5", "sourceHandle": "output", "targetHandle": "input", "type": "step", "label": "Concluído", "enabled": True, "condition": ""},
    ]

    return [
        {"id": "builtin_demo", "name": "Fluxo demonstrativo", "description": "Exemplo básico com decisão e raias.", "category": "Geral", "document": demo_flowchart_document(owner), "builtin": True},
        {"id": "builtin_approval", "name": "Aprovação corporativa", "description": "Solicitação, análise, decisão e retorno.", "category": "Governança", "document": approval, "builtin": True},
        {"id": "builtin_webhook", "name": "Integração com webhook", "description": "Recepção, validação, processamento e rejeição.", "category": "Tecnologia", "document": webhook, "builtin": True},
    ]


def clone_template(template: dict[str, Any], owner: str, name: str | None = None) -> dict[str, Any]:
    doc = deepcopy(template["document"])
    fresh = new_flowchart_document(name or template["name"], owner)
    flow = fresh["flow"]
    flow["description"] = str(doc.get("flow", {}).get("description") or template.get("description") or "")
    flow["tags"] = list(doc.get("flow", {}).get("tags", []))
    doc["flow"] = flow
    return doc
