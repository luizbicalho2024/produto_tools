from __future__ import annotations

from services.enterprise_tools import (
    bpmn_export,
    bpmn_import,
    dmn_evaluate,
    flow_from_text,
    pareto_rows,
    simulate_process,
    sipoc_from_document,
    vsm_metrics,
)

def test_flow_from_text_and_bpmn_roundtrip():
    doc = flow_from_text("Receber pedido\nValidar pedido\nConcluir", "tester")
    assert len(doc["nodes"]) == 5
    assert len(doc["edges"]) == 4
    xml = bpmn_export(doc)
    imported = bpmn_import(xml, "tester")
    assert imported["flow"]["name"] == doc["flow"]["name"]
    assert len(imported["nodes"]) == len(doc["nodes"])

def test_sipoc_and_simulation():
    doc = flow_from_text("Analisar\nExecutar", "tester")
    for node in doc["nodes"]:
        if node["type"] == "task":
            node["data"]["slaMinutes"] = 10
    sipoc = sipoc_from_document(doc)
    assert sipoc["Process"]
    result = simulate_process(doc, iterations=100, hourly_cost=60, variability_percent=0)
    assert result["iterations"] == 100
    assert result["mean_minutes"] == 20
    assert result["mean_cost"] == 20

def test_dmn_pareto_and_vsm():
    rules = [
        {"when": {"tipo": "VIP"}, "then": {"resultado": "prioridade"}},
        {"when": {"tipo": "*"}, "then": {"resultado": "normal"}},
    ]
    assert dmn_evaluate(rules, {"tipo": "VIP"})["resultado"] == "prioridade"
    rows = pareto_rows(["A", "B", "A"])
    assert rows[0]["Causa"] == "A"
    metrics = vsm_metrics([{"va_minutes": 10, "wait_minutes": 30}])
    assert metrics["lead_time_minutes"] == 40
    assert metrics["flow_efficiency_percent"] == 25