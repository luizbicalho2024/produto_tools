from __future__ import annotations

import json
import unicodedata

import streamlit as st
import streamlit.components.v1 as components

from core.auth import render_account_sidebar, require_login
from core.styles import apply_global_styles, get_ui_theme, page_header
from services.project_repository import list_project_flows, list_projects, project_links

st.set_page_config(page_title="Mapa de Relações", page_icon="🕸️", layout="wide")
apply_global_styles(full_width=True)
user = require_login()
render_account_sidebar()

username = str(user["username"]).strip().lower()
is_admin = user.get("role") == "admin"
projects = list_projects(username, include_all=is_admin, is_admin=is_admin)

page_header(
    "Mapa de relações",
    "Explore fluxos e cards como um grafo vivo, com física, tela cheia, busca e filtros que destacam o contexto sem esconder as demais relações.",
)

if not projects:
    st.info("Crie ou importe um projeto para visualizar o mapa de relações.")
    st.stop()

project_by_id = {item["id"]: item for item in projects}
default_project = str(st.session_state.get("selected_project_id") or projects[0]["id"])
if default_project not in project_by_id:
    default_project = projects[0]["id"]
project_id = st.selectbox(
    "Projeto",
    list(project_by_id),
    index=list(project_by_id).index(default_project),
    format_func=lambda value: project_by_id[value]["name"],
)
st.session_state["selected_project_id"] = project_id

records = list_project_flows(project_id, username, is_admin=is_admin, include_documents=True)
if not records:
    st.info("O projeto selecionado ainda não possui fluxos.")
    st.stop()

flow_by_id = {item["id"]: item for item in records}
control_a, control_b, control_c = st.columns([1.2, 1.4, 1])
with control_a:
    scope = st.selectbox(
        "Escopo do grafo",
        ["project", "flow", "complete"],
        format_func=lambda value: {
            "project": "Fluxos do projeto",
            "flow": "Cards de um fluxo",
            "complete": "Projeto completo",
        }[value],
    )
with control_b:
    flow_id = st.selectbox(
        "Fluxo detalhado",
        list(flow_by_id),
        format_func=lambda value: flow_by_id[value]["name"],
        disabled=scope == "project",
    )
with control_c:
    max_nodes = st.select_slider(
        "Limite de cards",
        options=[100, 200, 350, 500, 750, 1000],
        value=500,
        disabled=scope == "project",
    )

role_colors = {
    "executive": "#00a35c",
    "operational": "#2563eb",
    "subprocess": "#7c3aed",
    "support": "#64748b",
}
type_colors = {
    "start": "#00a35c", "end": "#dc2626", "task": "#2563eb",
    "decision": "#d97706", "subprocess": "#7c3aed", "event": "#0891b2",
    "wait": "#64748b", "document": "#0f766e", "api": "#9333ea", "note": "#ca8a04",
}

nodes: list[dict] = []
links: list[dict] = []


def _semantic_text(value: object) -> str:
    normalized = unicodedata.normalize("NFD", str(value or ""))
    return "".join(char for char in normalized if unicodedata.category(char) != "Mn").lower().strip()


def decision_edge_semantic(document: dict, edge: dict) -> str:
    node_by_id = {str(item.get("id")): item for item in document.get("nodes", [])}
    source = node_by_id.get(str(edge.get("source"))) or {}
    if str(source.get("type") or "") != "decision":
        return "neutral"
    target = node_by_id.get(str(edge.get("target"))) or {}
    target_data = target.get("data") or {}
    content = _semantic_text(" ".join([
        str(edge.get("label") or ""),
        str(edge.get("condition") or ""),
        str(target_data.get("label") or ""),
        " ".join(str(tag) for tag in target_data.get("tags") or []),
    ]))
    negative = (
        "nao", "negativo", "negativa", "recus", "rejeit", "falha", "erro",
        "cancel", "inval", "indispon", "fora do", "bloque", "reprov", "expir",
        "sem saldo", "nao aprovado",
    )
    positive = (
        "sim", "positivo", "positiva", "aprov", "aceit", "sucesso", "conclu",
        "valido", "valida", "disponivel", "dentro da", "ativo", "ativa", "pago",
        "confirmado",
    )
    if any(token in content for token in negative):
        return "negative"
    if any(token in content for token in positive):
        return "positive"
    return "neutral"


def add_flow_node(record: dict, *, node_id: str | None = None, radius: float = 12) -> None:
    nodes.append({
        "id": node_id or record["id"],
        "label": record["name"],
        "kind": "flow",
        "group": record.get("project_role") or "subprocess",
        "nodeType": "flow",
        "color": role_colors.get(record.get("project_role"), "#486581"),
        "radius": radius,
        "details": f"{record.get('project_role') or 'fluxo'} · v{record.get('current_version', 1)}",
        "flowId": record["id"],
        "flowName": record["name"],
        "lane": "",
        "owner": record.get("owner_username") or "",
        "criticality": "",
        "tags": record.get("tags") or [],
    })


def add_card_node(record: dict, node: dict, graph_id: str) -> None:
    data = node.get("data") or {}
    node_type = str(node.get("type") or "task")
    lane_names = {str(item.get("id")): str(item.get("name") or "Sem raia") for item in (record.get("document") or {}).get("lanes", [])}
    lane_name = lane_names.get(str(node.get("laneId")), "Sem raia")
    nodes.append({
        "id": graph_id,
        "label": str(data.get("label") or node.get("id")),
        "kind": "node",
        "group": node_type,
        "nodeType": node_type,
        "color": type_colors.get(node_type, "#486581"),
        "radius": 9 if node_type in {"start", "end", "decision", "subprocess"} else 6,
        "details": f"{lane_name} · {data.get('owner') or 'Sem responsável'}",
        "flowId": record["id"],
        "flowName": record["name"],
        "lane": lane_name,
        "owner": str(data.get("owner") or ""),
        "criticality": str(data.get("criticality") or "medium"),
        "tags": [str(tag) for tag in data.get("tags") or []],
    })


if scope == "project":
    graph = project_links(project_id, username, is_admin=is_admin)
    for record in records:
        add_flow_node(
            record,
            radius=15 if record["id"] == project_by_id[project_id].get("default_flow_id") else 11,
        )
    for item in graph.get("links", []):
        if item["source_flow_id"] in flow_by_id and item["target_flow_id"] in flow_by_id:
            links.append({
                "source": item["source_flow_id"],
                "target": item["target_flow_id"],
                "label": item.get("source_node_label") or "subprocesso",
                "kind": "flow-link",
            })
elif scope == "flow":
    record = flow_by_id[flow_id]
    document = record.get("document") or {}
    visible_nodes = document.get("nodes", [])[:max_nodes]
    visible_ids = {str(item.get("id")) for item in visible_nodes}
    for node in visible_nodes:
        add_card_node(record, node, str(node.get("id")))
    for edge in document.get("edges", []):
        if str(edge.get("source")) in visible_ids and str(edge.get("target")) in visible_ids and edge.get("enabled", True):
            links.append({
                "source": str(edge.get("source")),
                "target": str(edge.get("target")),
                "label": str(edge.get("label") or edge.get("condition") or ""),
                "kind": "edge",
                "semantic": decision_edge_semantic(document, edge),
            })
else:
    graph = project_links(project_id, username, is_admin=is_admin)
    remaining = int(max_nodes)
    for record in records:
        flow_node_id = f"flow::{record['id']}"
        add_flow_node(record, node_id=flow_node_id, radius=14)
        document = record.get("document") or {}
        flow_nodes = document.get("nodes", [])[:remaining]
        remaining -= len(flow_nodes)
        ids = {str(item.get("id")) for item in flow_nodes}
        for node in flow_nodes:
            graph_id = f"node::{record['id']}::{node.get('id')}"
            add_card_node(record, node, graph_id)
            links.append({"source": flow_node_id, "target": graph_id, "label": "", "kind": "membership"})
        for edge in document.get("edges", []):
            source, target = str(edge.get("source")), str(edge.get("target"))
            if source in ids and target in ids and edge.get("enabled", True):
                links.append({
                    "source": f"node::{record['id']}::{source}",
                    "target": f"node::{record['id']}::{target}",
                    "label": str(edge.get("label") or edge.get("condition") or ""),
                    "kind": "edge",
                    "semantic": decision_edge_semantic(document, edge),
                })
        if remaining <= 0:
            break
    for item in graph.get("links", []):
        source = f"flow::{item['source_flow_id']}"
        target = f"flow::{item['target_flow_id']}"
        if any(node["id"] == source for node in nodes) and any(node["id"] == target for node in nodes):
            links.append({"source": source, "target": target, "label": item.get("source_node_label") or "", "kind": "flow-link"})

payload = json.dumps({"nodes": nodes, "links": links}, ensure_ascii=False).replace("</", "<\\/")
dark = get_ui_theme() == "dark"
bg = "#00141f" if dark else "#f4f8fb"
text_color = "#f5fbf7" if dark else "#102a43"
muted = "#9bc3b3" if dark else "#486581"
edge_color = "rgba(155,195,179,.34)" if dark else "rgba(72,101,129,.26)"
panel = "rgba(0,30,43,.92)" if dark else "rgba(255,255,255,.94)"
dim_edge = "rgba(155,195,179,.035)" if dark else "rgba(72,101,129,.035)"

html = f"""
<!doctype html><html><head><meta charset="utf-8"><style>
:root{{color-scheme:{'dark' if dark else 'light'}}}
*{{box-sizing:border-box}}
html,body{{margin:0;width:100%;height:100%;overflow:hidden;background:{bg};font-family:Inter,Arial,sans-serif;color:{text_color}}}
#wrap{{position:relative;width:100%;height:780px;background:{bg};border:1px solid rgba(120,150,140,.25);border-radius:18px;overflow:hidden}}
#wrap.pseudo-fullscreen{{position:fixed;inset:0;z-index:99999;width:100vw;height:100vh;border:0;border-radius:0}}
#wrap:fullscreen{{width:100vw;height:100vh;border:0;border-radius:0}}
canvas{{width:100%;height:100%;display:block;cursor:grab}}canvas.dragging{{cursor:grabbing}}
.tools{{position:absolute;top:12px;left:12px;right:12px;display:flex;flex-wrap:wrap;gap:7px;align-items:center;z-index:3;pointer-events:none}}
.tools>*{{pointer-events:auto}}
input,select{{height:37px;background:{panel};color:{text_color};border:1px solid rgba(120,150,140,.35);border-radius:999px;padding:0 13px;outline:none}}
input{{width:min(270px,36vw)}}select{{max-width:210px}}
button{{height:37px;background:{panel};color:{text_color};border:1px solid rgba(120,150,140,.35);border-radius:999px;padding:0 13px;font-weight:700;cursor:pointer}}
button:hover,button.active{{border-color:#00a35c;color:#00a35c;background:{panel}}}
.info{{position:absolute;left:14px;bottom:14px;max-width:min(480px,72vw);background:{panel};border:1px solid rgba(120,150,140,.3);border-radius:14px;padding:10px 12px;z-index:3;backdrop-filter:blur(10px)}}
.info strong{{display:block;margin-bottom:3px}}.info small{{color:{muted};line-height:1.35}}.counter{{margin-left:auto;background:{panel};padding:8px 12px;border-radius:999px;color:{muted};font-size:12px;white-space:nowrap}}
.legend{{position:absolute;right:14px;bottom:14px;background:{panel};border-radius:14px;padding:9px 11px;color:{muted};font-size:11px;z-index:3}}
.filter-note{{position:absolute;right:14px;top:62px;max-width:310px;background:{panel};border-radius:12px;padding:8px 10px;color:{muted};font-size:10px;z-index:3;opacity:.92}}
@media(max-width:900px){{.counter{{margin-left:0}}.legend{{display:none}}.filter-note{{top:auto;bottom:72px}}}}
</style></head><body><div id="wrap"><canvas id="graph"></canvas>
<div class="tools">
  <input id="search" placeholder="Buscar card, fluxo, responsável ou tag">
  <select id="typeFilter" title="Destacar por tipo"><option value="all">Todos os tipos</option></select>
  <select id="flowFilter" title="Destacar por fluxo"><option value="all">Todos os fluxos</option></select>
  <button id="isolate" title="Ocultar temporariamente os pontos que não atendem aos filtros">Destacar</button>
  <button id="explode">Explodir</button><button id="fit">Centralizar</button><button id="pause">Pausar</button><button id="labels">Rótulos</button>
  <button id="fullscreen" title="Usar todo o monitor">⛶ Tela cheia</button>
  <span class="counter" id="counter">{len(nodes)} nós · {len(links)} ligações</span>
</div>
<div class="filter-note" id="filterNote">Os filtros mantêm os demais pontos visíveis em segundo plano. Use “Destacar” para isolar apenas os resultados.</div>
<div class="info" id="info"><strong>Mapa interativo</strong><small>Arraste nós, use o scroll para zoom, arraste o fundo para mover e clique em um ponto para destacar sua vizinhança.</small></div>
<div class="legend">Cor do ponto = tipo · Verde = Sim/positivo · Vermelho = Não/negativo · Seta = direção</div></div>
<script>
const DATA={payload};
const canvas=document.getElementById('graph'),ctx=canvas.getContext('2d'),wrap=document.getElementById('wrap');
const typeLabels={{flow:'Fluxos',start:'Inícios',end:'Finais',task:'Atividades',decision:'Decisões',subprocess:'Subprocessos',event:'Eventos',wait:'Esperas',document:'Documentos',api:'APIs e integrações',note:'Observações'}};
let width=0,height=0,dpr=1,zoom=1,panX=0,panY=0,paused=false,showLabels=true,selected=null,hovered=null,drag=null,panning=null;
let typeFilter='all',flowFilter='all',query='',isolated=false;
const nodes=DATA.nodes.map((n,i)=>({{...n,x:Math.cos(i*2.399)*Math.sqrt(i+1)*28,y:Math.sin(i*2.399)*Math.sqrt(i+1)*28,vx:0,vy:0}}));
const byId=new Map(nodes.map(n=>[n.id,n]));
const links=DATA.links.map(l=>({{...l,source:byId.get(l.source),target:byId.get(l.target)}})).filter(l=>l.source&&l.target);
const adjacency=new Map(nodes.map(n=>[n.id,new Set()]));links.forEach(l=>{{adjacency.get(l.source.id).add(l.target.id);adjacency.get(l.target.id).add(l.source.id)}});
function fillFilters(){{
  const type=document.getElementById('typeFilter'),flow=document.getElementById('flowFilter');
  [...new Set(nodes.map(n=>n.nodeType||n.group).filter(Boolean))].sort().forEach(value=>{{const o=document.createElement('option');o.value=value;o.textContent=typeLabels[value]||value;type.appendChild(o)}});
  [...new Map(nodes.filter(n=>n.flowId).map(n=>[n.flowId,n.flowName||n.flowId])).entries()].sort((a,b)=>a[1].localeCompare(b[1])).forEach(([value,label])=>{{const o=document.createElement('option');o.value=value;o.textContent=label;flow.appendChild(o)}});
}}fillFilters();
function resize(){{dpr=Math.min(2,devicePixelRatio||1);width=wrap.clientWidth;height=wrap.clientHeight;canvas.width=width*dpr;canvas.height=height*dpr;ctx.setTransform(dpr,0,0,dpr,0,0)}}new ResizeObserver(resize).observe(wrap);resize();
function screen(n){{return{{x:width/2+panX+n.x*zoom,y:height/2+panY+n.y*zoom}}}}function world(x,y){{return{{x:(x-width/2-panX)/zoom,y:(y-height/2-panY)/zoom}}}}
function textOf(n){{return [n.label,n.id,n.flowName,n.lane,n.owner,n.criticality,...(n.tags||[])].join(' ').toLowerCase()}}
function matchesFilters(n){{
  if(typeFilter!=='all'&&(n.nodeType||n.group)!==typeFilter)return false;
  if(flowFilter!=='all'&&n.flowId!==flowFilter)return false;
  if(query&&!textOf(n).includes(query))return false;
  return true;
}}
function selectedActive(id){{if(!selected)return true;return id===selected||adjacency.get(selected)?.has(id)}}
function visible(n){{return !isolated||matchesFilters(n)}}
function highlighted(n){{return matchesFilters(n)&&selectedActive(n.id)}}
function updateCounter(){{const matched=nodes.filter(matchesFilters).length;document.getElementById('counter').textContent=`${{matched}} em destaque · ${{nodes.length}} nós · ${{links.length}} ligações`;document.getElementById('isolate').textContent=isolated?'Mostrar todos':'Destacar';document.getElementById('isolate').classList.toggle('active',isolated)}}
function step(){{if(paused||drag)return;const activeNodes=nodes.filter(visible),count=activeNodes.length;const repel=count>600?3200:count>300?4500:6500;for(let i=0;i<count;i++){{const a=activeNodes[i];for(let j=i+1;j<count;j++){{const b=activeNodes[j],dx=a.x-b.x,dy=a.y-b.y,d2=Math.max(90,dx*dx+dy*dy),f=repel/d2,inv=1/Math.sqrt(d2);a.vx+=dx*inv*f;a.vy+=dy*inv*f;b.vx-=dx*inv*f;b.vy-=dy*inv*f}}}}links.forEach(l=>{{if(!visible(l.source)||!visible(l.target))return;const dx=l.target.x-l.source.x,dy=l.target.y-l.source.y,d=Math.max(1,Math.hypot(dx,dy)),ideal=l.kind==='membership'?70:l.kind==='flow-link'?180:105,f=(d-ideal)*.0045,ux=dx/d,uy=dy/d;l.source.vx+=ux*f;l.source.vy+=uy*f;l.target.vx-=ux*f;l.target.vy-=uy*f}});activeNodes.forEach(n=>{{n.vx+=-n.x*.00045;n.vy+=-n.y*.00045;n.vx*=.86;n.vy*=.86;n.x+=n.vx;n.y+=n.vy}})}}
function semanticColor(l){{if(l.semantic==='positive')return '#16a34a';if(l.semantic==='negative')return '#dc2626';return '{edge_color}'}}
function drawArrow(a,b,color,alpha){{const dx=b.x-a.x,dy=b.y-a.y,d=Math.max(1,Math.hypot(dx,dy)),ux=dx/d,uy=dy/d,targetRadius=6,tipX=b.x-ux*targetRadius,tipY=b.y-uy*targetRadius,size=Math.max(3.5,Math.min(7,5*zoom));ctx.globalAlpha=alpha;ctx.fillStyle=color;ctx.beginPath();ctx.moveTo(tipX,tipY);ctx.lineTo(tipX-ux*size-uy*size*.65,tipY-uy*size+ux*size*.65);ctx.lineTo(tipX-ux*size+uy*size*.65,tipY-uy*size-ux*size*.65);ctx.closePath();ctx.fill()}}
function draw(){{ctx.clearRect(0,0,width,height);ctx.save();links.forEach(l=>{{if(!visible(l.source)||!visible(l.target))return;const a=screen(l.source),b=screen(l.target),on=highlighted(l.source)&&highlighted(l.target),color=semanticColor(l),alpha=on?1:.11;ctx.globalAlpha=alpha;ctx.strokeStyle=l.semantic&&l.semantic!=='neutral'?color:(on?'{edge_color}':'{dim_edge}');ctx.lineWidth=(l.kind==='flow-link'?2.2:(l.semantic&&l.semantic!=='neutral'?1.8:1))*Math.max(.65,Math.min(1.5,zoom));ctx.beginPath();ctx.moveTo(a.x,a.y);ctx.lineTo(b.x,b.y);ctx.stroke();if(l.kind!=='membership')drawArrow(a,b,color,alpha)}});nodes.forEach(n=>{{if(!visible(n))return;const p=screen(n),on=highlighted(n),r=Math.max(2.3,n.radius*zoom);ctx.globalAlpha=on?1:.09;ctx.fillStyle=n.color;ctx.beginPath();ctx.arc(p.x,p.y,r,0,Math.PI*2);ctx.fill();if(n.id===selected||n.id===hovered){{ctx.globalAlpha=1;ctx.strokeStyle='#ffffff';ctx.lineWidth=2.2;ctx.stroke()}}if(showLabels&&(zoom>.72||n.kind==='flow'||n.id===selected||n.id===hovered||matchesFilters(n)&&query)){{ctx.globalAlpha=on?1:.16;ctx.fillStyle='{text_color}';ctx.font=`${{n.kind==='flow'?'700 ':'600 '}}${{Math.max(10,Math.min(14,11*zoom))}}px Inter,Arial`;ctx.textAlign='center';ctx.fillText(n.label,p.x,p.y+r+13)}}}});ctx.restore();ctx.globalAlpha=1}}
function frame(){{step();draw();requestAnimationFrame(frame)}}frame();
function hit(x,y){{let best=null,dist=Infinity;nodes.forEach(n=>{{if(!visible(n))return;const p=screen(n),d=Math.hypot(p.x-x,p.y-y),r=Math.max(8,n.radius*zoom+5);if(d<r&&d<dist){{best=n;dist=d}}}});return best}}
function showInfo(n){{document.getElementById('info').innerHTML=n?`<strong>${{n.label}}</strong><small>${{n.flowName||n.group||''}}${{n.lane?' · '+n.lane:''}}${{n.owner?' · '+n.owner:''}} · ${{adjacency.get(n.id)?.size||0}} ligação(ões)</small>`:'<strong>Mapa interativo</strong><small>Arraste nós, use o scroll para zoom, arraste o fundo para mover e clique em um ponto para destacar sua vizinhança.</small>'}}
canvas.addEventListener('pointerdown',e=>{{const n=hit(e.offsetX,e.offsetY);canvas.setPointerCapture(e.pointerId);if(n){{drag=n;selected=n.id;const w=world(e.offsetX,e.offsetY);drag.dx=n.x-w.x;drag.dy=n.y-w.y;showInfo(n)}}else{{panning={{x:e.clientX,y:e.clientY,px:panX,py:panY}};selected=null;showInfo(null)}}canvas.classList.add('dragging')}});
canvas.addEventListener('pointermove',e=>{{hovered=hit(e.offsetX,e.offsetY)?.id||null;if(drag){{const w=world(e.offsetX,e.offsetY);drag.x=w.x+drag.dx;drag.y=w.y+drag.dy;drag.vx=drag.vy=0}}else if(panning){{panX=panning.px+e.clientX-panning.x;panY=panning.py+e.clientY-panning.y}}}});
canvas.addEventListener('pointerup',()=>{{drag=null;panning=null;canvas.classList.remove('dragging')}});canvas.addEventListener('pointerleave',()=>{{hovered=null}});
canvas.addEventListener('wheel',e=>{{e.preventDefault();const before=world(e.offsetX,e.offsetY),factor=e.deltaY<0?1.12:.89;zoom=Math.max(.15,Math.min(4,zoom*factor));const after=world(e.offsetX,e.offsetY);panX+=(after.x-before.x)*zoom;panY+=(after.y-before.y)*zoom}},{{passive:false}});
document.getElementById('explode').onclick=()=>{{const active=nodes.filter(visible);active.forEach((n,i)=>{{const a=i/Math.max(1,active.length)*Math.PI*2,r=120+Math.sqrt(i+1)*24;n.x=Math.cos(a)*r;n.y=Math.sin(a)*r;n.vx=Math.cos(a)*8;n.vy=Math.sin(a)*8}});paused=false;document.getElementById('pause').textContent='Pausar'}};
document.getElementById('fit').onclick=()=>{{const active=nodes.filter(visible);if(!active.length)return;const minX=Math.min(...active.map(n=>n.x)),maxX=Math.max(...active.map(n=>n.x)),minY=Math.min(...active.map(n=>n.y)),maxY=Math.max(...active.map(n=>n.y));const spanX=Math.max(100,maxX-minX),spanY=Math.max(100,maxY-minY);zoom=Math.max(.15,Math.min(1.4,Math.min((width-100)/spanX,(height-120)/spanY)));panX=-(minX+maxX)/2*zoom;panY=-(minY+maxY)/2*zoom;selected=null;showInfo(null)}};
document.getElementById('pause').onclick=e=>{{paused=!paused;e.target.textContent=paused?'Continuar':'Pausar'}};document.getElementById('labels').onclick=()=>{{showLabels=!showLabels}};
document.getElementById('typeFilter').onchange=e=>{{typeFilter=e.target.value;selected=null;updateCounter()}};document.getElementById('flowFilter').onchange=e=>{{flowFilter=e.target.value;selected=null;updateCounter()}};
document.getElementById('isolate').onclick=()=>{{isolated=!isolated;selected=null;updateCounter();setTimeout(()=>document.getElementById('fit').click(),30)}};
document.getElementById('search').addEventListener('input',e=>{{query=e.target.value.trim().toLowerCase();selected=null;const result=nodes.find(matchesFilters);if(result&&query){{selected=result.id;panX=-result.x*zoom;panY=-result.y*zoom;showInfo(result)}}else showInfo(null);updateCounter()}});
const fsButton=document.getElementById('fullscreen');
function updateFullscreen(){{const active=document.fullscreenElement===wrap||wrap.classList.contains('pseudo-fullscreen');fsButton.textContent=active?'⛶ Sair da tela cheia':'⛶ Tela cheia'}}
fsButton.onclick=async()=>{{if(document.fullscreenElement===wrap){{await document.exitFullscreen();return}}if(wrap.classList.contains('pseudo-fullscreen')){{wrap.classList.remove('pseudo-fullscreen');updateFullscreen();resize();return}}try{{await wrap.requestFullscreen()}}catch(e){{wrap.classList.add('pseudo-fullscreen');updateFullscreen();resize()}}}};
document.addEventListener('fullscreenchange',()=>{{updateFullscreen();resize()}});document.addEventListener('keydown',e=>{{if(e.key==='Escape'&&wrap.classList.contains('pseudo-fullscreen')){{wrap.classList.remove('pseudo-fullscreen');updateFullscreen();resize()}}}});
updateCounter();
</script></body></html>
"""
components.html(html, height=800, scrolling=False)

st.caption("O mapa usa física local no navegador; os filtros e as posições não alteram o layout oficial dos fluxos.")
open_col, graph_col = st.columns([2, 1])
with open_col:
    open_flow_id = st.selectbox(
        "Abrir fluxo no editor",
        list(flow_by_id),
        format_func=lambda value: flow_by_id[value]["name"],
        key="graph_open_flow",
    )
with graph_col:
    st.write("")
    st.write("")
    if st.button("Abrir no editor", type="primary", use_container_width=True):
        st.session_state["selected_project_id"] = project_id
        st.session_state["selected_flowchart_id"] = open_flow_id
        st.switch_page("pages/5_Editor_de_Fluxos.py")
