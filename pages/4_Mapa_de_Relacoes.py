from __future__ import annotations

import json
from html import escape

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
    "Visualização dinâmica inspirada no grafo do Obsidian, com física, expansão, busca, zoom e destaque de vizinhança.",
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

if scope == "project":
    graph = project_links(project_id, username, is_admin=is_admin)
    for record in records:
        nodes.append({
            "id": record["id"],
            "label": record["name"],
            "kind": "flow",
            "group": record.get("project_role") or "subprocess",
            "color": role_colors.get(record.get("project_role"), "#486581"),
            "radius": 15 if record["id"] == project_by_id[project_id].get("default_flow_id") else 11,
            "details": f"{record.get('project_role') or 'fluxo'} · v{record.get('current_version', 1)}",
            "flowId": record["id"],
        })
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
    lane_names = {str(item.get("id")): str(item.get("name") or "Sem raia") for item in document.get("lanes", [])}
    visible_nodes = document.get("nodes", [])[:max_nodes]
    visible_ids = {str(item.get("id")) for item in visible_nodes}
    for node in visible_nodes:
        data = node.get("data") or {}
        node_type = str(node.get("type") or "task")
        nodes.append({
            "id": str(node.get("id")),
            "label": str(data.get("label") or node.get("id")),
            "kind": "node",
            "group": node_type,
            "color": type_colors.get(node_type, "#486581"),
            "radius": 9 if node_type in {"start", "end", "decision", "subprocess"} else 6,
            "details": f"{lane_names.get(str(node.get('laneId')), 'Sem raia')} · {data.get('owner') or 'Sem responsável'}",
            "flowId": flow_id,
        })
    for edge in document.get("edges", []):
        if str(edge.get("source")) in visible_ids and str(edge.get("target")) in visible_ids and edge.get("enabled", True):
            links.append({
                "source": str(edge.get("source")),
                "target": str(edge.get("target")),
                "label": str(edge.get("label") or edge.get("condition") or ""),
                "kind": "edge",
            })
else:
    graph = project_links(project_id, username, is_admin=is_admin)
    remaining = int(max_nodes)
    for record in records:
        flow_node_id = f"flow::{record['id']}"
        nodes.append({
            "id": flow_node_id,
            "label": record["name"],
            "kind": "flow",
            "group": record.get("project_role") or "subprocess",
            "color": role_colors.get(record.get("project_role"), "#486581"),
            "radius": 14,
            "details": f"Fluxo · {record.get('project_role') or 'subprocesso'}",
            "flowId": record["id"],
        })
        document = record.get("document") or {}
        flow_nodes = document.get("nodes", [])[:remaining]
        remaining -= len(flow_nodes)
        ids = {str(item.get("id")) for item in flow_nodes}
        for node in flow_nodes:
            data = node.get("data") or {}
            node_type = str(node.get("type") or "task")
            graph_id = f"node::{record['id']}::{node.get('id')}"
            nodes.append({
                "id": graph_id,
                "label": str(data.get("label") or node.get("id")),
                "kind": "node",
                "group": node_type,
                "color": type_colors.get(node_type, "#486581"),
                "radius": 5.5,
                "details": record["name"],
                "flowId": record["id"],
            })
            links.append({"source": flow_node_id, "target": graph_id, "label": "", "kind": "membership"})
        for edge in document.get("edges", []):
            source, target = str(edge.get("source")), str(edge.get("target"))
            if source in ids and target in ids and edge.get("enabled", True):
                links.append({
                    "source": f"node::{record['id']}::{source}",
                    "target": f"node::{record['id']}::{target}",
                    "label": "",
                    "kind": "edge",
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
panel = "rgba(0,30,43,.88)" if dark else "rgba(255,255,255,.92)"

html = f"""
<!doctype html><html><head><meta charset="utf-8"><style>
html,body{{margin:0;width:100%;height:100%;overflow:hidden;background:{bg};font-family:Inter,Arial,sans-serif;color:{text_color}}}
#wrap{{position:relative;width:100%;height:780px;background:{bg};border:1px solid rgba(120,150,140,.25);border-radius:18px;overflow:hidden}}
canvas{{width:100%;height:100%;display:block;cursor:grab}}canvas.dragging{{cursor:grabbing}}
.tools{{position:absolute;top:12px;left:12px;right:12px;display:flex;gap:8px;align-items:center;z-index:3;pointer-events:none}}
.tools>*{{pointer-events:auto}}input{{width:260px;background:{panel};color:{text_color};border:1px solid rgba(120,150,140,.35);border-radius:999px;padding:9px 14px;outline:none}}
button{{background:{panel};color:{text_color};border:1px solid rgba(120,150,140,.35);border-radius:999px;padding:9px 13px;font-weight:700;cursor:pointer}}button:hover{{border-color:#00a35c;color:#00a35c}}
.info{{position:absolute;left:14px;bottom:14px;max-width:420px;background:{panel};border:1px solid rgba(120,150,140,.3);border-radius:14px;padding:10px 12px;z-index:3;backdrop-filter:blur(10px)}}
.info strong{{display:block;margin-bottom:3px}}.info small{{color:{muted}}}.counter{{margin-left:auto;background:{panel};padding:8px 12px;border-radius:999px;color:{muted};font-size:12px}}
.legend{{position:absolute;right:14px;bottom:14px;background:{panel};border-radius:14px;padding:9px 11px;color:{muted};font-size:11px;z-index:3}}
</style></head><body><div id="wrap"><canvas id="graph"></canvas>
<div class="tools"><input id="search" placeholder="Buscar nó ou fluxo"><button id="explode">Explodir</button><button id="fit">Centralizar</button><button id="pause">Pausar</button><button id="labels">Rótulos</button><span class="counter">{len(nodes)} nós · {len(links)} ligações</span></div>
<div class="info" id="info"><strong>Mapa interativo</strong><small>Arraste nós, use o scroll para zoom, arraste o fundo para mover e clique para destacar vizinhos.</small></div>
<div class="legend">Clique destaca vizinhos · Arraste o fundo para mover</div></div>
<script>
const DATA={payload};
const canvas=document.getElementById('graph'),ctx=canvas.getContext('2d'),wrap=document.getElementById('wrap');
let width=0,height=0,dpr=1,zoom=1,panX=0,panY=0,paused=false,showLabels=true,selected=null,hovered=null,drag=null,panning=null;
const nodes=DATA.nodes.map((n,i)=>({{...n,x:Math.cos(i*2.399)*Math.sqrt(i+1)*28,y:Math.sin(i*2.399)*Math.sqrt(i+1)*28,vx:0,vy:0}}));
const byId=new Map(nodes.map(n=>[n.id,n]));
const links=DATA.links.map(l=>({{...l,source:byId.get(l.source),target:byId.get(l.target)}})).filter(l=>l.source&&l.target);
const adjacency=new Map(nodes.map(n=>[n.id,new Set()]));links.forEach(l=>{{adjacency.get(l.source.id).add(l.target.id);adjacency.get(l.target.id).add(l.source.id)}});
function resize(){{dpr=Math.min(2,devicePixelRatio||1);width=wrap.clientWidth;height=wrap.clientHeight;canvas.width=width*dpr;canvas.height=height*dpr;ctx.setTransform(dpr,0,0,dpr,0,0)}}new ResizeObserver(resize).observe(wrap);resize();
function screen(n){{return{{x:width/2+panX+n.x*zoom,y:height/2+panY+n.y*zoom}}}}function world(x,y){{return{{x:(x-width/2-panX)/zoom,y:(y-height/2-panY)/zoom}}}}
function step(){{if(paused||drag)return;const count=nodes.length;const repel=count>600?3200:count>300?4500:6500;for(let i=0;i<count;i++){{const a=nodes[i];for(let j=i+1;j<count;j++){{const b=nodes[j],dx=a.x-b.x,dy=a.y-b.y,d2=Math.max(90,dx*dx+dy*dy),f=repel/d2,inv=1/Math.sqrt(d2);a.vx+=dx*inv*f;a.vy+=dy*inv*f;b.vx-=dx*inv*f;b.vy-=dy*inv*f}}}}links.forEach(l=>{{const dx=l.target.x-l.source.x,dy=l.target.y-l.source.y,d=Math.max(1,Math.hypot(dx,dy)),ideal=l.kind==='membership'?70:l.kind==='flow-link'?180:105,f=(d-ideal)*.0045,ux=dx/d,uy=dy/d;l.source.vx+=ux*f;l.source.vy+=uy*f;l.target.vx-=ux*f;l.target.vy-=uy*f}});nodes.forEach(n=>{{n.vx+=-n.x*.00045;n.vy+=-n.y*.00045;n.vx*=.86;n.vy*=.86;n.x+=n.vx;n.y+=n.vy}})}}
function active(id){{if(!selected)return true;return id===selected||adjacency.get(selected)?.has(id)}}
function draw(){{ctx.clearRect(0,0,width,height);ctx.save();links.forEach(l=>{{const a=screen(l.source),b=screen(l.target),on=!selected||(active(l.source.id)&&active(l.target.id));ctx.strokeStyle=on?'{edge_color}':'rgba(120,130,130,.05)';ctx.lineWidth=(l.kind==='flow-link'?2.2:1)*Math.max(.65,Math.min(1.5,zoom));ctx.beginPath();ctx.moveTo(a.x,a.y);ctx.lineTo(b.x,b.y);ctx.stroke()}});nodes.forEach(n=>{{const p=screen(n),on=active(n.id),r=Math.max(2.3,n.radius*zoom);ctx.globalAlpha=on?1:.11;ctx.fillStyle=n.color;ctx.beginPath();ctx.arc(p.x,p.y,r,0,Math.PI*2);ctx.fill();if(n.id===selected||n.id===hovered){{ctx.strokeStyle='#ffffff';ctx.lineWidth=2;ctx.stroke()}}if(showLabels&&(zoom>.72||n.kind==='flow'||n.id===selected||n.id===hovered)){{ctx.globalAlpha=on?1:.18;ctx.fillStyle='{text_color}';ctx.font=`${{n.kind==='flow'?'700 ':'600 '}}${{Math.max(10,Math.min(14,11*zoom))}}px Inter,Arial`;ctx.textAlign='center';ctx.fillText(n.label,p.x,p.y+r+13)}}}});ctx.restore();ctx.globalAlpha=1}}
function frame(){{step();draw();requestAnimationFrame(frame)}}frame();
function hit(x,y){{let best=null,dist=Infinity;nodes.forEach(n=>{{const p=screen(n),d=Math.hypot(p.x-x,p.y-y),r=Math.max(8,n.radius*zoom+5);if(d<r&&d<dist){{best=n;dist=d}}}});return best}}
canvas.addEventListener('pointerdown',e=>{{const n=hit(e.offsetX,e.offsetY);canvas.setPointerCapture(e.pointerId);if(n){{drag=n;selected=n.id;const w=world(e.offsetX,e.offsetY);drag.dx=n.x-w.x;drag.dy=n.y-w.y;document.getElementById('info').innerHTML=`<strong>${{n.label}}</strong><small>${{n.details||n.group||''}} · ${{adjacency.get(n.id)?.size||0}} ligação(ões)</small>`}}else{{panning={{x:e.clientX,y:e.clientY,px:panX,py:panY}};selected=null}}canvas.classList.add('dragging')}});
canvas.addEventListener('pointermove',e=>{{hovered=hit(e.offsetX,e.offsetY)?.id||null;if(drag){{const w=world(e.offsetX,e.offsetY);drag.x=w.x+drag.dx;drag.y=w.y+drag.dy;drag.vx=drag.vy=0}}else if(panning){{panX=panning.px+e.clientX-panning.x;panY=panning.py+e.clientY-panning.y}}}});
canvas.addEventListener('pointerup',e=>{{drag=null;panning=null;canvas.classList.remove('dragging')}});canvas.addEventListener('pointerleave',()=>{{hovered=null}});
canvas.addEventListener('wheel',e=>{{e.preventDefault();const before=world(e.offsetX,e.offsetY),factor=e.deltaY<0?1.12:.89;zoom=Math.max(.15,Math.min(4,zoom*factor));const after=world(e.offsetX,e.offsetY);panX+=(after.x-before.x)*zoom;panY+=(after.y-before.y)*zoom}},{{passive:false}});
document.getElementById('explode').onclick=()=>{{nodes.forEach((n,i)=>{{const a=i/nodes.length*Math.PI*2,r=120+Math.sqrt(i+1)*24;n.x=Math.cos(a)*r;n.y=Math.sin(a)*r;n.vx=Math.cos(a)*8;n.vy=Math.sin(a)*8}});paused=false;document.getElementById('pause').textContent='Pausar'}};
document.getElementById('fit').onclick=()=>{{panX=panY=0;zoom=.85;selected=null}};document.getElementById('pause').onclick=e=>{{paused=!paused;e.target.textContent=paused?'Continuar':'Pausar'}};document.getElementById('labels').onclick=()=>{{showLabels=!showLabels}};
document.getElementById('search').addEventListener('input',e=>{{const q=e.target.value.trim().toLowerCase();if(!q){{selected=null;return}}const n=nodes.find(n=>n.label.toLowerCase().includes(q)||n.id.toLowerCase().includes(q));if(n){{selected=n.id;panX=-n.x*zoom;panY=-n.y*zoom;document.getElementById('info').innerHTML=`<strong>${{n.label}}</strong><small>${{n.details||n.group||''}}</small>`}}}});
</script></body></html>
"""
components.html(html, height=800, scrolling=False)

st.caption("O mapa usa física local no navegador; nenhuma posição é gravada nos fluxos.")
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
