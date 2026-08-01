const NODE_TYPES = {
  start:      { label: "Início",       description: "Ponto inicial",         icon: "▶", color: "#059669", soft: "#d1fae5" },
  end:        { label: "Fim",          description: "Encerramento",          icon: "■", color: "#dc2626", soft: "#fee2e2" },
  task:       { label: "Atividade",    description: "Etapa do processo",     icon: "✓", color: "#2563eb", soft: "#dbeafe" },
  decision:   { label: "Decisão",      description: "Regra ou condição",     icon: "◇", color: "#d97706", soft: "#fef3c7" },
  subprocess: { label: "Subprocesso",  description: "Fluxo interno",         icon: "▣", color: "#7c3aed", soft: "#ede9fe" },
  event:      { label: "Evento",       description: "Evento intermediário",  icon: "●", color: "#0891b2", soft: "#cffafe" },
  wait:       { label: "Espera",       description: "Prazo ou dependência",  icon: "◷", color: "#475569", soft: "#e2e8f0" },
  document:   { label: "Documento",    description: "Entrada ou saída",      icon: "▤", color: "#0f766e", soft: "#ccfbf1" },
  api:        { label: "Integração",   description: "Sistema ou API",        icon: "⇄", color: "#9333ea", soft: "#f3e8ff" },
  note:       { label: "Observação",   description: "Nota explicativa",      icon: "✎", color: "#ca8a04", soft: "#fef9c3" },
};

const DEFAULT_WORLD_WIDTH = 4200;
const DEFAULT_WORLD_HEIGHT = 2600;
const WORLD_PADDING_X = 260;
const WORLD_PADDING_Y = 140;
const LANE_HEADER_HEIGHT = 38;
const LANE_CONTENT_TOP = 58;
const LANE_BOTTOM_PADDING = 28;
const LANE_ROW_GAP = 104;
const LEVEL_GAP = 260;
const MIN_ZOOM = 0.04;
const MAX_ZOOM = 2.2;
const NODE_WIDTH = 184;
const NODE_HEIGHT = 72;
const LANE_PLAYBACK_FALLBACK_STEP = 220;

function clone(value) {
  return JSON.parse(JSON.stringify(value));
}

function uid(prefix) {
  if (globalThis.crypto && crypto.randomUUID) {
    return `${prefix}_${crypto.randomUUID().replaceAll("-", "").slice(0, 12)}`;
  }
  return `${prefix}_${Date.now().toString(36)}${Math.random().toString(36).slice(2, 8)}`;
}

function nowIso() {
  return new Date().toISOString();
}

function clamp(value, min, max) {
  return Math.min(max, Math.max(min, value));
}

function normalizeDocument(input) {
  const doc = clone(input || {});
  doc.schemaVersion ||= "1.0.0";
  doc.flow ||= {};
  doc.flow.id ||= uid("flow");
  doc.flow.name ||= "Novo processo";
  doc.flow.description ||= "";
  doc.flow.status ||= "draft";
  doc.flow.orientation ||= "LR";
  doc.flow.createdAt ||= nowIso();
  doc.flow.updatedAt ||= nowIso();
  doc.settings = {
    snapToGrid: true,
    gridSize: 20,
    autoLayout: false,
    showMiniMap: true,
    showGrid: true,
    ...(doc.settings || {}),
  };
  doc.viewport = { x: 0, y: 0, zoom: 1, ...(doc.viewport || {}) };
  doc.lanes = Array.isArray(doc.lanes) ? doc.lanes : [];
  doc.nodes = Array.isArray(doc.nodes) ? doc.nodes : [];
  doc.edges = Array.isArray(doc.edges) ? doc.edges : [];

  doc.lanes.forEach((lane, index) => {
    lane.id ||= uid("lane");
    lane.name ||= `Raia ${index + 1}`;
    lane.order = Number.isFinite(Number(lane.order)) ? Number(lane.order) : index + 1;
    lane.color ||= "#EEF2FF";
    lane.enabled = lane.enabled !== false;
    lane.collapsed = lane.collapsed === true;
    lane.height = clamp(Number(lane.height) || 240, 110, 1200);
  });
  doc.nodes.forEach((node) => {
    node.id ||= uid("node");
    node.type = NODE_TYPES[node.type] ? node.type : "task";
    node.position ||= { x: 120, y: 100 };
    node.position.x = Number(node.position.x) || 0;
    node.position.y = Number(node.position.y) || 0;
    node.data ||= {};
    node.data.label ||= NODE_TYPES[node.type].label;
    node.data.description ||= "";
    node.data.owner ||= "";
    node.data.enabled = node.data.enabled !== false;
    node.data.locked = node.data.locked === true;
    node.data.slaMinutes = node.data.slaMinutes ?? null;
    node.data.tags = Array.isArray(node.data.tags) ? node.data.tags : [];
    node.data.preferredEdgeId = node.data.preferredEdgeId || null;
  });
  doc.edges.forEach((edge) => {
    edge.id ||= uid("edge");
    edge.type ||= "smoothstep";
    edge.label ||= "";
    edge.condition = edge.condition ?? "";
    edge.enabled = edge.enabled !== false;
    edge.targetHandle ||= "input";
  });

  const nodeMap = new Map(doc.nodes.map((node) => [node.id, node]));
  const outgoingByNode = new Map();
  doc.edges.forEach((edge) => {
    if (!outgoingByNode.has(edge.source)) outgoingByNode.set(edge.source, []);
    outgoingByNode.get(edge.source).push(edge);
  });
  outgoingByNode.forEach((outgoing, sourceId) => {
    const sourceNode = nodeMap.get(sourceId);
    outgoing.forEach((edge, index) => {
      if (sourceNode?.type === "decision") {
        const current = String(edge.sourceHandle || "");
        edge.sourceHandle = /^branch-\d+$/.test(current) ? current : `branch-${index}`;
      } else {
        edge.sourceHandle ||= "output";
      }
    });
  });

  return doc;
}

export default function flowEditor(component) {
  const { data, setTriggerValue, parentElement } = component;
  const root = parentElement.querySelector('[data-role="root"]');
  if (!root) return;

  const $ = (selector) => root.querySelector(selector);
  const $$ = (selector) => Array.from(root.querySelectorAll(selector));
  const el = (tag, className, text) => {
    const node = document.createElement(tag);
    if (className) node.className = className;
    if (text !== undefined) node.textContent = text;
    return node;
  };

  const state = {
    doc: normalizeDocument(data?.document),
    selected: null,
    connecting: null,
    pointerWorld: { x: 0, y: 0 },
    history: [],
    future: [],
    dragging: null,
    panning: null,
    spaceDown: false,
    dirty: false,
    destroyed: false,
    paletteCollapsed: localStorage.getItem("produto_tools_palette_collapsed") === "true",
    inspectorCollapsed: localStorage.getItem("produto_tools_inspector_collapsed") === "true",
    uiTheme: String(data?.theme || localStorage.getItem("produto_tools_editor_theme") || "light") === "dark" ? "dark" : "light",
    worldSize: { width: DEFAULT_WORLD_WIDTH, height: DEFAULT_WORLD_HEIGHT },
    branchChoices: {},
    pendingPathAction: null,
    focusPath: null,
    playback: {
      running: false,
      paused: false,
      timer: null,
      nodeSequence: [],
      edgeSequence: [],
      index: -1,
      currentNodeId: null,
      currentEdgeId: null,
      visitedNodeIds: new Set(),
      visitedEdgeIds: new Set(),
    },
  };

  state.doc.nodes.forEach((node) => {
    if (node.type === "decision" && node.data.preferredEdgeId) {
      state.branchChoices[node.id] = node.data.preferredEdgeId;
    }
  });

  const viewport = $('[data-role="viewport"]');
  const world = $('[data-role="world"]');
  const laneLayer = $('[data-role="lane-layer"]');
  const nodeLayer = $('[data-role="node-layer"]');
  const edgeLayer = $('[data-role="edge-layer"]');
  const propertiesBody = $('[data-role="properties-body"]');
  const fileInput = $('[data-role="file-input"]');
  const minimap = $('[data-role="minimap"]');

  root.style.height = `${Number(data?.height) || 820}px`;
  root.dataset.theme = state.uiTheme;
  root.classList.toggle("palette-collapsed", state.paletteCollapsed);
  root.classList.toggle("inspector-collapsed", state.inspectorCollapsed);

  function laneGeometry() {
    const sorted = [...state.doc.lanes].sort((a, b) => Number(a.order) - Number(b.order));
    let top = 0;
    const map = new Map();
    sorted.forEach((lane) => {
      const height = lane.collapsed ? 48 : clamp(Number(lane.height) || 240, 110, 1200);
      map.set(lane.id, { top, height, bottom: top + height });
      top += height;
    });
    return { sorted, map, totalHeight: top };
  }

  function worldWidth() {
    return Math.max(DEFAULT_WORLD_WIDTH, Number(state.worldSize?.width) || DEFAULT_WORLD_WIDTH);
  }

  function worldHeight() {
    return Math.max(DEFAULT_WORLD_HEIGHT, Number(state.worldSize?.height) || DEFAULT_WORLD_HEIGHT);
  }

  function computeWorldSize() {
    const geometry = laneGeometry();
    let maxX = 900;
    let maxY = Math.max(500, geometry.totalHeight);
    state.doc.nodes.forEach((node) => {
      const dimensions = nodeDimensions(node);
      maxX = Math.max(maxX, Number(node.position.x) + dimensions.width);
      maxY = Math.max(maxY, Number(node.position.y) + dimensions.height);
    });
    return {
      width: Math.ceil(Math.max(DEFAULT_WORLD_WIDTH, maxX + WORLD_PADDING_X) / 100) * 100,
      height: Math.ceil(Math.max(DEFAULT_WORLD_HEIGHT, maxY + WORLD_PADDING_Y, geometry.totalHeight + 80) / 100) * 100,
    };
  }

  function updateWorldSize() {
    state.worldSize = computeWorldSize();
    const width = worldWidth();
    const height = worldHeight();
    world.style.width = `${width}px`;
    world.style.height = `${height}px`;
    edgeLayer.style.width = `${width}px`;
    edgeLayer.style.height = `${height}px`;
    edgeLayer.setAttribute("width", String(width));
    edgeLayer.setAttribute("height", String(height));
    edgeLayer.setAttribute("viewBox", `0 0 ${width} ${height}`);
    root.style.setProperty("--flow-world-width", `${width}px`);
    root.style.setProperty("--flow-world-height", `${height}px`);
  }

  function worldPoint(clientX, clientY) {
    const rect = viewport.getBoundingClientRect();
    const { x, y, zoom } = state.doc.viewport;
    return {
      x: (clientX - rect.left - x) / zoom,
      y: (clientY - rect.top - y) / zoom,
    };
  }

  function laneAtY(y) {
    const geometry = laneGeometry();
    for (const lane of geometry.sorted) {
      const box = geometry.map.get(lane.id);
      if (y >= box.top && y <= box.bottom) return lane.id;
    }
    return null;
  }

  function constrainNodeYToLane(node, value) {
    const raw = Math.max(0, Number(value) || 0);
    if (!node?.laneId) return raw;
    const box = laneGeometry().map.get(node.laneId);
    if (!box) return raw;
    const minY = box.top + LANE_CONTENT_TOP;
    const maxY = Math.max(minY, box.bottom - NODE_HEIGHT - LANE_BOTTOM_PADDING);
    return clamp(raw, minY, maxY);
  }

  function normalizeNodesIntoLanes() {
    const geometry = laneGeometry();
    const lanesById = new Set(geometry.sorted.map((lane) => lane.id));
    const unassigned = state.doc.nodes.filter((node) => !node.laneId || !lanesById.has(node.laneId));
    unassigned.forEach((node) => {
      const detected = laneAtY(Number(node.position.y) + NODE_HEIGHT / 2);
      if (detected) node.laneId = detected;
    });
    state.doc.nodes.forEach((node) => {
      if (!node.laneId || !geometry.map.has(node.laneId)) return;
      node.position.y = constrainNodeYToLane(node, node.position.y);
    });
  }

  function snap(value) {
    if (!state.doc.settings.snapToGrid) return Math.round(value);
    const size = Number(state.doc.settings.gridSize) || 20;
    return Math.round(value / size) * size;
  }

  function getNode(id) {
    return state.doc.nodes.find((node) => node.id === id);
  }

  function getEdge(id) {
    return state.doc.edges.find((edge) => edge.id === id);
  }

  function getLane(id) {
    return state.doc.lanes.find((lane) => lane.id === id);
  }

  function lanePlaybackState(laneId) {
    const currentNode = state.playback.currentNodeId ? getNode(state.playback.currentNodeId) : null;
    if (currentNode?.laneId === laneId) return "current";
    for (const nodeId of state.playback.visitedNodeIds) {
      if (getNode(nodeId)?.laneId === laneId) return "visited";
    }
    return null;
  }

  function markDirty() {
    state.dirty = true;
    state.doc.flow.updatedAt = nowIso();
    $('[data-role="save-state"]').textContent = "Alterações não salvas";
    $('[data-role="save-state"]').style.color = "#d97706";
  }

  function checkpoint() {
    state.history.push(clone(state.doc));
    if (state.history.length > 80) state.history.shift();
    state.future = [];
  }

  function mutate(callback, message = null) {
    checkpoint();
    clearPlaybackTimer();
    state.playback.running = false;
    state.playback.paused = false;
    resetPlaybackVisuals();
    callback();
    if (state.focusPath) {
      const validNodes = new Set(state.doc.nodes.map((node) => node.id));
      const validEdges = new Set(state.doc.edges.map((edge) => edge.id));
      state.focusPath.nodeSequence = state.focusPath.nodeSequence.filter((id) => validNodes.has(id));
      state.focusPath.edgeSequence = state.focusPath.edgeSequence.filter((id) => validEdges.has(id));
      state.focusPath.nodeIds = new Set(state.focusPath.nodeSequence);
      state.focusPath.edgeIds = new Set(state.focusPath.edgeSequence);
      if (!state.focusPath.nodeSequence.length) state.focusPath = null;
    }
    markDirty();
    renderAll();
    if (message) toast(message, "success");
  }

  function undo() {
    if (!state.history.length) return toast("Nada para desfazer", "info");
    state.future.push(clone(state.doc));
    state.doc = state.history.pop();
    state.selected = null;
    state.focusPath = null;
    clearPlaybackTimer();
    state.playback.running = false;
    resetPlaybackVisuals();
    markDirty();
    renderAll();
  }

  function redo() {
    if (!state.future.length) return toast("Nada para refazer", "info");
    state.history.push(clone(state.doc));
    state.doc = state.future.pop();
    state.selected = null;
    state.focusPath = null;
    clearPlaybackTimer();
    state.playback.running = false;
    resetPlaybackVisuals();
    markDirty();
    renderAll();
  }

  function toast(message, type = "info") {
    const container = $('[data-role="toasts"]');
    const item = el("div", `toast ${type}`, message);
    container.appendChild(item);
    setTimeout(() => item.remove(), 2800);
  }

  function palette() {
    const container = $('[data-role="palette-items"]');
    container.innerHTML = "";
    Object.entries(NODE_TYPES).forEach(([type, meta]) => {
      const item = el("div", "palette-item");
      item.draggable = true;
      item.dataset.type = type;
      item.dataset.search = `${meta.label} ${meta.description}`.toLowerCase();
      const icon = el("div", "palette-icon", meta.icon);
      icon.style.color = meta.color;
      icon.style.background = meta.soft;
      const text = el("div");
      text.append(el("strong", "", meta.label), el("small", "", meta.description));
      item.append(icon, text);
      item.addEventListener("dragstart", (event) => {
        event.dataTransfer.setData("application/x-produto-tools-node", type);
        event.dataTransfer.effectAllowed = "copy";
      });
      container.appendChild(item);
    });
  }

  function renderViewport() {
    const v = state.doc.viewport;
    v.zoom = clamp(Number(v.zoom) || 1, MIN_ZOOM, MAX_ZOOM);
    world.style.transform = `translate(${v.x}px, ${v.y}px) scale(${v.zoom})`;
    world.classList.toggle("show-grid", state.doc.settings.showGrid !== false);
    $('[data-role="zoom-label"]').textContent = `${Math.round(v.zoom * 100)}%`;
    $$('[data-setting]').forEach((input) => {
      input.checked = state.doc.settings[input.dataset.setting] !== false;
    });
    minimap.style.display = state.doc.settings.showMiniMap === false ? "none" : "block";
  }

  function renderLanes() {
    laneLayer.innerHTML = "";
    const geometry = laneGeometry();
    geometry.sorted.forEach((lane) => {
      const box = geometry.map.get(lane.id);
      const laneEl = el("section", "flow-lane");
      laneEl.dataset.id = lane.id;
      laneEl.style.top = `${box.top}px`;
      laneEl.style.height = `${box.height}px`;
      laneEl.style.width = `${Math.max(600, worldWidth() - 68)}px`;
      laneEl.style.background = lane.color || "#EEF2FF";
      laneEl.classList.toggle("selected", state.selected?.kind === "lane" && state.selected.id === lane.id);
      laneEl.classList.toggle("disabled", lane.enabled === false);
      const lanePlayback = lanePlaybackState(lane.id);
      laneEl.classList.toggle("play-current", lanePlayback === "current");
      laneEl.classList.toggle("play-visited", lanePlayback === "visited");
      if (state.focusPath) {
        const laneHasFocus = state.doc.nodes.some((node) => node.laneId === lane.id && focusContainsNode(node.id));
        laneEl.classList.toggle("focus-member", laneHasFocus);
        laneEl.classList.toggle("focus-dimmed", !laneHasFocus);
      } else {
        laneEl.classList.remove("focus-member");
        laneEl.classList.remove("focus-dimmed");
      }

      const header = el("header", "lane-header");
      const color = el("span", "lane-color");
      color.style.background = lane.color || "#EEF2FF";
      header.append(color, el("strong", "", lane.name), el("small", "", `${state.doc.nodes.filter((n) => n.laneId === lane.id).length} itens`));
      const handle = el("span", "lane-handle", "↕");
      handle.title = "Arraste para redimensionar a raia";
      header.appendChild(handle);
      header.addEventListener("click", (event) => {
        if (event.target === handle) return;
        selectItem("lane", lane.id);
      });
      handle.addEventListener("pointerdown", (event) => beginLaneResize(event, lane.id, box.height));
      laneEl.appendChild(header);
      laneLayer.appendChild(laneEl);
    });
  }

  function nodeSubtitle(node) {
    const parts = [];
    if (node.data.owner) parts.push(node.data.owner);
    if (node.data.slaMinutes) parts.push(`SLA ${node.data.slaMinutes} min`);
    return parts.join(" • ") || NODE_TYPES[node.type].description;
  }

  function nodeDimensions(node) {
    if (node.type === "start" || node.type === "end") return { width: 160, height: NODE_HEIGHT };
    if (node.type === "decision") return { width: 174, height: NODE_HEIGHT };
    if (node.type === "event" || node.type === "wait") return { width: 170, height: NODE_HEIGHT };
    return { width: NODE_WIDTH, height: NODE_HEIGHT };
  }

  function enabledGraph() {
    const nodes = state.doc.nodes.filter((node) => node.data.enabled !== false);
    const nodeIds = new Set(nodes.map((node) => node.id));
    const edges = state.doc.edges.filter((edge) => edge.enabled !== false && nodeIds.has(edge.source) && nodeIds.has(edge.target));
    return { nodes, nodeIds, edges };
  }

  function graphMaps(graph) {
    const outgoing = new Map(graph.nodes.map((node) => [node.id, []]));
    const incoming = new Map(graph.nodes.map((node) => [node.id, []]));
    graph.edges.forEach((edge) => {
      if (outgoing.has(edge.source)) outgoing.get(edge.source).push(edge);
      if (incoming.has(edge.target)) incoming.get(edge.target).push(edge);
    });
    return { outgoing, incoming };
  }

  function orderedOutgoingEdges(nodeId, graph = null) {
    const edges = (graph?.edges || state.doc.edges).filter((edge) => edge.source === nodeId && edge.enabled !== false);
    return edges.sort((left, right) => {
      const lt = getNode(left.target);
      const rt = getNode(right.target);
      return (Number(lt?.position?.x) || 0) - (Number(rt?.position?.x) || 0)
        || (Number(lt?.position?.y) || 0) - (Number(rt?.position?.y) || 0)
        || String(left.sourceHandle || "").localeCompare(String(right.sourceHandle || ""));
    });
  }

  function orderedIncomingEdges(nodeId, graph = null) {
    const edges = (graph?.edges || state.doc.edges).filter((edge) => edge.target === nodeId && edge.enabled !== false);
    return edges.sort((left, right) => {
      const ls = getNode(left.source);
      const rs = getNode(right.source);
      return (Number(ls?.position?.x) || 0) - (Number(rs?.position?.x) || 0)
        || (Number(ls?.position?.y) || 0) - (Number(rs?.position?.y) || 0)
        || String(left.sourceHandle || "").localeCompare(String(right.sourceHandle || ""));
    });
  }

  function shortestPath(startIds, targetIds, edges) {
    const targets = targetIds instanceof Set ? targetIds : new Set(targetIds || []);
    const outgoing = new Map();
    edges.forEach((edge) => {
      if (!outgoing.has(edge.source)) outgoing.set(edge.source, []);
      outgoing.get(edge.source).push(edge);
    });
    const queue = [];
    const visited = new Set();
    (startIds || []).forEach((id) => {
      if (!id || visited.has(id)) return;
      visited.add(id);
      queue.push({ nodeId: id, nodeSequence: [id], edgeSequence: [] });
    });
    while (queue.length) {
      const current = queue.shift();
      if (targets.has(current.nodeId)) return current;
      for (const edge of outgoing.get(current.nodeId) || []) {
        if (visited.has(edge.target)) continue;
        visited.add(edge.target);
        queue.push({
          nodeId: edge.target,
          nodeSequence: [...current.nodeSequence, edge.target],
          edgeSequence: [...current.edgeSequence, edge.id],
        });
      }
    }
    return null;
  }

  function combinePaths(...paths) {
    const nodeSequence = [];
    const edgeSequence = [];
    paths.filter(Boolean).forEach((path) => {
      (path.nodeSequence || []).forEach((id) => {
        if (!nodeSequence.length || nodeSequence[nodeSequence.length - 1] !== id) nodeSequence.push(id);
      });
      (path.edgeSequence || []).forEach((id) => {
        if (!edgeSequence.length || edgeSequence[edgeSequence.length - 1] !== id) edgeSequence.push(id);
      });
    });
    return {
      nodeSequence,
      edgeSequence,
      nodeIds: new Set(nodeSequence),
      edgeIds: new Set(edgeSequence),
    };
  }

  function branchText(edge) {
    const target = getNode(edge.target);
    return `${edge.label || ""} ${edge.condition || ""} ${target?.data?.label || ""} ${(target?.data?.tags || []).join(" ")}`.toLowerCase();
  }

  function continuationStats(startId, graph, maps, blocked = new Set()) {
    const queue = [{ nodeId: startId, depth: 0 }];
    const visited = new Set(blocked);
    visited.add(startId);
    let maxDepth = 0;
    let normalTerminals = 0;
    let exceptionTerminals = 0;
    while (queue.length) {
      const current = queue.shift();
      maxDepth = Math.max(maxDepth, current.depth);
      const outgoing = (maps.outgoing.get(current.nodeId) || []).filter((edge) => !blocked.has(edge.target));
      const node = getNode(current.nodeId);
      if (!outgoing.length || node?.type === "end") {
        const text = `${node?.data?.label || ""} ${(node?.data?.tags || []).join(" ")}`.toLowerCase();
        if (/(erro|falha|recus|cancel|indispon|bloque|expir|encerrad|não cobrar|nao cobrar)/.test(text)) exceptionTerminals += 1;
        else normalTerminals += 1;
      }
      for (const edge of outgoing) {
        if (visited.has(edge.target)) continue;
        visited.add(edge.target);
        queue.push({ nodeId: edge.target, depth: current.depth + 1 });
      }
    }
    return { count: visited.size, maxDepth, normalTerminals, exceptionTerminals };
  }

  function branchScore(edge, graph, maps, blocked = new Set()) {
    const stats = continuationStats(edge.target, graph, maps, blocked);
    const text = branchText(edge);
    let score = stats.maxDepth * 18 + stats.count * 1.4 + stats.normalTerminals * 24 - stats.exceptionTerminals * 12;
    if (/(sim|aceit|aprov|conclu|sucesso|disponível|disponivel|suficiente|ativo|continuar|automático|automatico|cobrar|gerar|criar|cliente|ambos|válid|valid)/.test(text)) score += 55;
    if (/(erro|falha|recus|cancel|indispon|bloque|expir|corrigir|aguardar|pendente|não cobrar|nao cobrar)/.test(text)) score -= 70;
    if (/^\s*n[aã]o\b/.test(text)) score -= 12;
    const target = getNode(edge.target);
    if (target?.type === "end" && stats.maxDepth === 0) score -= 20;
    return score;
  }

  function chooseForwardEdge(nodeId, candidates, graph, maps, forcedEdgeId = null, blocked = new Set()) {
    if (!candidates.length) return null;
    if (forcedEdgeId) {
      const forced = candidates.find((edge) => edge.id === forcedEdgeId);
      if (forced) return forced;
    }
    const chosenId = state.branchChoices[nodeId];
    if (chosenId) {
      const chosen = candidates.find((edge) => edge.id === chosenId);
      if (chosen) return chosen;
    }
    return [...candidates].sort((left, right) => branchScore(right, graph, maps, blocked) - branchScore(left, graph, maps, blocked))[0];
  }

  function buildForwardRoute(startId, graph, forcedFirstEdgeId = null) {
    const maps = graphMaps(graph);
    const nodeSequence = [startId];
    const edgeSequence = [];
    const visited = new Set([startId]);
    let currentId = startId;
    let first = true;
    const limit = Math.max(20, graph.nodes.length * 2);

    while (nodeSequence.length <= limit) {
      const currentNode = getNode(currentId);
      if (currentNode?.type === "end") break;
      const candidates = (maps.outgoing.get(currentId) || []).filter((edge) => !visited.has(edge.target));
      if (!candidates.length) break;
      const edge = chooseForwardEdge(currentId, candidates, graph, maps, first ? forcedFirstEdgeId : null, visited);
      first = false;
      if (!edge) break;
      edgeSequence.push(edge.id);
      currentId = edge.target;
      nodeSequence.push(currentId);
      visited.add(currentId);
    }

    return { nodeSequence, edgeSequence };
  }

  function pathToAnchor(starts, anchorId, graph) {
    if (starts.includes(anchorId)) return { nodeSequence: [anchorId], edgeSequence: [] };
    const candidates = starts
      .map((startId) => shortestPath([startId], new Set([anchorId]), graph.edges))
      .filter(Boolean)
      .sort((left, right) => right.nodeSequence.length - left.nodeSequence.length);
    return candidates[0] || { nodeSequence: [anchorId], edgeSequence: [] };
  }

  function buildLanePath(laneId) {
    const graph = enabledGraph();
    const laneNodes = graph.nodes.filter((node) => node.laneId === laneId);
    if (!laneNodes.length) return null;
    const laneIds = new Set(laneNodes.map((node) => node.id));
    const laneEdges = graph.edges.filter((edge) => laneIds.has(edge.source) && laneIds.has(edge.target));
    const laneGraph = { nodes: laneNodes, nodeIds: laneIds, edges: laneEdges };
    if (!laneEdges.length) {
      const ordered = [...laneNodes].sort((left, right) => (Number(left.position.x) || 0) - (Number(right.position.x) || 0) || (Number(left.position.y) || 0) - (Number(right.position.y) || 0));
      return {
        nodeSequence: ordered.map((node) => node.id),
        edgeSequence: [],
        nodeIds: new Set(ordered.map((node) => node.id)),
        edgeIds: new Set(),
      };
    }
    const incomingInside = new Set(laneEdges.map((edge) => edge.target));
    const starts = laneNodes
      .filter((node) => !incomingInside.has(node.id))
      .sort((left, right) => (Number(left.position.x) || 0) - (Number(right.position.x) || 0) || (Number(left.position.y) || 0) - (Number(right.position.y) || 0));
    const candidates = (starts.length ? starts : laneNodes).map((node) => buildForwardRoute(node.id, laneGraph));
    const best = candidates.sort((left, right) => (right.nodeSequence?.length || 0) - (left.nodeSequence?.length || 0))[0];
    if (!best) return null;
    return {
      nodeSequence: best.nodeSequence,
      edgeSequence: best.edgeSequence,
      nodeIds: new Set(best.nodeSequence),
      edgeIds: new Set(best.edgeSequence),
    };
  }

  function buildReadablePath() {
    const graph = enabledGraph();
    if (!graph.nodes.length) return null;
    const startIds = graph.nodes.filter((node) => node.type === "start").map((node) => node.id);
    const rootIds = graph.nodes.filter((node) => !graph.edges.some((edge) => edge.target === node.id)).map((node) => node.id);
    const effectiveStarts = startIds.length ? startIds : (rootIds.length ? rootIds : [graph.nodes[0].id]);

    if (state.selected?.kind === "edge") {
      const selectedEdge = getEdge(state.selected.id);
      if (selectedEdge && selectedEdge.enabled !== false && graph.nodeIds.has(selectedEdge.source) && graph.nodeIds.has(selectedEdge.target)) {
        const sourceNode = getNode(selectedEdge.source);
        if (sourceNode?.type === "decision") state.branchChoices[sourceNode.id] = selectedEdge.id;
        const middle = { nodeSequence: [selectedEdge.source, selectedEdge.target], edgeSequence: [selectedEdge.id] };
        const suffix = buildForwardRoute(selectedEdge.target, graph);
        return combinePaths(middle, suffix);
      }
    }

    if (state.selected?.kind === "lane") {
      return buildLanePath(state.selected.id);
    }

    if (state.selected?.kind === "node" && graph.nodeIds.has(state.selected.id)) {
      const node = getNode(state.selected.id);
      const suffix = buildForwardRoute(state.selected.id, graph);
      return {
        nodeSequence: suffix.nodeSequence,
        edgeSequence: suffix.edgeSequence,
        nodeIds: new Set(suffix.nodeSequence),
        edgeIds: new Set(suffix.edgeSequence),
        title: node?.data?.label || "Rota selecionada",
      };
    }

    const anchorId = effectiveStarts[0];
    const suffix = buildForwardRoute(anchorId, graph);
    return {
      nodeSequence: suffix.nodeSequence,
      edgeSequence: suffix.edgeSequence,
      nodeIds: new Set(suffix.nodeSequence),
      edgeIds: new Set(suffix.edgeSequence),
    };
  }

  function focusContainsNode(nodeId) {
    return Boolean(state.focusPath?.nodeIds?.has(nodeId));
  }

  function focusContainsEdge(edgeId) {
    return Boolean(state.focusPath?.edgeIds?.has(edgeId));
  }

  function renderNodes() {
    nodeLayer.innerHTML = "";
    state.doc.nodes.forEach((node) => {
      const meta = NODE_TYPES[node.type] || NODE_TYPES.task;
      const nodeEl = el("article", `flow-node type-${node.type}`);
      nodeEl.dataset.id = node.id;
      nodeEl.style.left = `${node.position.x}px`;
      nodeEl.style.top = `${node.position.y}px`;
      nodeEl.style.setProperty("--node-color", meta.color);
      nodeEl.style.setProperty("--node-soft", meta.soft);
      nodeEl.classList.toggle("selected", state.selected?.kind === "node" && state.selected.id === node.id);
      nodeEl.classList.toggle("disabled", node.data.enabled === false);
      nodeEl.classList.toggle("locked", node.data.locked === true);
      nodeEl.classList.toggle("focus-member", Boolean(state.focusPath) && focusContainsNode(node.id));
      nodeEl.classList.toggle("focus-dimmed", Boolean(state.focusPath) && !focusContainsNode(node.id));
      nodeEl.classList.toggle("play-visited", state.playback.visitedNodeIds.has(node.id));
      nodeEl.classList.toggle("play-current", state.playback.currentNodeId === node.id);

      const accent = el("div", "node-accent");
      const content = el("div", "node-content");
      const icon = el("div", "node-icon", meta.icon);
      const text = el("div", "node-text");
      text.append(el("strong", "", node.data.label), el("small", "", nodeSubtitle(node)));
      if (node.data.tags.length) {
        const badges = el("div", "node-badges");
        node.data.tags.slice(0, 3).forEach((tag) => badges.appendChild(el("span", "node-badge", tag)));
        text.appendChild(badges);
      }
      content.append(icon, text);

      const inputPort = el("span", "node-port input");
      inputPort.title = "Solte ou clique para receber uma conexão";
      inputPort.addEventListener("pointerup", (event) => finishConnection(event, node.id));
      inputPort.addEventListener("click", (event) => finishConnection(event, node.id));

      const outputPorts = [];
      if (node.type === "decision") {
        const outgoing = state.doc.edges.filter((edge) => edge.source === node.id);
        const highestHandle = outgoing.reduce((highest, edge) => {
          const match = String(edge.sourceHandle || "").match(/^branch-(\d+)$/);
          return match ? Math.max(highest, Number(match[1])) : highest;
        }, -1);
        const portCount = Math.max(2, highestHandle + 1);
        for (let index = 0; index < portCount; index += 1) {
          const handle = `branch-${index}`;
          const branchEdge = outgoing.find((edge) => edge.sourceHandle === handle);
          const outputPort = el("span", "node-port output decision-output");
          outputPort.dataset.handle = handle;
          outputPort.style.top = `${((index + 1) / (portCount + 1)) * 100}%`;
          outputPort.title = branchEdge
            ? `Saída ${index + 1}: ${branchEdge.label || branchEdge.condition || getNode(branchEdge.target)?.data.label || "ramificação"}`
            : `Saída ${index + 1}: arraste para conectar`;
          outputPort.classList.toggle("used", Boolean(branchEdge));
          outputPort.classList.toggle("connecting", state.connecting?.source === node.id && state.connecting?.sourceHandle === handle);
          outputPort.addEventListener("pointerdown", (event) => beginConnection(event, node.id, handle));
          outputPort.addEventListener("click", (event) => beginConnection(event, node.id, handle));
          outputPorts.push(outputPort);
        }
      } else {
        const outputPort = el("span", "node-port output");
        outputPort.dataset.handle = "output";
        outputPort.title = "Arraste para conectar";
        outputPort.classList.toggle("connecting", state.connecting?.source === node.id);
        outputPort.addEventListener("pointerdown", (event) => beginConnection(event, node.id, "output"));
        outputPort.addEventListener("click", (event) => beginConnection(event, node.id, "output"));
        outputPorts.push(outputPort);
      }

      nodeEl.append(accent, content, inputPort, ...outputPorts);
      nodeEl.addEventListener("pointerdown", (event) => beginNodeDrag(event, node.id));
      nodeEl.addEventListener("click", (event) => {
        event.stopPropagation();
        selectItem("node", node.id);
      });
      nodeEl.addEventListener("dblclick", (event) => {
        event.stopPropagation();
        selectItem("node", node.id);
        const labelInput = propertiesBody.querySelector('[data-field="label"]');
        labelInput?.focus();
        labelInput?.select();
      });
      nodeLayer.appendChild(nodeEl);
    });
  }

  function decisionPortCount(nodeId) {
    const outgoing = state.doc.edges.filter((edge) => edge.source === nodeId);
    const highestHandle = outgoing.reduce((highest, edge) => {
      const match = String(edge.sourceHandle || "").match(/^branch-(\d+)$/);
      return match ? Math.max(highest, Number(match[1])) : highest;
    }, -1);
    return Math.max(2, highestHandle + 1);
  }

  function nextDecisionHandle(nodeId) {
    const used = new Set(
      state.doc.edges
        .filter((edge) => edge.source === nodeId)
        .map((edge) => String(edge.sourceHandle || "")),
    );
    let index = 0;
    while (used.has(`branch-${index}`)) index += 1;
    return `branch-${index}`;
  }

  function nodeCenter(node, side, handle = null) {
    const dimensions = nodeDimensions(node);
    let y = node.position.y + dimensions.height / 2;
    if (side === "source" && node.type === "decision") {
      const count = decisionPortCount(node.id);
      const match = String(handle || "branch-0").match(/^branch-(\d+)$/);
      const index = clamp(match ? Number(match[1]) : 0, 0, count - 1);
      y = node.position.y + ((index + 1) / (count + 1)) * dimensions.height;
    }
    return {
      x: node.position.x + (side === "source" ? dimensions.width : 0),
      y,
    };
  }

  function edgePath(source, target, type = "smoothstep", edge = null) {
    if (type === "straight") return `M ${source.x} ${source.y} L ${target.x} ${target.y}`;
    const direction = target.x >= source.x ? 1 : -1;
    const outgoing = edge ? orderedOutgoingEdges(edge.source) : [];
    const incoming = edge ? orderedIncomingEdges(edge.target) : [];
    const outIndex = Math.max(0, outgoing.findIndex((item) => item.id === edge?.id));
    const inIndex = Math.max(0, incoming.findIndex((item) => item.id === edge?.id));
    const outTrack = outgoing.length ? outIndex - (outgoing.length - 1) / 2 : 0;
    const inTrack = incoming.length ? inIndex - (incoming.length - 1) / 2 : 0;
    const exitX = source.x + direction * (36 + Math.abs(outTrack) * 16);
    const entryX = target.x - direction * (36 + Math.abs(inTrack) * 16);
    const verticalOffset = (outTrack - inTrack) * 12;
    let bridgeX = (exitX + entryX) / 2 + direction * outTrack * 14;
    let bridgeY = ((source.y + target.y) / 2) + verticalOffset;

    if (direction === 1) bridgeX = Math.max(exitX + 22, Math.min(entryX - 22, bridgeX));
    else bridgeX = Math.min(exitX - 22, Math.max(entryX + 22, bridgeX));

    if (Math.abs(target.x - source.x) < 140 || direction === -1) {
      bridgeY = ((source.y + target.y) / 2) + verticalOffset + (outTrack * 10);
      const elbowX = source.x + direction * (52 + Math.abs(outTrack) * 18);
      const targetElbowX = target.x - direction * (52 + Math.abs(inTrack) * 18);
      return `M ${source.x} ${source.y} L ${elbowX} ${source.y} L ${elbowX} ${bridgeY} L ${targetElbowX} ${bridgeY} L ${targetElbowX} ${target.y} L ${target.x} ${target.y}`;
    }

    return `M ${source.x} ${source.y} L ${exitX} ${source.y} L ${bridgeX} ${source.y} L ${bridgeX} ${target.y} L ${entryX} ${target.y} L ${target.x} ${target.y}`;
  }

  function svgNode(tag, attrs = {}) {
    const node = document.createElementNS("http://www.w3.org/2000/svg", tag);
    Object.entries(attrs).forEach(([key, value]) => node.setAttribute(key, String(value)));
    return node;
  }

  function renderEdges() {
    Array.from(edgeLayer.querySelectorAll(".edge-group, .temp-edge")).forEach((item) => item.remove());
    state.doc.edges.forEach((edge) => {
      const sourceNode = getNode(edge.source);
      const targetNode = getNode(edge.target);
      if (!sourceNode || !targetNode) return;
      const source = nodeCenter(sourceNode, "source", edge.sourceHandle);
      const target = nodeCenter(targetNode, "target");
      const path = edgePath(source, target, edge.type, edge);
      const edgeClasses = ["edge-group"];
      if (edge.enabled === false) edgeClasses.push("disabled");
      if (state.selected?.kind === "edge" && state.selected.id === edge.id) edgeClasses.push("selected");
      if (state.focusPath) edgeClasses.push(focusContainsEdge(edge.id) ? "focus-member" : "focus-dimmed");
      if (state.playback.visitedEdgeIds.has(edge.id)) edgeClasses.push("play-visited");
      if (state.playback.currentEdgeId === edge.id) edgeClasses.push("play-current");
      const group = svgNode("g", { class: edgeClasses.join(" ") });
      group.dataset.id = edge.id;
      const sourceMeta = NODE_TYPES[sourceNode.type] || NODE_TYPES.task;
      group.style.setProperty("--edge-color", sourceMeta.color);
      const hit = svgNode("path", { d: path, class: "edge-hit" });
      const visible = svgNode("path", { d: path, class: "edge-path" });
      group.append(hit, visible);
      if (edge.label) {
        const mx = (source.x + target.x) / 2;
        const my = (source.y + target.y) / 2;
        const width = clamp(edge.label.length * 6 + 18, 46, 170);
        const bg = svgNode("rect", { x: mx - width / 2, y: my - 11, width, height: 22, class: "edge-label-bg" });
        const label = svgNode("text", { x: mx, y: my + 1, class: "edge-label" });
        label.textContent = edge.label;
        group.append(bg, label);
      }
      group.addEventListener("click", (event) => {
        event.stopPropagation();
        selectItem("edge", edge.id);
      });
      edgeLayer.appendChild(group);
    });
    renderTemporaryEdge();
  }

  function renderTemporaryEdge() {
    edgeLayer.querySelector(".temp-edge")?.remove();
    if (!state.connecting) return;
    const node = getNode(state.connecting.source);
    if (!node) return;
    const source = nodeCenter(node, "source", state.connecting.sourceHandle);
    const path = edgePath(source, state.pointerWorld, "smoothstep", null);
    const temp = svgNode("path", { d: path, class: "edge-path temp-edge", "stroke-dasharray": "7 5", opacity: ".8" });
    edgeLayer.appendChild(temp);
  }

  function renderMinimap() {
    minimap.innerHTML = "";
    if (state.doc.settings.showMiniMap === false) return;
    const mapWidth = 156;
    const mapHeight = 92;
    const scaleX = mapWidth / worldWidth();
    const scaleY = mapHeight / worldHeight();
    const geometry = laneGeometry();
    geometry.sorted.forEach((lane) => {
      const box = geometry.map.get(lane.id);
      const item = el("div", "minimap-lane");
      item.style.left = `${34 * scaleX}px`;
      item.style.top = `${box.top * scaleY}px`;
      item.style.width = `${Math.max(2, (worldWidth() - 68) * scaleX)}px`;
      item.style.height = `${Math.max(1, box.height * scaleY)}px`;
      item.style.background = lane.color;
      minimap.appendChild(item);
    });
    state.doc.nodes.forEach((node) => {
      const item = el("div", "minimap-node");
      item.style.left = `${node.position.x * scaleX}px`;
      item.style.top = `${node.position.y * scaleY}px`;
      item.style.background = (NODE_TYPES[node.type] || NODE_TYPES.task).color;
      if (state.focusPath) item.classList.add(focusContainsNode(node.id) ? "focus-member" : "focus-dimmed");
      minimap.appendChild(item);
    });
    const rect = viewport.getBoundingClientRect();
    const v = state.doc.viewport;
    const view = el("div", "minimap-viewport");
    view.style.left = `${clamp((-v.x / v.zoom) * scaleX, 0, mapWidth)}px`;
    view.style.top = `${clamp((-v.y / v.zoom) * scaleY, 0, mapHeight)}px`;
    view.style.width = `${clamp((rect.width / v.zoom) * scaleX, 2, mapWidth)}px`;
    view.style.height = `${clamp((rect.height / v.zoom) * scaleY, 2, mapHeight)}px`;
    minimap.appendChild(view);
  }

  function applyPanelState() {
    root.classList.toggle("palette-collapsed", state.paletteCollapsed);
    root.classList.toggle("inspector-collapsed", state.inspectorCollapsed);
    const paletteToggle = $('[data-role="palette-toggle"]');
    if (paletteToggle) {
      paletteToggle.textContent = state.paletteCollapsed ? "›" : "‹";
      paletteToggle.title = state.paletteCollapsed ? "Maximizar elementos" : "Minimizar elementos";
    }
    const inspectorToggle = $('[data-role="inspector-toggle"]');
    if (inspectorToggle) {
      inspectorToggle.textContent = state.inspectorCollapsed ? "⌄" : "⌃";
      inspectorToggle.title = state.inspectorCollapsed ? "Expandir propriedades" : "Recolher propriedades";
    }
  }

  function applyTheme(theme, persist = true) {
    state.uiTheme = theme === "dark" ? "dark" : "light";
    root.dataset.theme = state.uiTheme;
    const button = $('[data-role="theme-button"]');
    if (button) {
      button.textContent = state.uiTheme === "dark" ? "☀ Claro" : "☾ Escuro";
      button.title = state.uiTheme === "dark" ? "Usar modo claro" : "Usar modo escuro";
    }
    if (persist) localStorage.setItem("produto_tools_editor_theme", state.uiTheme);
  }

  function updatePlaybackUi() {
    const playButton = $('[data-role="play-button"]');
    const stopButton = $('[data-role="stop-button"]');
    const overlay = $('[data-role="playback-overlay"]');
    if (playButton) {
      playButton.classList.toggle("is-paused", state.playback.running && !state.playback.paused);
      if (!state.playback.running) playButton.textContent = "▶ Play";
      else if (state.playback.paused) playButton.textContent = "▶ Continuar";
      else playButton.textContent = "Ⅱ Pausar";
    }
    if (stopButton) stopButton.disabled = !state.playback.running && !state.playback.visitedNodeIds.size;
    if (overlay) {
      overlay.hidden = !state.playback.running;
      const progress = $('[data-role="playback-progress"]');
      const title = $('[data-role="playback-title"]');
      if (title) {
        const baseTitle = state.selected?.kind === "lane" ? "Reproduzindo raia" : (state.selected?.kind === "node" ? "Reproduzindo a partir do card" : "Reproduzindo fluxo");
        title.textContent = state.playback.paused ? `${baseTitle} — pausado` : baseTitle;
      }
      if (progress) progress.textContent = state.playback.running
        ? `Etapa ${Math.max(1, state.playback.index + 1)} de ${state.playback.nodeSequence.length}`
        : "";
    }
  }

  function renderHeaderAndStatus() {
    $('[data-role="flow-name"]').textContent = state.doc.flow.name || "Editor de Processos";
    let selectedText = "Nenhum item selecionado";
    if (state.selected?.kind === "node") selectedText = `Elemento: ${getNode(state.selected.id)?.data.label || ""}`;
    if (state.selected?.kind === "edge") selectedText = `Conexão: ${getEdge(state.selected.id)?.label || "sem nome"}`;
    if (state.selected?.kind === "lane") selectedText = `Raia: ${getLane(state.selected.id)?.name || ""}`;
    $('[data-role="selection-label"]').textContent = selectedText;
    $('[data-role="connection-hint"]').textContent = state.connecting ? "Selecione a entrada do elemento de destino" : "";
    $('[data-role="empty-state"]').style.display = state.doc.nodes.length || state.doc.lanes.length ? "none" : "grid";
    const focusStatus = $('[data-role="focus-status"]');
    if (focusStatus) {
      if (!state.focusPath) focusStatus.textContent = "";
      else if (state.selected?.kind === "lane") focusStatus.textContent = `${state.focusPath.nodeSequence.length} etapas destacadas na raia`;
      else if (state.selected?.kind === "node") focusStatus.textContent = `${state.focusPath.nodeSequence.length} etapas a partir do card selecionado`;
      else focusStatus.textContent = `${state.focusPath.nodeSequence.length} etapas destacadas`;
    }
    const focusButton = $('[data-role="focus-button"]');
    if (focusButton) {
      focusButton.classList.toggle("is-active", Boolean(state.focusPath));
      focusButton.textContent = state.focusPath ? "× Limpar destaque" : "◎ Destacar caminho";
    }
    applyPanelState();
    applyTheme(state.uiTheme, false);
    updatePlaybackUi();
  }

  function fieldGroup(label, control) {
    const group = el("div", "property-group");
    group.append(el("label", "", label), control);
    return group;
  }

  function textInput(field, value, placeholder = "") {
    const input = el("input");
    input.type = "text";
    input.value = value ?? "";
    input.placeholder = placeholder;
    input.dataset.field = field;
    return input;
  }

  function numberInput(field, value, min = 0) {
    const input = el("input");
    input.type = "number";
    input.value = value ?? "";
    input.min = String(min);
    input.dataset.field = field;
    return input;
  }

  function textarea(field, value) {
    const input = el("textarea");
    input.value = value ?? "";
    input.dataset.field = field;
    return input;
  }

  function selectInput(field, value, options) {
    const select = el("select");
    select.dataset.field = field;
    options.forEach(([optionValue, label]) => {
      const option = el("option", "", label);
      option.value = optionValue;
      option.selected = optionValue === value;
      select.appendChild(option);
    });
    return select;
  }

  function switchRow(title, subtitle, enabled, onToggle) {
    const row = el("div", "switch-row");
    const text = el("span");
    text.append(el("strong", "", title), el("small", "", subtitle));
    const toggle = el("button", `switch${enabled ? " on" : ""}`);
    toggle.type = "button";
    toggle.setAttribute("aria-label", title);
    toggle.addEventListener("click", onToggle);
    row.append(text, toggle);
    return row;
  }

  function bindCommit(control, callback, eventName = "change") {
    control.addEventListener(eventName, () => mutate(() => callback(control)));
  }

  function renderDocumentProperties() {
    $('[data-role="properties-caption"]').textContent = "Configurações do fluxo";
    const name = textInput("flow-name", state.doc.flow.name, "Nome do processo");
    bindCommit(name, (input) => { state.doc.flow.name = input.value.trim() || "Processo sem nome"; });
    const description = textarea("flow-description", state.doc.flow.description);
    bindCommit(description, (input) => { state.doc.flow.description = input.value; });
    const status = selectInput("flow-status", state.doc.flow.status, [["draft", "Rascunho"], ["active", "Ativo"], ["archived", "Arquivado"]]);
    bindCommit(status, (input) => { state.doc.flow.status = input.value; });
    const orientation = selectInput("flow-orientation", state.doc.flow.orientation, [["LR", "Esquerda → direita"], ["TB", "Cima → baixo"], ["RL", "Direita → esquerda"], ["BT", "Baixo → cima"]]);
    bindCommit(orientation, (input) => { state.doc.flow.orientation = input.value; });
    propertiesBody.append(
      fieldGroup("Nome", name),
      fieldGroup("Descrição", description),
      fieldGroup("Status", status),
      fieldGroup("Orientação do layout", orientation),
      switchRow("Encaixar na grade", "Alinha movimentos ao espaçamento configurado", state.doc.settings.snapToGrid, () => mutate(() => { state.doc.settings.snapToGrid = !state.doc.settings.snapToGrid; })),
    );
  }

  function renderNodeProperties(node) {
    const meta = NODE_TYPES[node.type];
    $('[data-role="properties-caption"]').textContent = meta.label;
    const type = selectInput("type", node.type, Object.entries(NODE_TYPES).map(([value, item]) => [value, item.label]));
    bindCommit(type, (input) => {
      node.type = input.value;
      const outgoing = state.doc.edges.filter((edge) => edge.source === node.id);
      outgoing.forEach((edge, index) => { edge.sourceHandle = node.type === "decision" ? `branch-${index}` : "output"; });
      if (node.type !== "decision") {
        node.data.preferredEdgeId = null;
        delete state.branchChoices[node.id];
      }
    });
    const label = textInput("label", node.data.label, "Nome da etapa");
    bindCommit(label, (input) => { node.data.label = input.value.trim() || meta.label; });
    const description = textarea("description", node.data.description);
    bindCommit(description, (input) => { node.data.description = input.value; });
    const owner = textInput("owner", node.data.owner, "Pessoa, setor ou sistema");
    bindCommit(owner, (input) => { node.data.owner = input.value; });
    const lane = selectInput("lane", node.laneId || "", [["", "Sem raia"], ...laneGeometry().sorted.map((item) => [item.id, item.name])]);
    bindCommit(lane, (input) => {
      node.laneId = input.value || null;
      if (node.laneId) {
        const box = laneGeometry().map.get(node.laneId);
        node.position.y = clamp(node.position.y, box.top + 48, box.bottom - NODE_HEIGHT - 10);
      }
    });
    const sla = numberInput("sla", node.data.slaMinutes, 0);
    bindCommit(sla, (input) => { node.data.slaMinutes = input.value === "" ? null : Math.max(0, Number(input.value)); });
    const tags = textInput("tags", node.data.tags.join(", "), "Ex.: contrato, aprovação");
    bindCommit(tags, (input) => { node.data.tags = input.value.split(",").map((tag) => tag.trim()).filter(Boolean).slice(0, 12); });

    let preferredBranchField = null;
    if (node.type === "decision") {
      const outgoing = state.doc.edges
        .filter((edge) => edge.source === node.id && edge.enabled !== false)
        .sort((left, right) => String(left.sourceHandle || "").localeCompare(String(right.sourceHandle || "")));
      const branchOptions = [["", "Automática — rota principal mais completa"], ...outgoing.map((edge, index) => [
        edge.id,
        `${index + 1}. ${edge.label || edge.condition || getNode(edge.target)?.data.label || "Saída sem nome"}`,
      ])];
      const branch = selectInput("preferred-branch", node.data.preferredEdgeId || state.branchChoices[node.id] || "", branchOptions);
      bindCommit(branch, (input) => {
        node.data.preferredEdgeId = input.value || null;
        if (input.value) state.branchChoices[node.id] = input.value;
        else delete state.branchChoices[node.id];
      });
      preferredBranchField = fieldGroup("Saída padrão para destaque e Play", branch);
    }

    const coordinates = el("div", "property-row");
    const x = numberInput("x", Math.round(node.position.x));
    const y = numberInput("y", Math.round(node.position.y));
    bindCommit(x, (input) => { node.position.x = snap(Math.max(0, Number(input.value) || 0)); });
    bindCommit(y, (input) => { node.position.y = snap(constrainNodeYToLane(node, Number(input.value) || 0)); });
    coordinates.append(fieldGroup("Posição X", x), fieldGroup("Posição Y", y));

    const actions = el("div", "property-actions");
    const duplicate = el("button", "", "Duplicar");
    duplicate.addEventListener("click", () => duplicateSelected());
    const remove = el("button", "danger", "Excluir");
    remove.addEventListener("click", () => deleteSelected());
    actions.append(duplicate, remove);

    const propertyItems = [
      fieldGroup("Tipo", type), fieldGroup("Nome", label), fieldGroup("Descrição", description),
      fieldGroup("Responsável", owner), fieldGroup("Raia", lane), fieldGroup("SLA em minutos", sla),
      fieldGroup("Tags separadas por vírgula", tags),
    ];
    if (preferredBranchField) propertyItems.push(preferredBranchField);
    propertyItems.push(
      coordinates,
      switchRow("Elemento ativo", "Itens inativos permanecem visíveis, mas são ignorados na validação principal", node.data.enabled, () => mutate(() => { node.data.enabled = !node.data.enabled; })),
      switchRow("Posição bloqueada", "Impede o arraste manual e o auto-layout", node.data.locked, () => mutate(() => { node.data.locked = !node.data.locked; })),
      actions,
    );
    propertiesBody.append(...propertyItems);
  }

  function renderEdgeProperties(edge) {
    $('[data-role="properties-caption"]').textContent = "Conexão";
    const label = textInput("edge-label", edge.label, "Ex.: Sim, Não, Aprovado");
    bindCommit(label, (input) => { edge.label = input.value; });
    const condition = textarea("edge-condition", edge.condition || "");
    bindCommit(condition, (input) => { edge.condition = input.value; });
    const type = selectInput("edge-type", edge.type, [["smoothstep", "Curva suave"], ["step", "Ortogonal"], ["straight", "Linha reta"]]);
    bindCommit(type, (input) => { edge.type = input.value; });
    const source = getNode(edge.source)?.data.label || edge.source;
    const target = getNode(edge.target)?.data.label || edge.target;
    const route = el("div", "property-group");
    route.append(el("label", "", "Percurso"), el("div", "switch-row", `${source} → ${target}`));
    const actions = el("div", "property-actions");
    const reverse = el("button", "", "Inverter");
    reverse.addEventListener("click", () => mutate(() => {
      [edge.source, edge.target] = [edge.target, edge.source];
      const newSource = getNode(edge.source);
      edge.sourceHandle = newSource?.type === "decision" ? nextDecisionHandle(newSource.id) : "output";
      edge.targetHandle = "input";
    }));
    const remove = el("button", "danger", "Excluir");
    remove.addEventListener("click", () => deleteSelected());
    actions.append(reverse, remove);
    propertiesBody.append(
      route,
      fieldGroup("Rótulo", label),
      fieldGroup("Condição", condition),
      fieldGroup("Estilo", type),
      switchRow("Conexão ativa", "Conexões inativas são exibidas com linha tracejada", edge.enabled, () => mutate(() => { edge.enabled = !edge.enabled; })),
      actions,
    );
  }

  function renderLaneProperties(lane) {
    $('[data-role="properties-caption"]').textContent = "Raia do processo";
    const name = textInput("lane-name", lane.name, "Nome da raia");
    bindCommit(name, (input) => { lane.name = input.value.trim() || "Raia"; });
    const order = numberInput("lane-order", lane.order, 1);
    bindCommit(order, (input) => { lane.order = Math.max(1, Number(input.value) || 1); });
    const height = numberInput("lane-height", lane.height, 110);
    bindCommit(height, (input) => { lane.height = clamp(Number(input.value) || 240, 110, 1200); });
    const color = el("input");
    color.type = "color";
    color.value = lane.color || "#EEF2FF";
    bindCommit(color, (input) => { lane.color = input.value; }, "input");
    const actions = el("div", "property-actions");
    const addTask = el("button", "", "Adicionar etapa");
    addTask.addEventListener("click", () => addNode("task", 220, laneGeometry().map.get(lane.id).top + 76, lane.id));
    const remove = el("button", "danger", "Excluir raia");
    remove.addEventListener("click", () => deleteSelected());
    actions.append(addTask, remove);
    propertiesBody.append(
      fieldGroup("Nome", name),
      fieldGroup("Ordem", order),
      fieldGroup("Altura", height),
      fieldGroup("Cor", color),
      switchRow("Raia ativa", "Ao desativar, os itens internos permanecem disponíveis", lane.enabled, () => mutate(() => { lane.enabled = !lane.enabled; })),
      switchRow("Recolher raia", "Mostra somente o cabeçalho", lane.collapsed, () => mutate(() => { lane.collapsed = !lane.collapsed; })),
      actions,
    );
  }

  function renderProperties() {
    propertiesBody.innerHTML = "";
    if (!state.selected) return renderDocumentProperties();
    if (state.selected.kind === "node") {
      const node = getNode(state.selected.id);
      return node ? renderNodeProperties(node) : renderDocumentProperties();
    }
    if (state.selected.kind === "edge") {
      const edge = getEdge(state.selected.id);
      return edge ? renderEdgeProperties(edge) : renderDocumentProperties();
    }
    if (state.selected.kind === "lane") {
      const lane = getLane(state.selected.id);
      return lane ? renderLaneProperties(lane) : renderDocumentProperties();
    }
  }

  function renderAll() {
    updateWorldSize();
    renderViewport();
    renderLanes();
    renderNodes();
    renderEdges();
    renderProperties();
    renderHeaderAndStatus();
    renderMinimap();
  }

  function selectItem(kind, id) {
    state.selected = kind && id ? { kind, id } : null;
    if (kind === "edge" && id) {
      const edge = getEdge(id);
      const source = edge ? getNode(edge.source) : null;
      if (edge && source?.type === "decision") state.branchChoices[source.id] = edge.id;
    }
    renderAll();
  }

  function addNode(type, x, y, laneId = null) {
    const meta = NODE_TYPES[type] || NODE_TYPES.task;
    const id = uid("node");
    mutate(() => {
      state.doc.nodes.push({
        id,
        type,
        laneId: laneId || laneAtY(y),
        position: { x: snap(Math.max(0, x)), y: snap(Math.max(0, y)) },
        data: { label: meta.label, description: "", owner: "", enabled: true, locked: false, slaMinutes: null, tags: [] },
      });
      state.selected = { kind: "node", id };
    });
  }

  function addLane() {
    const order = state.doc.lanes.length + 1;
    const colors = ["#EEF2FF", "#ECFDF5", "#FFF7ED", "#FDF2F8", "#ECFEFF"];
    const id = uid("lane");
    mutate(() => {
      state.doc.lanes.push({ id, name: `Raia ${order}`, orientation: "horizontal", order, color: colors[(order - 1) % colors.length], collapsed: false, enabled: true, height: 240 });
      state.selected = { kind: "lane", id };
    }, "Raia adicionada");
  }

  function duplicateSelected() {
    if (state.selected?.kind !== "node") return;
    const source = getNode(state.selected.id);
    if (!source) return;
    const duplicate = clone(source);
    duplicate.id = uid("node");
    duplicate.position.x = Math.max(0, source.position.x + 40);
    duplicate.position.y = constrainNodeYToLane(duplicate, source.position.y + 40);
    duplicate.data.label = `${source.data.label} (cópia)`;
    mutate(() => {
      state.doc.nodes.push(duplicate);
      state.selected = { kind: "node", id: duplicate.id };
    }, "Elemento duplicado");
  }

  function deleteSelected() {
    if (!state.selected) return;
    const selected = clone(state.selected);
    mutate(() => {
      if (selected.kind === "node") {
        state.doc.nodes = state.doc.nodes.filter((node) => node.id !== selected.id);
        state.doc.edges = state.doc.edges.filter((edge) => edge.source !== selected.id && edge.target !== selected.id);
      } else if (selected.kind === "edge") {
        state.doc.edges = state.doc.edges.filter((edge) => edge.id !== selected.id);
      } else if (selected.kind === "lane") {
        state.doc.lanes = state.doc.lanes.filter((lane) => lane.id !== selected.id);
        state.doc.nodes.forEach((node) => { if (node.laneId === selected.id) node.laneId = null; });
      }
      state.selected = null;
    }, "Item excluído");
  }

  function beginNodeDrag(event, nodeId) {
    if (event.button !== 0 || event.target.classList.contains("node-port")) return;
    const node = getNode(nodeId);
    if (!node || node.data.locked) return;
    event.stopPropagation();
    selectItem("node", nodeId);
    const start = worldPoint(event.clientX, event.clientY);
    state.dragging = { kind: "node", id: nodeId, start, origin: clone(node.position), snapshot: clone(state.doc) };
  }

  function beginLaneResize(event, laneId, initialHeight) {
    event.stopPropagation();
    const start = worldPoint(event.clientX, event.clientY);
    state.dragging = { kind: "lane-resize", id: laneId, start, initialHeight, snapshot: clone(state.doc) };
  }

  function beginConnection(event, sourceId, sourceHandle = "output") {
    event.stopPropagation();
    event.preventDefault();
    const source = getNode(sourceId);
    if (!source) return;
    state.connecting = { source: sourceId, sourceHandle };
    state.pointerWorld = nodeCenter(source, "source", sourceHandle);
    renderAll();
  }

  function finishConnection(event, targetId) {
    event.stopPropagation();
    if (!state.connecting) return;
    const sourceId = state.connecting.source;
    const sourceHandle = state.connecting.sourceHandle || "output";
    state.connecting = null;
    if (sourceId === targetId) {
      renderAll();
      return toast("Um elemento não pode ser ligado a ele mesmo", "warning");
    }
    const sourceNode = getNode(sourceId);
    const existing = state.doc.edges.some((edge) => {
      if (edge.source !== sourceId || edge.target !== targetId) return false;
      return sourceNode?.type === "decision"
        ? String(edge.sourceHandle || "branch-0") === String(sourceHandle)
        : true;
    });
    if (existing) {
      renderAll();
      return toast(
        sourceNode?.type === "decision"
          ? "Essa saída da decisão já está ligada a esse elemento"
          : "Essa conexão já existe",
        "warning",
      );
    }
    const id = uid("edge");
    mutate(() => {
      state.doc.edges.push({ id, source: sourceId, target: targetId, sourceHandle, targetHandle: "input", type: "smoothstep", label: "", enabled: true, condition: "" });
      state.selected = { kind: "edge", id };
    }, "Elementos conectados");
  }

  function onPointerMove(event) {
    state.pointerWorld = worldPoint(event.clientX, event.clientY);
    if (state.connecting) renderTemporaryEdge();

    if (state.dragging?.kind === "node") {
      const node = getNode(state.dragging.id);
      if (!node) return;
      const point = worldPoint(event.clientX, event.clientY);
      const dx = point.x - state.dragging.start.x;
      const dy = point.y - state.dragging.start.y;
      node.position.x = snap(Math.max(0, state.dragging.origin.x + dx));
      node.position.y = snap(Math.max(0, state.dragging.origin.y + dy));
      const detectedLane = laneAtY(node.position.y + NODE_HEIGHT / 2);
      if (detectedLane) node.laneId = detectedLane;
      node.position.y = snap(constrainNodeYToLane(node, node.position.y));
      updateWorldSize();
      renderNodes();
      renderEdges();
      renderMinimap();
      return;
    }

    if (state.dragging?.kind === "lane-resize") {
      const lane = getLane(state.dragging.id);
      if (!lane) return;
      const point = worldPoint(event.clientX, event.clientY);
      lane.height = clamp(state.dragging.initialHeight + (point.y - state.dragging.start.y), 110, 1200);
      renderLanes();
      renderNodes();
      renderEdges();
      renderMinimap();
      return;
    }

    if (state.panning) {
      const dx = event.clientX - state.panning.startX;
      const dy = event.clientY - state.panning.startY;
      state.doc.viewport.x = state.panning.originX + dx;
      state.doc.viewport.y = state.panning.originY + dy;
      renderViewport();
      renderMinimap();
    }
  }

  function onPointerUp() {
    if (state.dragging) {
      state.history.push(state.dragging.snapshot);
      state.future = [];
      state.dragging = null;
      markDirty();
      renderAll();
    }
    state.panning = null;
    viewport.classList.remove("is-panning");
  }

  function beginPan(event) {
    const background = event.target === viewport || event.target === world || event.target === laneLayer || event.target.classList.contains("canvas-world");
    const panIntent = event.button === 1 || state.spaceDown || (event.button === 0 && background);
    if (!panIntent) return;
    event.preventDefault();
    if (event.button === 0 && background && !state.spaceDown) selectItem(null, null);
    state.panning = {
      startX: event.clientX,
      startY: event.clientY,
      originX: Number(state.doc.viewport.x) || 0,
      originY: Number(state.doc.viewport.y) || 0,
    };
    viewport.classList.add("is-panning");
  }

  function zoomAt(clientX, clientY, delta) {
    const rect = viewport.getBoundingClientRect();
    const before = worldPoint(clientX, clientY);
    const oldZoom = state.doc.viewport.zoom;
    const nextZoom = clamp(oldZoom * delta, MIN_ZOOM, MAX_ZOOM);
    state.doc.viewport.zoom = nextZoom;
    state.doc.viewport.x = clientX - rect.left - before.x * nextZoom;
    state.doc.viewport.y = clientY - rect.top - before.y * nextZoom;
    renderViewport();
    renderMinimap();
  }

  function fitView() {
    if (!state.doc.nodes.length && !state.doc.lanes.length) {
      state.doc.viewport = { x: 0, y: 0, zoom: 1 };
      return renderViewport();
    }
    const rect = viewport.getBoundingClientRect();
    const geometry = laneGeometry();
    let minX = 25, minY = 20, maxX = 900, maxY = Math.max(500, geometry.totalHeight);
    state.doc.nodes.forEach((node) => {
      const dimensions = nodeDimensions(node);
      minX = Math.min(minX, node.position.x - 60);
      minY = Math.min(minY, node.position.y - 60);
      maxX = Math.max(maxX, node.position.x + dimensions.width + 100);
      maxY = Math.max(maxY, node.position.y + dimensions.height + 80);
    });
    const width = maxX - minX;
    const height = maxY - minY;
    const zoom = clamp(Math.min((rect.width - 80) / width, (rect.height - 70) / height), MIN_ZOOM, 1.25);
    state.doc.viewport.zoom = zoom;
    state.doc.viewport.x = (rect.width - width * zoom) / 2 - minX * zoom;
    state.doc.viewport.y = (rect.height - height * zoom) / 2 - minY * zoom;
    renderViewport();
    renderMinimap();
  }

  function fitNodeIds(nodeIds) {
    const ids = nodeIds instanceof Set ? nodeIds : new Set(nodeIds || []);
    const nodes = state.doc.nodes.filter((node) => ids.has(node.id));
    if (!nodes.length) return fitView();
    const rect = viewport.getBoundingClientRect();
    let minX = Infinity, minY = Infinity, maxX = -Infinity, maxY = -Infinity;
    nodes.forEach((node) => {
      const dimensions = nodeDimensions(node);
      minX = Math.min(minX, node.position.x - 70);
      minY = Math.min(minY, node.position.y - 70);
      maxX = Math.max(maxX, node.position.x + dimensions.width + 70);
      maxY = Math.max(maxY, node.position.y + dimensions.height + 70);
    });
    const width = Math.max(240, maxX - minX);
    const height = Math.max(160, maxY - minY);
    const zoom = clamp(Math.min((rect.width - 90) / width, (rect.height - 90) / height), .38, 1.35);
    state.doc.viewport.zoom = zoom;
    state.doc.viewport.x = (rect.width - width * zoom) / 2 - minX * zoom;
    state.doc.viewport.y = (rect.height - height * zoom) / 2 - minY * zoom;
    world.classList.add("is-animating");
    renderViewport();
    renderMinimap();
    setTimeout(() => world.classList.remove("is-animating"), 350);
  }

  function centerOnNode(nodeId) {
    const node = getNode(nodeId);
    if (!node) return;
    const dimensions = nodeDimensions(node);
    const rect = viewport.getBoundingClientRect();
    const zoom = clamp(Math.max(Number(state.doc.viewport.zoom) || 1, .9), .9, 1.2);
    state.doc.viewport.zoom = zoom;
    state.doc.viewport.x = rect.width / 2 - (node.position.x + dimensions.width / 2) * zoom;
    state.doc.viewport.y = rect.height / 2 - (node.position.y + dimensions.height / 2) * zoom;
    world.classList.add("is-animating");
    renderViewport();
    renderMinimap();
    setTimeout(() => world.classList.remove("is-animating"), 350);
  }

  function ensureUnassignedLane() {
    const laneIds = new Set(state.doc.lanes.map((lane) => lane.id));
    const missing = state.doc.nodes.filter((node) => !node.laneId || !laneIds.has(node.laneId));
    if (!missing.length) return null;
    let lane = state.doc.lanes.find((item) => item.id === "lane_unassigned");
    if (!lane) {
      const order = state.doc.lanes.reduce((max, item) => Math.max(max, Number(item.order) || 0), 0) + 1;
      lane = {
        id: "lane_unassigned",
        name: "Sem raia definida",
        orientation: "horizontal",
        order,
        color: "#F8FAFC",
        collapsed: false,
        enabled: true,
        height: 240,
      };
      state.doc.lanes.push(lane);
    }
    missing.forEach((node) => { node.laneId = lane.id; });
    return lane;
  }

  function computeLayoutLevels(nodes, edges) {
    const xValues = nodes.map((node) => Number(node.position.x) || 0).sort((a, b) => a - b);
    const xSpread = xValues.length ? xValues[xValues.length - 1] - xValues[0] : 0;
    if (nodes.length >= 40 && xSpread >= LEVEL_GAP * 5) {
      const columns = [];
      xValues.forEach((value) => {
        const last = columns[columns.length - 1];
        if (last === undefined || Math.abs(value - last) > LEVEL_GAP * 0.38) columns.push(value);
        else columns[columns.length - 1] = (last + value) / 2;
      });
      const levels = new Map();
      nodes.forEach((node) => {
        const x = Number(node.position.x) || 0;
        let bestIndex = 0;
        let bestDistance = Infinity;
        columns.forEach((columnX, index) => {
          const distance = Math.abs(columnX - x);
          if (distance < bestDistance) { bestDistance = distance; bestIndex = index; }
        });
        levels.set(node.id, bestIndex);
      });
      return levels;
    }

    const nodeIds = new Set(nodes.map((node) => node.id));
    const adjacency = new Map(nodes.map((node) => [node.id, []]));
    edges.forEach((edge) => {
      if (nodeIds.has(edge.source) && nodeIds.has(edge.target)) adjacency.get(edge.source).push(edge.target);
    });

    let sequence = 0;
    const indexByNode = new Map();
    const lowLink = new Map();
    const stack = [];
    const onStack = new Set();
    const components = [];

    function strongConnect(nodeId) {
      indexByNode.set(nodeId, sequence);
      lowLink.set(nodeId, sequence);
      sequence += 1;
      stack.push(nodeId);
      onStack.add(nodeId);

      for (const targetId of adjacency.get(nodeId) || []) {
        if (!indexByNode.has(targetId)) {
          strongConnect(targetId);
          lowLink.set(nodeId, Math.min(lowLink.get(nodeId), lowLink.get(targetId)));
        } else if (onStack.has(targetId)) {
          lowLink.set(nodeId, Math.min(lowLink.get(nodeId), indexByNode.get(targetId)));
        }
      }

      if (lowLink.get(nodeId) === indexByNode.get(nodeId)) {
        const componentNodes = [];
        while (stack.length) {
          const current = stack.pop();
          onStack.delete(current);
          componentNodes.push(current);
          if (current === nodeId) break;
        }
        components.push(componentNodes);
      }
    }

    nodes.forEach((node) => {
      if (!indexByNode.has(node.id)) strongConnect(node.id);
    });

    const componentByNode = new Map();
    components.forEach((members, componentId) => members.forEach((nodeId) => componentByNode.set(nodeId, componentId)));
    const dag = new Map(components.map((_, index) => [index, new Set()]));
    const indegree = new Map(components.map((_, index) => [index, 0]));
    edges.forEach((edge) => {
      const sourceComponent = componentByNode.get(edge.source);
      const targetComponent = componentByNode.get(edge.target);
      if (sourceComponent === undefined || targetComponent === undefined || sourceComponent === targetComponent) return;
      if (!dag.get(sourceComponent).has(targetComponent)) {
        dag.get(sourceComponent).add(targetComponent);
        indegree.set(targetComponent, indegree.get(targetComponent) + 1);
      }
    });

    const queue = [...indegree.entries()].filter(([, degree]) => degree === 0).map(([componentId]) => componentId);
    const componentLevel = new Map(queue.map((componentId) => [componentId, 0]));
    let cursor = 0;
    while (cursor < queue.length) {
      const componentId = queue[cursor++];
      const currentLevel = componentLevel.get(componentId) || 0;
      for (const targetComponent of dag.get(componentId) || []) {
        componentLevel.set(targetComponent, Math.max(componentLevel.get(targetComponent) || 0, currentLevel + 1));
        indegree.set(targetComponent, indegree.get(targetComponent) - 1);
        if (indegree.get(targetComponent) === 0) queue.push(targetComponent);
      }
    }

    const levels = new Map();
    components.forEach((members, componentId) => {
      const baseLevel = componentLevel.get(componentId) || 0;
      const ordered = [...members].sort((leftId, rightId) => {
        const left = getNode(leftId);
        const right = getNode(rightId);
        return (Number(left?.position.x) || 0) - (Number(right?.position.x) || 0)
          || (Number(left?.position.y) || 0) - (Number(right?.position.y) || 0);
      });
      ordered.forEach((nodeId, index) => levels.set(nodeId, baseLevel + (ordered.length > 1 ? index : 0)));
    });

    return levels;
  }

  function layoutDocumentInPlace() {
    if (!state.doc.nodes.length) return { columns: 0, moved: 0 };
    ensureUnassignedLane();
    const layoutNodes = [...state.doc.nodes];
    const activeIds = new Set(layoutNodes.map((node) => node.id));
    const layoutEdges = state.doc.edges.filter((edge) => edge.enabled !== false && activeIds.has(edge.source) && activeIds.has(edge.target));
    const levels = computeLayoutLevels(layoutNodes, layoutEdges);
    const maxLevel = Math.max(0, ...levels.values());
    const laneBuckets = new Map();

    layoutNodes.forEach((node) => {
      const laneId = node.laneId || "lane_unassigned";
      const level = levels.get(node.id) || 0;
      if (!laneBuckets.has(laneId)) laneBuckets.set(laneId, new Map());
      const buckets = laneBuckets.get(laneId);
      if (!buckets.has(level)) buckets.set(level, []);
      buckets.get(level).push(node);
    });

    state.doc.lanes.forEach((lane) => {
      const levelsForLane = laneBuckets.get(lane.id);
      const maxRows = levelsForLane
        ? Math.max(1, ...[...levelsForLane.values()].map((items) => items.length))
        : 1;
      const requiredHeight = LANE_CONTENT_TOP + maxRows * LANE_ROW_GAP + LANE_BOTTOM_PADDING;
      lane.height = clamp(Math.max(Number(lane.height) || 240, requiredHeight), 150, 1200);
      lane.collapsed = false;
    });

    const geometry = laneGeometry();
    const orientation = state.doc.flow.orientation || "LR";
    let moved = 0;
    laneBuckets.forEach((levelsForLane, laneId) => {
      const laneBox = geometry.map.get(laneId);
      if (!laneBox) return;
      [...levelsForLane.entries()].sort((a, b) => a[0] - b[0]).forEach(([level, items]) => {
        const ordered = [...items].sort((left, right) => {
          return (Number(left.position.y) || 0) - (Number(right.position.y) || 0)
            || String(left.data.label || "").localeCompare(String(right.data.label || ""));
        });
        ordered.forEach((node, row) => {
          if (node.data.locked === true) return;
          const visualLevel = orientation === "RL" ? maxLevel - level : level;
          const nextX = 120 + visualLevel * LEVEL_GAP;
          const nextY = laneBox.top + LANE_CONTENT_TOP + row * LANE_ROW_GAP;
          if (node.position.x !== nextX || node.position.y !== nextY) moved += 1;
          node.position.x = nextX;
          node.position.y = nextY;
        });
      });
    });

    normalizeNodesIntoLanes();
    updateWorldSize();
    return { columns: maxLevel + 1, moved };
  }

  function countLayoutProblems() {
    const geometry = laneGeometry();
    let outside = 0;
    let overlaps = 0;
    const byLane = new Map();
    state.doc.nodes.forEach((node) => {
      const box = geometry.map.get(node.laneId);
      if (!box || node.position.y < box.top + LANE_HEADER_HEIGHT || node.position.y + NODE_HEIGHT > box.bottom) outside += 1;
      const laneId = node.laneId || "__none__";
      if (!byLane.has(laneId)) byLane.set(laneId, []);
      byLane.get(laneId).push(node);
    });
    byLane.forEach((items) => {
      for (let leftIndex = 0; leftIndex < items.length; leftIndex += 1) {
        const left = items[leftIndex];
        const leftSize = nodeDimensions(left);
        for (let rightIndex = leftIndex + 1; rightIndex < items.length; rightIndex += 1) {
          const right = items[rightIndex];
          const rightSize = nodeDimensions(right);
          const separated = left.position.x + leftSize.width + 14 < right.position.x
            || right.position.x + rightSize.width + 14 < left.position.x
            || left.position.y + leftSize.height + 10 < right.position.y
            || right.position.y + rightSize.height + 10 < left.position.y;
          if (!separated) overlaps += 1;
        }
      }
    });
    return { outside, overlaps };
  }

  function autoLayout() {
    if (!state.doc.nodes.length) return toast("Adicione elementos antes de organizar", "info");
    let summary = null;
    mutate(() => {
      summary = layoutDocumentInPlace();
    });
    toast(`Fluxo organizado em ${summary.columns} colunas; ${summary.moved} elementos reposicionados.`, "success");
    setTimeout(fitView, 40);
  }

  function validationReport() {
    const issues = [];
    const activeNodes = state.doc.nodes.filter((node) => node.data.enabled !== false);
    const activeEdges = state.doc.edges.filter((edge) => edge.enabled !== false);
    const activeIds = new Set(activeNodes.map((node) => node.id));
    const starts = activeNodes.filter((node) => node.type === "start");
    const ends = activeNodes.filter((node) => node.type === "end");
    if (!starts.length) issues.push({ level: "error", message: "O fluxo não possui um elemento de início." });
    if (!ends.length) issues.push({ level: "error", message: "O fluxo não possui um elemento de fim." });
    if (starts.length > 1) issues.push({ level: "warning", message: `O fluxo possui ${starts.length} elementos de início.` });
    activeNodes.forEach((node) => {
      if (!String(node.data.label || "").trim()) issues.push({ level: "error", message: `O elemento ${node.id} está sem nome.`, target: { kind: "node", id: node.id } });
      const incoming = activeEdges.filter((edge) => edge.target === node.id && activeIds.has(edge.source));
      const outgoing = activeEdges.filter((edge) => edge.source === node.id && activeIds.has(edge.target));
      if (node.type !== "start" && !incoming.length) issues.push({ level: "warning", message: `“${node.data.label}” não possui conexão de entrada.`, target: { kind: "node", id: node.id } });
      if (node.type !== "end" && !outgoing.length) issues.push({ level: "warning", message: `“${node.data.label}” não possui conexão de saída.`, target: { kind: "node", id: node.id } });
      if (node.type === "decision") {
        if (outgoing.length < 2) {
          issues.push({ level: "error", message: `A decisão “${node.data.label}” deve possuir no mínimo duas conexões de saída.`, target: { kind: "node", id: node.id } });
        }
        const usedHandles = outgoing.map((edge) => edge.sourceHandle || "branch-0");
        if (new Set(usedHandles).size < outgoing.length) {
          issues.push({ level: "warning", message: `A decisão “${node.data.label}” possui saídas usando o mesmo conector visual. Use conectores diferentes para melhorar a leitura.`, target: { kind: "node", id: node.id } });
        }
        outgoing.filter((edge) => !String(edge.label || edge.condition || "").trim()).forEach((edge) => {
          issues.push({ level: "warning", message: `Uma saída da decisão “${node.data.label}” está sem rótulo ou condição.`, target: { kind: "edge", id: edge.id } });
        });
      }
      if (!node.laneId && state.doc.lanes.length) issues.push({ level: "warning", message: `“${node.data.label}” está fora de uma raia.`, target: { kind: "node", id: node.id } });
      if (node.laneId && state.doc.lanes.length) {
        const laneBox = laneGeometry().map.get(node.laneId);
        if (!laneBox || node.position.y < laneBox.top + LANE_HEADER_HEIGHT || node.position.y + NODE_HEIGHT > laneBox.bottom) {
          issues.push({ level: "error", message: `“${node.data.label}” não está posicionado dentro dos limites da raia selecionada.`, target: { kind: "node", id: node.id } });
        }
      }
    });
    activeEdges.forEach((edge) => {
      if (!activeIds.has(edge.source) || !activeIds.has(edge.target)) issues.push({ level: "error", message: `A conexão ${edge.id} aponta para um elemento inexistente ou inativo.`, target: { kind: "edge", id: edge.id } });
      if (edge.source === edge.target) issues.push({ level: "error", message: `A conexão ${edge.id} liga um elemento a ele mesmo.`, target: { kind: "edge", id: edge.id } });
    });
    state.doc.lanes.filter((lane) => lane.enabled !== false).forEach((lane) => {
      if (!activeNodes.some((node) => node.laneId === lane.id)) issues.push({ level: "warning", message: `A raia “${lane.name}” está vazia.`, target: { kind: "lane", id: lane.id } });
    });
    return issues;
  }

  function showValidation() {
    const issues = validationReport();
    const modal = $('[data-role="modal"]');
    const body = $('[data-role="modal-body"]');
    const title = $('[data-role="modal-title"]');
    title.textContent = "Validação do fluxo";
    body.innerHTML = "";
    if (!issues.length) {
      body.appendChild(el("div", "validation-ok", "Fluxo válido. Nenhum problema estrutural foi encontrado."));
    } else {
      const list = el("div", "validation-list");
      issues.forEach((issue) => {
        const item = el("button", `validation-item ${issue.level}`);
        item.type = "button";
        item.append(el("span", "", issue.level === "error" ? "●" : "▲"), el("span", "", issue.message));
        if (issue.target) {
          item.addEventListener("click", () => {
            modal.hidden = true;
            selectItem(issue.target.kind, issue.target.id);
          });
        }
        list.appendChild(item);
      });
      body.appendChild(list);
    }
    modal.hidden = false;
  }

  function exportJson() {
    const payload = JSON.stringify(state.doc, null, 2);
    const blob = new Blob([payload], { type: "application/json;charset=utf-8" });
    const url = URL.createObjectURL(blob);
    const anchor = document.createElement("a");
    const safeName = (state.doc.flow.name || "fluxo").normalize("NFD").replace(/[\u0300-\u036f]/g, "").replace(/[^a-zA-Z0-9_-]+/g, "_").replace(/^_+|_+$/g, "").toLowerCase();
    anchor.href = url;
    anchor.download = `${safeName || "fluxo"}.json`;
    anchor.click();
    setTimeout(() => URL.revokeObjectURL(url), 1000);
    toast("JSON exportado", "success");
  }

  function importJson(file) {
    if (!file) return;
    const reader = new FileReader();
    reader.onload = () => {
      try {
        const parsed = JSON.parse(String(reader.result));
        const normalized = normalizeDocument(parsed);
        if (!normalized.flow || !Array.isArray(normalized.nodes) || !Array.isArray(normalized.edges)) throw new Error("Estrutura incompatível");
        // A importação substitui o conteúdo do fluxo aberto sem trocar sua identidade no banco.
        normalized.flow.id = state.doc.flow.id;
        normalized.flow.createdAt = state.doc.flow.createdAt;
        normalized.flow.createdBy = state.doc.flow.createdBy;
        normalized.flow.updatedAt = nowIso();
        checkpoint();
        state.doc = normalized;
        state.selected = null;
        state.focusPath = null;
        state.branchChoices = {};
        state.doc.nodes.forEach((node) => {
          if (node.type === "decision" && node.data.preferredEdgeId) state.branchChoices[node.id] = node.data.preferredEdgeId;
        });
        clearPlaybackTimer();
        state.playback.running = false;
        resetPlaybackVisuals();
        state.future = [];

        ensureUnassignedLane();
        normalizeNodesIntoLanes();
        const initialProblems = countLayoutProblems();
        const shouldOrganize = state.doc.nodes.length >= 60 || initialProblems.outside > 0 || initialProblems.overlaps > 2;
        const layoutSummary = shouldOrganize ? layoutDocumentInPlace() : null;

        markDirty();
        renderAll();
        setTimeout(fitView, 40);
        if (layoutSummary) {
          toast(`Fluxo grande importado e organizado em ${layoutSummary.columns} colunas, respeitando todas as raias.`, "success");
        } else {
          toast("Fluxo importado e ajustado às raias. Clique em Salvar para persistir.", "success");
        }
      } catch (error) {
        toast(`Não foi possível importar: ${error.message}`, "error");
      }
      fileInput.value = "";
    };
    reader.readAsText(file, "utf-8");
  }

  function save() {
    const issues = validationReport().filter((issue) => issue.level === "error");
    if (issues.length) toast(`O fluxo possui ${issues.length} erro(s), mas será salvo como rascunho.`, "warning");
    state.doc.flow.updatedAt = nowIso();
    $('[data-role="save-state"]').textContent = "Salvando...";
    $('[data-role="save-state"]').style.color = "#4f46e5";
    setTriggerValue("save", clone(state.doc));
  }

  function togglePalettePanel() {
    state.paletteCollapsed = !state.paletteCollapsed;
    localStorage.setItem("produto_tools_palette_collapsed", String(state.paletteCollapsed));
    applyPanelState();
    setTimeout(() => { renderMinimap(); }, 220);
  }

  function toggleInspectorPanel() {
    state.inspectorCollapsed = !state.inspectorCollapsed;
    localStorage.setItem("produto_tools_inspector_collapsed", String(state.inspectorCollapsed));
    applyPanelState();
    setTimeout(() => { renderMinimap(); }, 220);
  }

  function toggleTheme() {
    applyTheme(state.uiTheme === "dark" ? "light" : "dark", true);
    renderAll();
  }

  async function toggleFullscreen() {
    try {
      if (root.classList.contains("pseudo-fullscreen")) {
        root.classList.remove("pseudo-fullscreen");
      } else if (document.fullscreenElement) {
        await document.exitFullscreen();
      } else if (root.requestFullscreen) {
        await root.requestFullscreen({ navigationUI: "hide" });
      } else {
        root.classList.add("pseudo-fullscreen");
      }
    } catch (error) {
      root.classList.add("pseudo-fullscreen");
      toast("O navegador bloqueou a tela cheia nativa; foi usado o modo expandido.", "warning");
    }
    updateFullscreenButton();
    setTimeout(() => { renderMinimap(); fitView(); }, 120);
  }

  function updateFullscreenButton() {
    const button = $('[data-role="fullscreen-button"]');
    if (!button) return;
    const active = Boolean(document.fullscreenElement) || root.classList.contains("pseudo-fullscreen");
    button.textContent = active ? "⛶ Sair" : "⛶ Expandir";
    button.title = active ? "Sair da tela cheia" : "Usar o monitor inteiro";
    button.classList.toggle("is-active", active);
  }

  function activeDecisionBranches(nodeId) {
    return state.doc.edges
      .filter((edge) => edge.source === nodeId && edge.enabled !== false && getNode(edge.target)?.data.enabled !== false)
      .sort((left, right) => {
        const leftMatch = String(left.sourceHandle || "").match(/(\d+)$/);
        const rightMatch = String(right.sourceHandle || "").match(/(\d+)$/);
        return Number(leftMatch?.[1] || 0) - Number(rightMatch?.[1] || 0);
      });
  }

  function showBranchChooser(decisionId, action) {
    const node = getNode(decisionId);
    const branches = activeDecisionBranches(decisionId);
    if (!node || branches.length < 2) return false;
    const modal = $('[data-role="modal"]');
    const body = $('[data-role="modal-body"]');
    const title = $('[data-role="modal-title"]');
    title.textContent = "Escolha a ramificação";
    body.innerHTML = "";
    const intro = el("div", "route-choice-intro");
    intro.append(
      el("strong", "", node.data.label || "Decisão"),
      el("span", "", "Selecione a saída que deve ser usada no destaque e na reprodução a partir deste ponto. A escolha ficará salva no fluxo."),
    );
    const list = el("div", "route-choice-list");
    branches.forEach((edge, index) => {
      const target = getNode(edge.target);
      const button = el("button", "route-choice");
      button.type = "button";
      const badge = el("span", "route-choice-index", String(index + 1));
      const text = el("span", "route-choice-text");
      text.append(
        el("strong", "", edge.label || edge.condition || `Saída ${index + 1}`),
        el("small", "", `Segue para: ${target?.data.label || edge.target}${edge.condition ? ` • ${edge.condition}` : ""}`),
      );
      button.append(badge, text, el("span", "route-choice-arrow", "→"));
      button.addEventListener("click", () => {
        modal.hidden = true;
        mutate(() => {
          state.branchChoices[decisionId] = edge.id;
          node.data.preferredEdgeId = edge.id;
          state.selected = { kind: "edge", id: edge.id };
        });
        if (action === "play") startPlayback(true);
        else applyFocusPath();
      });
      list.appendChild(button);
    });
    body.append(intro, list);
    modal.hidden = false;
    return true;
  }

  function promptSelectedDecisionBranch(action) {
    if (state.selected?.kind !== "node") return false;
    const node = getNode(state.selected.id);
    if (!node || node.type !== "decision") return false;
    const branches = activeDecisionBranches(node.id);
    if (branches.length < 2 || state.branchChoices[node.id]) return false;
    return showBranchChooser(node.id, action);
  }

  function applyFocusPath() {
    const path = buildReadablePath();
    if (!path || !path.nodeSequence.length) {
      toast("Selecione uma etapa ou conexão válida para destacar a rota completa.", "warning");
      return;
    }
    state.focusPath = path;
    renderAll();
    setTimeout(() => fitNodeIds(path.nodeIds), 40);
    const scopeLabel = state.selected?.kind === "lane" ? "Raia destacada" : (state.selected?.kind === "node" ? "Sequência destacada" : "Rota completa destacada");
    toast(`${scopeLabel} com ${path.nodeSequence.length} etapas.`, "success");
  }

  function toggleFocusPath() {
    if (state.focusPath) {
      state.focusPath = null;
      stopPlayback(false);
      renderAll();
      toast("Destaque removido", "info");
      return;
    }
    if (promptSelectedDecisionBranch("focus")) return;
    applyFocusPath();
  }

  function playbackDelay() {
    return Math.max(250, Number($('[data-role="play-speed"]')?.value) || 850);
  }

  function clearPlaybackTimer() {
    if (state.playback.timer) clearTimeout(state.playback.timer);
    state.playback.timer = null;
  }

  function resetPlaybackVisuals() {
    state.playback.index = -1;
    state.playback.currentNodeId = null;
    state.playback.currentEdgeId = null;
    state.playback.visitedNodeIds = new Set();
    state.playback.visitedEdgeIds = new Set();
  }

  function stopPlayback(showMessage = true) {
    clearPlaybackTimer();
    state.playback.running = false;
    state.playback.paused = false;
    state.playback.nodeSequence = [];
    state.playback.edgeSequence = [];
    resetPlaybackVisuals();
    renderAll();
    if (showMessage) toast("Reprodução encerrada", "info");
  }

  function completePlayback() {
    clearPlaybackTimer();
    state.playback.running = false;
    state.playback.paused = false;
    state.playback.currentNodeId = null;
    state.playback.currentEdgeId = null;
    renderAll();
    const doneMessage = state.selected?.kind === "lane" ? "Reprodução da raia concluída." : (state.selected?.kind === "node" ? "Reprodução concluída a partir do card selecionado." : "Reprodução concluída até o fim do fluxo.");
    toast(doneMessage, "success");
  }

  function advancePlayback() {
    if (!state.playback.running || state.playback.paused) return;
    state.playback.index += 1;
    if (state.playback.index >= state.playback.nodeSequence.length) {
      completePlayback();
      return;
    }

    const index = state.playback.index;
    const nodeId = state.playback.nodeSequence[index];
    const edgeId = index > 0 ? state.playback.edgeSequence[index - 1] : null;
    state.playback.currentNodeId = nodeId;
    state.playback.currentEdgeId = edgeId;
    state.playback.visitedNodeIds.add(nodeId);
    if (edgeId) state.playback.visitedEdgeIds.add(edgeId);
    renderNodes();
    renderEdges();
    renderHeaderAndStatus();
    renderMinimap();
    centerOnNode(nodeId);

    state.playback.timer = setTimeout(() => {
      state.playback.currentEdgeId = null;
      advancePlayback();
    }, playbackDelay());
  }

  function startPlayback(skipPrompt = false) {
    if (!skipPrompt && promptSelectedDecisionBranch("play")) return;
    const path = state.focusPath || buildReadablePath();
    if (!path || !path.nodeSequence.length) {
      toast("Não foi encontrada uma rota completa ativa para reproduzir.", "warning");
      return;
    }
    clearPlaybackTimer();
    state.focusPath = path;
    state.playback.running = true;
    state.playback.paused = false;
    state.playback.nodeSequence = [...path.nodeSequence];
    state.playback.edgeSequence = [...path.edgeSequence];
    resetPlaybackVisuals();
    renderAll();
    advancePlayback();
  }

  function togglePlayback() {
    if (!state.playback.running) {
      startPlayback();
      return;
    }
    if (state.playback.paused) {
      state.playback.paused = false;
      renderHeaderAndStatus();
      state.playback.timer = setTimeout(advancePlayback, Math.min(350, playbackDelay()));
    } else {
      state.playback.paused = true;
      clearPlaybackTimer();
      renderHeaderAndStatus();
    }
  }

  function toolbarAction(action) {
    const rect = viewport.getBoundingClientRect();
    const centerX = rect.left + rect.width / 2;
    const centerY = rect.top + rect.height / 2;
    if (action === "undo") undo();
    else if (action === "redo") redo();
    else if (action === "zoom-in") zoomAt(centerX, centerY, 1.15);
    else if (action === "zoom-out") zoomAt(centerX, centerY, 1 / 1.15);
    else if (action === "zoom-reset") { state.doc.viewport = { x: 0, y: 0, zoom: 1 }; renderViewport(); renderMinimap(); }
    else if (action === "fit") fitView();
    else if (action === "fullscreen") toggleFullscreen();
    else if (action === "focus-path") toggleFocusPath();
    else if (action === "play") togglePlayback();
    else if (action === "stop") stopPlayback(true);
    else if (action === "layout") autoLayout();
    else if (action === "validate") showValidation();
    else if (action === "theme") toggleTheme();
    else if (action === "toggle-palette") togglePalettePanel();
    else if (action === "toggle-inspector") toggleInspectorPanel();
    else if (action === "import") fileInput.click();
    else if (action === "export") exportJson();
    else if (action === "save") save();
    else if (action === "add-lane") addLane();
    else if (action === "close-modal") $('[data-role="modal"]').hidden = true;
  }

  function onKeyDown(event) {
    if (["INPUT", "TEXTAREA", "SELECT"].includes(event.target.tagName)) return;
    if (event.code === "Space") { state.spaceDown = true; event.preventDefault(); }
    if ((event.ctrlKey || event.metaKey) && event.key.toLowerCase() === "z") { event.preventDefault(); event.shiftKey ? redo() : undo(); }
    if ((event.ctrlKey || event.metaKey) && event.key.toLowerCase() === "y") { event.preventDefault(); redo(); }
    if ((event.ctrlKey || event.metaKey) && event.key.toLowerCase() === "d") { event.preventDefault(); duplicateSelected(); }
    if ((event.ctrlKey || event.metaKey) && event.key.toLowerCase() === "s") { event.preventDefault(); save(); }
    if ((event.ctrlKey || event.metaKey) && event.shiftKey && event.key.toLowerCase() === "f") { event.preventDefault(); toggleFullscreen(); }
    if (event.key.toLowerCase() === "p") { event.preventDefault(); togglePlayback(); }
    if (event.key.toLowerCase() === "f" && !event.ctrlKey && !event.metaKey) { event.preventDefault(); toggleFocusPath(); }
    if (event.key === "Delete" || event.key === "Backspace") { event.preventDefault(); deleteSelected(); }
    if (event.key === "Escape") {
      state.connecting = null;
      state.selected = null;
      $('[data-role="modal"]').hidden = true;
      if (root.classList.contains("pseudo-fullscreen")) root.classList.remove("pseudo-fullscreen");
      updateFullscreenButton();
      renderAll();
    }
  }

  function onKeyUp(event) {
    if (event.code === "Space") state.spaceDown = false;
  }

  function bindEvents() {
    $$('[data-action]').forEach((button) => button.addEventListener("click", () => toolbarAction(button.dataset.action)));
    $('[data-role="palette-search"]').addEventListener("input", (event) => {
      const query = event.target.value.trim().toLowerCase();
      $$('.palette-item').forEach((item) => { item.style.display = !query || item.dataset.search.includes(query) ? "flex" : "none"; });
    });
    fileInput.addEventListener("change", () => importJson(fileInput.files?.[0]));
    $$('[data-setting]').forEach((input) => input.addEventListener("change", () => {
      mutate(() => { state.doc.settings[input.dataset.setting] = input.checked; });
    }));
    viewport.addEventListener("dragover", (event) => { event.preventDefault(); event.dataTransfer.dropEffect = "copy"; });
    viewport.addEventListener("drop", (event) => {
      event.preventDefault();
      const type = event.dataTransfer.getData("application/x-produto-tools-node");
      if (!NODE_TYPES[type]) return;
      const point = worldPoint(event.clientX, event.clientY);
      addNode(type, point.x - NODE_WIDTH / 2, point.y - NODE_HEIGHT / 2, laneAtY(point.y));
    });
    viewport.addEventListener("pointerdown", beginPan);
    viewport.addEventListener("wheel", (event) => {
      event.preventDefault();
      zoomAt(event.clientX, event.clientY, event.deltaY < 0 ? 1.1 : 1 / 1.1);
    }, { passive: false });
    viewport.addEventListener("click", (event) => {
      if (event.target === viewport || event.target === world) selectItem(null, null);
    });
    window.addEventListener("pointermove", onPointerMove);
    window.addEventListener("pointerup", onPointerUp);
    window.addEventListener("keydown", onKeyDown);
    window.addEventListener("keyup", onKeyUp);
    window.addEventListener("resize", renderMinimap);
    document.addEventListener("fullscreenchange", updateFullscreenButton);
  }

  palette();
  bindEvents();
  ensureUnassignedLane();
  const initialLayoutProblems = countLayoutProblems();
  const repairedLargeLayout = state.doc.nodes.length >= 60
    && (initialLayoutProblems.outside > 0 || initialLayoutProblems.overlaps > 2);
  if (repairedLargeLayout) layoutDocumentInPlace();
  else normalizeNodesIntoLanes();
  renderAll();
  if (repairedLargeLayout) {
    markDirty();
    setTimeout(() => toast("O fluxo grande foi reorganizado dentro das raias. Salve para persistir o novo layout.", "success"), 120);
  }
  setTimeout(fitView, 60);

  return () => {
    state.destroyed = true;
    window.removeEventListener("pointermove", onPointerMove);
    window.removeEventListener("pointerup", onPointerUp);
    window.removeEventListener("keydown", onKeyDown);
    window.removeEventListener("keyup", onKeyUp);
    window.removeEventListener("resize", renderMinimap);
    document.removeEventListener("fullscreenchange", updateFullscreenButton);
    clearPlaybackTimer();
  };
}
