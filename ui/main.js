const list = document.getElementById("list");
const tpl = document.getElementById("row-tpl");
const nodeInfoEl = document.getElementById("node-info");

const rows = new Map();

function humanSize(bytes) {
  const units = ["B", "KiB", "MiB", "GiB", "TiB"];
  let n = bytes, i = 0;
  while (n >= 1024 && i < units.length - 1) { n /= 1024; i++; }
  const fixed = i === 0 ? 0 : n < 10 ? 1 : 0;
  return `${n.toFixed(fixed)} ${units[i]}`;
}
function shortHash(h) { return h.slice(0, 10) + "…"; }

function renderPieceMap(canvas, map) {
  const dpr = window.devicePixelRatio || 1;
  const cssW = canvas.clientWidth;
  const cssH = canvas.clientHeight;
  if (cssW === 0 || cssH === 0) return;
  const w = Math.round(cssW * dpr);
  const h = Math.round(cssH * dpr);
  if (canvas.width !== w || canvas.height !== h) {
    canvas.width = w; canvas.height = h;
  }
  const ctx = canvas.getContext("2d");
  ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
  ctx.clearRect(0, 0, cssW, cssH);
  const n = map.length;
  if (n === 0) return;
  const colW = cssW / n;
  const COLORS = ["#2a2e35", "#58a6ff", "#3fb950"];
  for (let i = 0; i < n; i++) {
    ctx.fillStyle = COLORS[map[i]] || COLORS[0];
    const x0 = Math.floor(i * colW);
    const x1 = Math.floor((i + 1) * colW);
    ctx.fillRect(x0, 0, Math.max(1, x1 - x0), cssH);
  }
}

async function api(path, body) {
  const opts = body ? {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  } : {};
  const res = await fetch(path, opts);
  if (!res.ok) throw new Error(await res.text());
  const txt = await res.text();
  return txt ? JSON.parse(txt) : null;
}

function createRow(hash) {
  const el = tpl.content.firstElementChild.cloneNode(true);
  list.appendChild(el);
  const entry = { el, canvas: el.querySelector(".piece-map"), expanded: false, lastSnap: null };
  rows.set(hash, entry);

  el.querySelector(".row-head").addEventListener("click", () => {
    entry.expanded = !entry.expanded;
    el.classList.toggle("expanded", entry.expanded);
    if (entry.expanded && entry.lastSnap) {
      requestAnimationFrame(() => renderPieceMap(entry.canvas, entry.lastSnap.piece_map));
    }
  });

  el.querySelector(".act-toggle").addEventListener("click", async (e) => {
    e.stopPropagation();
    const paused = el.classList.contains("paused");
    try { await api(paused ? "/api/resume" : "/api/pause", { info_hash: hash }); }
    catch (err) { console.error(err); }
  });

  el.querySelector(".act-remove").addEventListener("click", async (e) => {
    e.stopPropagation();
    try {
      await api("/api/remove", { info_hash: hash });
      el.remove(); rows.delete(hash);
    } catch (err) { console.error(err); }
  });

  return entry;
}

function paint(entry, snap) {
  entry.lastSnap = snap;
  const { el } = entry;
  const pct = snap.piece_count > 0 ? (snap.pieces_have / snap.piece_count) * 100 : 0;
  const done = pct >= 100;

  el.querySelector(".name").textContent = snap.filename;
  el.querySelector(".bar-fill").style.width = `${pct}%`;
  el.querySelector(".pct").textContent = `${pct.toFixed(1)}%`;
  el.querySelector(".m-hash").textContent = shortHash(snap.info_hash);
  el.querySelector(".m-pieces").textContent = `${snap.pieces_have} / ${snap.piece_count} pieces`;
  el.querySelector(".m-size").textContent = humanSize(snap.total_length);
  el.querySelector(".m-peers").textContent = `${snap.peers} ${snap.peers === 1 ? "peer" : "peers"}`;

  const pill = el.querySelector(".pill");
  el.classList.remove("paused", "seeding", "downloading");
  if (snap.is_paused) {
    pill.textContent = "paused"; pill.className = "pill paused";
    el.classList.add("paused");
    el.querySelector(".act-toggle").textContent = "Resume";
  } else if (done) {
    pill.textContent = "seeding"; pill.className = "pill seeding";
    el.classList.add("seeding");
    el.querySelector(".act-toggle").textContent = "Pause";
  } else {
    pill.textContent = "downloading"; pill.className = "pill downloading";
    el.classList.add("downloading");
    el.querySelector(".act-toggle").textContent = "Pause";
  }

  if (entry.expanded) {
    renderPieceMap(entry.canvas, snap.piece_map);
    el.querySelector(".map-info").textContent =
      `${snap.pieces_have} / ${snap.piece_count} pieces complete`;
  }
}

function applySnapshot(snaps) {
  const seen = new Set();
  for (const snap of snaps) {
    seen.add(snap.info_hash);
    let entry = rows.get(snap.info_hash);
    if (!entry) entry = createRow(snap.info_hash);
    paint(entry, snap);
  }
  for (const [hash, entry] of rows) {
    if (!seen.has(hash)) { entry.el.remove(); rows.delete(hash); }
  }
}

document.getElementById("btn-seed").addEventListener("click", async () => {
  const path = prompt("Path to file to seed (e.g. /shared/test1.bin):");
  if (!path) return;
  try { await api("/api/add-seed", { path }); }
  catch (err) { alert("Failed to seed: " + err.message); }
});

document.getElementById("btn-add").addEventListener("click", async () => {
  const yarn_path = prompt("Path to .yarn file:");
  if (!yarn_path) return;
  const output_dir = prompt("Output directory (e.g. /shared/out3):");
  if (!output_dir) return;
  try { await api("/api/add-download", { yarn_path, output_dir }); }
  catch (err) { alert("Failed to add: " + err.message); }
});

async function refreshNodeInfo() {
  try {
    const info = await api("/api/node");
    nodeInfoEl.textContent =
      `${info.node_id.slice(0, 16)}…  ·  tcp ${info.listen_port}  ·  ${info.peer_addr}`;
  } catch { nodeInfoEl.textContent = "engine unavailable"; }
}

const es = new EventSource("/api/events");
es.addEventListener("progress", (e) => {
  try { applySnapshot(JSON.parse(e.data)); } catch {}
});

let resizePending = false;
window.addEventListener("resize", () => {
  if (resizePending) return;
  resizePending = true;
  requestAnimationFrame(() => {
    resizePending = false;
    for (const entry of rows.values()) {
      if (entry.expanded && entry.lastSnap) {
        renderPieceMap(entry.canvas, entry.lastSnap.piece_map);
      }
    }
  });
});

api("/api/torrents").then(applySnapshot).catch(console.error);
refreshNodeInfo();
setInterval(refreshNodeInfo, 5000);