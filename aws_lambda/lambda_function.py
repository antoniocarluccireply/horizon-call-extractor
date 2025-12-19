import os
import uuid
import json
import boto3
import urllib.request
import urllib.error
import traceback
import time
from pypdf import PdfReader
from openpyxl import Workbook

from parser_horizon import parse_calls

s3 = boto3.client(
    "s3",
    region_name="eu-central-1",
    endpoint_url="https://s3.eu-central-1.amazonaws.com",
)

BUCKET = os.environ.get("BUCKET_NAME", "")
def _require_bucket():
    if not BUCKET:
        # Non blocchiamo la UI, ma blocchiamo le API che richiedono S3
        raise RuntimeError("Missing env var BUCKET_NAME")

# --- OpenAI (optional) ---
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY")  # keep secret in Lambda env vars
OPENAI_MODEL = os.environ.get("OPENAI_MODEL", "gpt-5-mini")
OPENAI_MAX_TOPICS = int(os.environ.get("OPENAI_MAX_TOPICS", "25"))
OPENAI_BODY_MAX_CHARS = int(os.environ.get("OPENAI_BODY_MAX_CHARS", "6000"))


def extract_text(pdf_path: str) -> str:
    """
    Extract text with explicit page markers so parser_horizon can set 'page'.
    """
    reader = PdfReader(pdf_path)
    chunks = []
    for idx, p in enumerate(reader.pages, start=1):
        chunks.append(f"\n<<<PAGE {idx}>>>\n")
        chunks.append(p.extract_text() or "")
    return "\n".join(chunks)


def write_xlsx(rows, xlsx_path: str):
    wb = Workbook()
    ws = wb.active
    ws.title = "calls"

    headers = [
        "program",
        "cluster",
        "stage",
        "call_round",
        "page",
        "call_id",
        "topic_id",
        "topic_title",
        "action_type",
        "trl",
        "budget_eur_m",
        "budget_per_project_min_eur_m",
        "budget_per_project_max_eur_m",
        "projects",
        "opening_date",
        "deadline_date",
    ]
    ws.append(headers)

    for r in rows:
        ws.append([r.get(h) for h in headers])

    wb.save(xlsx_path)


def _apply_filters(rows, filters):
    if not filters:
        return rows

    allowed_types = set(filters.get("action_types") or [])
    allowed_programs = set(filters.get("programs") or [])
    min_budget = filters.get("min_budget_m")

    out = []
    for r in rows:
        if allowed_types and (r.get("action_type") or "").upper() not in allowed_types:
            continue
        if allowed_programs and (r.get("program") or "") not in allowed_programs:
            continue

        if min_budget is not None:
            candidate = r.get("budget_per_project_min_eur_m")
            if candidate is None:
                candidate = r.get("budget_eur_m")
            if candidate is None or candidate < float(min_budget):
                continue

        out.append(r)
    return out


# --- OpenAI helpers (Responses API) ---
def _extract_output_text(resp_json: dict) -> str:
    # Some responses include output_text directly
    txt = (resp_json.get("output_text") or "").strip()
    if txt:
        return txt

    # Otherwise parse output array
    out = resp_json.get("output") or []
    parts = []
    for item in out:
        content = item.get("content") or []
        for c in content:
            # depending on the exact shape, you may see output_text or text
            if c.get("type") == "output_text" and c.get("text"):
                parts.append(c["text"])
            elif c.get("type") == "text" and c.get("text"):
                parts.append(c["text"])
    return "\n".join(p.strip() for p in parts if p and p.strip()).strip()


def _openai_topic_description(topic_id: str, topic_title: str, body_text: str) -> str:
    """
    2-3 frasi in italiano, semplici, usando SOLO il testo del PDF.
    Se insufficiente: "Descrizione non disponibile dal PDF"
    """
    if not OPENAI_API_KEY:
        return ""

    body_text = (body_text or "").strip()
    if len(body_text) < 200:
        return ""

    if len(body_text) > OPENAI_BODY_MAX_CHARS:
        body_text = body_text[:OPENAI_BODY_MAX_CHARS] + "\n[...TRONCATO...]"

    instructions = (
        "Sei un assistente che scrive una descrizione semplice di un topic Horizon Europe.\n"
        "VINCOLI (importantissimo):\n"
        "- Usa SOLO informazioni presenti nel testo fornito (estratto dal PDF).\n"
        "- Non inventare nulla: niente numeri, date, budget o dettagli non presenti.\n"
        "- Se il testo non contiene abbastanza info, rispondi ESATTAMENTE: \"Descrizione non disponibile dal PDF\".\n"
        "- Output: 2-3 frasi in italiano, chiare e concrete, max ~80 parole.\n"
    )

    user_input = (
        f"TOPIC ID: {topic_id}\n"
        f"TITOLO: {topic_title}\n\n"
        "TESTO (estratto dal PDF):\n"
        f"{body_text}"
    )

    payload = {
        "model": OPENAI_MODEL,
        "instructions": instructions,
        "input": user_input,
        "store": False,
    }

    req = urllib.request.Request(
        "https://api.openai.com/v1/responses",
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Authorization": f"Bearer {OPENAI_API_KEY}",
            "Content-Type": "application/json",
        },
        method="POST",
    )

    try:
        with urllib.request.urlopen(req, timeout=20) as r:
            data = json.loads(r.read().decode("utf-8"))
            return _extract_output_text(data)
    except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError):
        return ""


# --- HTML UI ---
HTML = """<!doctype html>
<html lang="it">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Horizon Call Extractor</title>
  <style>
    :root{
      --bg1:#070a16;
      --bg2:#0b1430;
      --card: rgba(255,255,255,.08);
      --card2: rgba(255,255,255,.10);
      --stroke: rgba(255,255,255,.14);
      --text: rgba(255,255,255,.92);
      --muted: rgba(255,255,255,.72);
      --muted2: rgba(255,255,255,.55);

      --accent:#7c3aed;   /* violet */
      --accent2:#06b6d4;  /* cyan */
      --good:#22c55e;
      --warn:#f59e0b;
      --bad:#ef4444;

      --shadow: 0 25px 70px rgba(0,0,0,.55);
      --radius: 18px;
    }

    *{ box-sizing:border-box; }
    html,body{ height:100%; }
    body{
      margin:0;
      font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial, "Apple Color Emoji","Segoe UI Emoji";
      color: var(--text);
      background:
        radial-gradient(1200px 700px at 15% 10%, rgba(124,58,237,.35), transparent 55%),
        radial-gradient(1000px 600px at 90% 25%, rgba(6,182,212,.28), transparent 60%),
        radial-gradient(900px 600px at 45% 95%, rgba(34,197,94,.10), transparent 55%),
        linear-gradient(180deg, var(--bg1), var(--bg2));
      overflow-x:hidden;
    }

    .wrap{
      max-width: 940px;
      margin: 40px auto;
      padding: 0 18px 60px;
    }

    .topbar{
      display:flex;
      align-items:center;
      justify-content:space-between;
      gap:16px;
      margin-bottom: 18px;
    }

    .brand{
      display:flex;
      align-items:center;
      gap:14px;
      min-width: 240px;
    }

    .logo{
      width: 112px;
      height: auto;
      display:block;
      filter: drop-shadow(0 8px 18px rgba(0,0,0,.35));
      border-radius: 10px;
    }

    .brand h1{
      margin:0;
      font-size: 18px;
      letter-spacing:.2px;
    }
    .brand p{
      margin:4px 0 0;
      color: var(--muted);
      font-size: 13px;
      line-height:1.35;
    }

    .pill{
      display:inline-flex;
      align-items:center;
      gap:8px;
      border:1px solid var(--stroke);
      background: rgba(255,255,255,.05);
      padding: 8px 12px;
      border-radius: 999px;
      color: var(--muted);
      font-size: 12px;
      white-space: nowrap;
    }
    .dot{
      width:8px;height:8px;border-radius:999px;
      background: rgba(255,255,255,.35);
      box-shadow: 0 0 0 6px rgba(255,255,255,.06);
    }

    .grid{
      display:grid;
      grid-template-columns: 1.2fr .8fr;
      gap: 18px;
      align-items: start;
    }

    @media (max-width: 860px){
      .grid{ grid-template-columns: 1fr; }
      .brand{ min-width: 0; }
      .logo{ width: 96px; }
    }

    .card{
      background: linear-gradient(180deg, rgba(255,255,255,.10), rgba(255,255,255,.06));
      border: 1px solid var(--stroke);
      border-radius: var(--radius);
      box-shadow: var(--shadow);
      backdrop-filter: blur(10px);
      padding: 18px;
    }

    .card h2{
      margin: 0 0 10px;
      font-size: 16px;
      letter-spacing:.2px;
    }
    .sub{
      margin:0 0 14px;
      color: var(--muted);
      font-size: 13px;
      line-height: 1.45;
    }

    .dropzone{
      position: relative;
      border-radius: 16px;
      border: 1px dashed rgba(255,255,255,.22);
      background: rgba(255,255,255,.05);
      padding: 18px;
      transition: .15s ease;
    }
    .dropzone.drag{
      border-color: rgba(6,182,212,.65);
      box-shadow: 0 0 0 4px rgba(6,182,212,.12);
      transform: translateY(-1px);
    }

    .dzrow{
      display:flex;
      align-items:center;
      justify-content: space-between;
      gap: 12px;
      flex-wrap: wrap;
    }

    .filemeta{
      display:flex;
      flex-direction:column;
      gap: 6px;
      min-width: 220px;
    }

    .filename{
      font-weight: 650;
      font-size: 14px;
      color: var(--text);
    }
    .filesub{
      color: var(--muted2);
      font-size: 12px;
    }

    .actions{
      display:flex;
      align-items:center;
      gap: 10px;
      flex-wrap: wrap;
    }

    .btn{
      appearance:none;
      border:1px solid rgba(255,255,255,.18);
      background: rgba(255,255,255,.06);
      color: var(--text);
      padding: 10px 12px;
      border-radius: 12px;
      cursor:pointer;
      font-weight: 650;
      font-size: 13px;
      letter-spacing:.2px;
      transition: .15s ease;
      display:inline-flex;
      align-items:center;
      gap: 8px;
      user-select:none;
    }
    .btn:hover{ transform: translateY(-1px); border-color: rgba(255,255,255,.28); }
    .btn:active{ transform: translateY(0px); }
    .btn:disabled{ opacity:.55; cursor:not-allowed; transform:none; }

    .btn.primary{
      border: none;
      background: linear-gradient(90deg, var(--accent), var(--accent2));
      box-shadow: 0 10px 30px rgba(124,58,237,.25);
    }
    .btn.primary:hover{
      box-shadow: 0 14px 40px rgba(124,58,237,.28);
    }

    .btn.ghost{
      background: rgba(255,255,255,.05);
    }

    input[type=file]{
      position:absolute;
      inset:0;
      opacity:0;
      pointer-events:none; /* <-- IMPORTANT: non intercetta click */
    }

    .progress{
      margin-top: 12px;
      height: 10px;
      border-radius: 999px;
      background: rgba(255,255,255,.07);
      border: 1px solid rgba(255,255,255,.12);
      overflow: hidden;
    }

    .bar{
      width: 0%;
      height: 100%;
      border-radius: 999px;
      background: linear-gradient(90deg, var(--accent2), var(--accent));
      transition: width .2s ease;
    }

    .bar.indet{
      width: 100%;
      animation: indet 1.2s infinite linear;
      transform-origin:left;
    }
    @keyframes indet{
      0%{ transform: translateX(-100%); }
      100%{ transform: translateX(100%); }
    }

    .stepper{
      margin-top: 14px;
      display:grid;
      grid-template-columns: repeat(4, 1fr);
      gap: 10px;
    }
    .filters{
      margin: 12px 0 18px;
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(240px, 1fr));
      gap: 10px;
    }
    .filterbox{
      border: 1px solid rgba(255,255,255,.12);
      background: rgba(255,255,255,.04);
      border-radius: 14px;
      padding: 12px;
    }
    .filterbox h4{
      margin: 0 0 8px;
      font-size: 13px;
      letter-spacing: .1px;
    }
    .pillrow{
      display:flex;
      flex-wrap: wrap;
      gap: 8px;
    }
    .pillbtn{
      border: 1px solid rgba(255,255,255,.14);
      background: rgba(255,255,255,.05);
      border-radius: 10px;
      padding: 8px 10px;
      color: var(--muted);
      font-size: 12px;
      cursor: pointer;
      transition: .15s ease;
    }
    .pillbtn.active{
      color: rgba(255,255,255,.96);
      border-color: rgba(6,182,212,.45);
      background: rgba(6,182,212,.10);
      box-shadow: 0 10px 25px rgba(6,182,212,.12);
    }
    .sliderrow{
      display:flex;
      align-items:center;
      gap: 10px;
    }
    .sliderrow input[type=range]{
      flex:1;
    }
    .badgeghost{
      display:inline-block;
      padding: 4px 8px;
      border-radius: 8px;
      border:1px solid rgba(255,255,255,.14);
      background: rgba(255,255,255,.05);
      font-size: 11px;
      color: var(--muted);
    }
    .step{
      border: 1px solid rgba(255,255,255,.12);
      background: rgba(255,255,255,.04);
      border-radius: 14px;
      padding: 10px 10px;
      color: var(--muted2);
      font-size: 12px;
      display:flex;
      align-items:center;
      gap: 10px;
      min-height: 44px;
    }
    .badge{
      width: 22px;height:22px;border-radius: 7px;
      display:flex;align-items:center;justify-content:center;
      border: 1px solid rgba(255,255,255,.18);
      background: rgba(255,255,255,.06);
      color: var(--muted);
      font-weight: 750;
      font-size: 12px;
      flex: 0 0 auto;
    }
    .step.active{
      color: var(--text);
      border-color: rgba(6,182,212,.35);
      background: rgba(6,182,212,.08);
    }
    .step.done{
      color: rgba(255,255,255,.86);
      border-color: rgba(34,197,94,.35);
      background: rgba(34,197,94,.08);
    }
    .step.done .badge{
      border-color: rgba(34,197,94,.40);
      background: rgba(34,197,94,.12);
      color: rgba(255,255,255,.92);
    }

    .statusbox{
      margin-top: 14px;
      border-radius: 14px;
      padding: 12px;
      border: 1px solid rgba(255,255,255,.12);
      background: rgba(0,0,0,.18);
    }
    .statusline{
      display:flex;
      align-items:flex-start;
      gap: 10px;
      font-size: 13px;
      color: var(--muted);
      white-space: pre-wrap;
      line-height: 1.45;
    }
    .icon{
      width: 18px;height:18px; flex: 0 0 auto;
      margin-top: 1px;
      opacity: .95;
    }

    .ok{ color: rgba(255,255,255,.92); }
    .err{ color: rgba(255,255,255,.92); }
    .kpi{
      margin-top: 10px;
      display:flex;
      gap: 10px;
      flex-wrap: wrap;
    }
    .chip{
      border: 1px solid rgba(255,255,255,.12);
      background: rgba(255,255,255,.05);
      border-radius: 999px;
      padding: 7px 10px;
      font-size: 12px;
      color: var(--muted);
      display:inline-flex;
      align-items:center;
      gap: 8px;
    }
    .chip strong{ color: var(--text); font-weight: 750; }

    .aside h3{
      margin: 0 0 10px;
      font-size: 14px;
    }
    .aside ul{
      margin: 0;
      padding-left: 18px;
      color: var(--muted);
      font-size: 13px;
      line-height: 1.55;
    }
    .aside li{ margin: 8px 0; }

    .footerhint{
      margin-top: 12px;
      color: var(--muted2);
      font-size: 12px;
    }

    .tiny{
      font-size: 11.5px;
      color: var(--muted2);
    }

    a.link{
      color: rgba(255,255,255,.88);
      text-decoration: none;
      border-bottom: 1px dashed rgba(255,255,255,.35);
    }
    a.link:hover{ opacity:.92; }
  </style>
</head>
<body>
  <div class="wrap">
    <div class="topbar">
      <div class="brand">
        <img class="logo" alt="Adeptic"
          src="data:image/png;base64,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" />
        <div>
          <h1>Horizon Call Extractor</h1>
          <p>Carica un PDF Horizon Europe → estrai call/topic → scarica l’Excel.</p>
        </div>
      </div>

      <div class="pill" id="envpill" title="Endpoint: Lambda Function URL">
        <span class="dot" id="envdot"></span>
        <span>Lambda UI</span>
      </div>
    </div>

    <div class="grid">
      <div class="card">
        <h2>Upload PDF</h2>
        <p class="sub">
          Trascina qui il file oppure clicca per selezionare. Ora supporta bandi Horizon Europe e EDF con filtri per tipo di call.
        </p>

        <div class="filters">
          <div class="filterbox">
            <h4>Programmi</h4>
            <div class="pillrow" id="programFilters"></div>
          </div>
          <div class="filterbox">
            <h4>Action type (in inglese)</h4>
            <div class="pillrow" id="actionFilters"></div>
          </div>
          <div class="filterbox">
            <h4>Min budget per progetto (M€)</h4>
            <div class="sliderrow">
              <input type="range" min="0" max="50" step="0.5" value="0" id="minBudget" />
              <span class="badgeghost" id="minBudgetLabel">All budgets</span>
            </div>
          </div>
        </div>

        <div class="dropzone" id="dz">
          <input id="file" type="file" accept="application/pdf" />
          <div class="dzrow">
            <div class="filemeta">
              <div class="filename" id="filename">Nessun file selezionato</div>
              <div class="filesub" id="filesub">Formato supportato: PDF</div>
            </div>
            <div class="actions">
              <button class="btn ghost" id="clear" disabled>Rimuovi</button>
              <button class="btn primary" id="go" disabled>Genera Excel</button>
            </div>
          </div>

          <div class="progress" aria-label="progress">
            <div class="bar" id="bar"></div>
          </div>

          <div class="stepper" aria-label="steps">
            <div class="step" id="s1"><span class="badge">1</span><span>Presign</span></div>
            <div class="step" id="s2"><span class="badge">2</span><span>Upload S3</span></div>
            <div class="step" id="s3"><span class="badge">3</span><span>Parsing</span></div>
            <div class="step" id="s4"><span class="badge">4</span><span>Download</span></div>
          </div>

          <div class="statusbox">
            <div class="statusline" id="status">
              <svg class="icon" viewBox="0 0 24 24" fill="none" aria-hidden="true">
                <path d="M12 2a10 10 0 1 0 0 20 10 10 0 0 0 0-20Z" stroke="rgba(255,255,255,.55)" stroke-width="1.5"/>
                <path d="M12 8v5" stroke="rgba(255,255,255,.65)" stroke-width="1.5" stroke-linecap="round"/>
                <path d="M12 16h.01" stroke="rgba(255,255,255,.75)" stroke-width="2.5" stroke-linecap="round"/>
              </svg>
              <div>
                <div class="ok" id="statusMain">Seleziona un PDF per iniziare.</div>
                <div class="tiny" id="statusSub">Suggerimento: se il PDF è molto grande, l’elaborazione può richiedere qualche secondo.</div>
              </div>
            </div>

            <div class="kpi" id="kpi" style="display:none;">
              <div class="chip">Righe estratte: <strong id="rows">0</strong></div>
              <div class="chip">Stato: <strong id="final">OK</strong></div>
              <button class="btn primary" id="downloadBtn" style="display:none;">Scarica Excel</button>
            </div>

          </div>
        </div>
      </div>

      <div class="card aside">
        <h3>Cosa otterrai</h3>
        <ul>
          <li>Un file <strong>Excel</strong> con cluster/call/topic estratti.</li>
          <li>Colonne standardizzate: Call ID, Topic ID, titolo, budget, date, ecc.</li>
          <li>Filtri opzionali per programma (Horizon/EDF), action type e budget minimo per progetto.</li>
        </ul>
      </div>
    </div>
  </div>

<script>
const $ = (id) => document.getElementById(id);

const state = {
  file: null,
  downloadUrl: null,
  filters: {
    programs: new Set(["Horizon Europe", "European Defence Fund"]),
    actions: new Set(["RIA","IA","CSA","PCP","PPI","COFUND","RA","DA","SE"]),
    minBudget: 0,
  }
};

const PROGRAM_OPTIONS = [
  { value: "Horizon Europe", label: "Horizon Europe" },
  { value: "European Defence Fund", label: "European Defence Fund" },
];

const ACTION_OPTIONS = [
  { code: "RIA", label: "RIA", program: "Horizon Europe" },
  { code: "IA", label: "IA", program: "Horizon Europe" },
  { code: "CSA", label: "CSA", program: "Horizon Europe" },
  { code: "PCP", label: "PCP", program: "Horizon Europe" },
  { code: "PPI", label: "PPI", program: "Horizon Europe" },
  { code: "COFUND", label: "COFUND", program: "Horizon Europe" },
  { code: "RA", label: "RA", program: "European Defence Fund" },
  { code: "DA", label: "DA", program: "European Defence Fund" },
  { code: "SE", label: "SE", program: "European Defence Fund" },
];

function fmtBytes(bytes){
  const u = ["B","KB","MB","GB"];
  let i = 0;
  let n = bytes;
  while(n >= 1024 && i < u.length-1){ n/=1024; i++; }
  return (i === 0 ? n : n.toFixed(1)) + " " + u[i];
}

function setSteps(activeIdx, doneBefore=false){
  const steps = [$("s1"),$("s2"),$("s3"),$("s4")];
  steps.forEach((el, idx) => {
    el.classList.remove("active","done");
    if(doneBefore && idx < activeIdx) el.classList.add("done");
    if(idx === activeIdx) el.classList.add("active");
  });
}

function clearSteps(){
  [$("s1"),$("s2"),$("s3"),$("s4")].forEach(el => el.classList.remove("active","done"));
}

function setStatus(main, sub, isError=false){
  $("statusMain").textContent = main;
  $("statusSub").textContent = sub || "";
  if(isError){
    $("final").textContent = "ERRORE";
    $("final").style.color = "rgba(255,255,255,.92)";
  }
}

function setBusy(busy){
  $("go").disabled = busy || !state.file;
  $("clear").disabled = busy || !state.file;
  $("file").disabled = busy;
}

function barNone(){
  $("bar").classList.remove("indet");
  $("bar").style.width = "0%";
}
function barIndeterminate(){
  $("bar").style.width = "100%";
  $("bar").classList.add("indet");
}
function barSet(pct){
  $("bar").classList.remove("indet");
  $("bar").style.width = pct + "%";
}

async function fetchJson(url, opts){
  const res = await fetch(url, opts);
  const text = await res.text();
  let data = null;
  try { data = text ? JSON.parse(text) : null; } catch(e) {}
  if(!res.ok){
    const msg = (data && (data.message || data.error)) ? (data.message || data.error) : (text || ("HTTP " + res.status));
    const err = new Error(msg);
    err.status = res.status;
    err.payload = data;
    throw err;
  }
  return data;
}

function showResult(rows, downloadUrl){
  $("kpi").style.display = "flex";
  $("rows").textContent = String(rows ?? 0);
  $("final").textContent = "OK";
  $("downloadBtn").style.display = downloadUrl ? "inline-flex" : "none";
}

function resetResult(){
  $("kpi").style.display = "none";
  $("downloadBtn").style.display = "none";
  state.downloadUrl = null;
}

function setFile(f){
  state.file = f || null;
  resetResult();
  if(!state.file){
    $("filename").textContent = "Nessun file selezionato";
    $("filesub").textContent = "Formato supportato: PDF";
    setStatus("Seleziona un PDF per iniziare.", "Trascina e rilascia oppure clicca per selezionare.");
    clearSteps();
    barNone();
    setBusy(false);
    return;
  }
  $("filename").textContent = state.file.name;
  $("filesub").textContent = fmtBytes(state.file.size) + " • " + (state.file.type || "application/pdf");
  setStatus("Pronto a generare l’Excel.", "Clicca “Genera Excel”.");
  setBusy(false);
}

function renderProgramFilters(){
  const box = $("programFilters");
  box.innerHTML = "";
  PROGRAM_OPTIONS.forEach(opt => {
    const btn = document.createElement("button");
    btn.className = "pillbtn" + (state.filters.programs.has(opt.value) ? " active" : "");
    btn.textContent = opt.label;
    btn.onclick = () => {
      if(state.filters.programs.has(opt.value)){
        state.filters.programs.delete(opt.value);
      } else {
        state.filters.programs.add(opt.value);
      }
      renderProgramFilters();
    };
    box.appendChild(btn);
  });
}

function renderActionFilters(){
  const box = $("actionFilters");
  box.innerHTML = "";
  ACTION_OPTIONS.forEach(opt => {
    const btn = document.createElement("button");
    btn.className = "pillbtn" + (state.filters.actions.has(opt.code) ? " active" : "");
    btn.textContent = `${opt.code} · ${opt.program}`;
    btn.onclick = () => {
      if(state.filters.actions.has(opt.code)){
        state.filters.actions.delete(opt.code);
      } else {
        state.filters.actions.add(opt.code);
      }
      renderActionFilters();
    };
    box.appendChild(btn);
  });
}

function updateBudgetLabel(){
  const slider = $("minBudget");
  const v = parseFloat(slider.value || "0");
  state.filters.minBudget = v;
  if(v <= 0){
    $("minBudgetLabel").textContent = "All budgets";
  } else {
    $("minBudgetLabel").textContent = `≥ ${v.toFixed(1)} M€`;
  }
}

function filtersPayload(){
  const actions = Array.from(state.filters.actions);
  const programs = Array.from(state.filters.programs);
  const minBudget = parseFloat(state.filters.minBudget || 0);

  return {
    action_types: actions.length === ACTION_OPTIONS.length ? [] : actions,
    programs: programs.length === PROGRAM_OPTIONS.length ? [] : programs,
    min_budget_m: minBudget > 0 ? minBudget : null,
  };
}

function isPdfFile(f){
  if(!f) return false;
  // alcuni browser non impostano type: controlliamo anche l'estensione
  const nameOk = (f.name || "").toLowerCase().endsWith(".pdf");
  const typeOk = (f.type || "") === "application/pdf";
  return typeOk || nameOk;
}

// Dropzone UX
const dz = $("dz");
dz.addEventListener("click", (e) => {
  // se clicchi un bottone o un elemento interattivo, NON aprire il picker
  if (e.target.closest("button")) return;
  $("file").click();
});
$("minBudget").addEventListener("input", updateBudgetLabel);
["dragenter","dragover"].forEach(evt => dz.addEventListener(evt, (e) => {
  e.preventDefault(); e.stopPropagation();
  dz.classList.add("drag");
}));
["dragleave","drop"].forEach(evt => dz.addEventListener(evt, (e) => {
  e.preventDefault(); e.stopPropagation();
  dz.classList.remove("drag");
}));
dz.addEventListener("drop", (e) => {
  const f = e.dataTransfer && e.dataTransfer.files && e.dataTransfer.files[0];
  if(!f) return;
  if(!isPdfFile(f)){
    setStatus("Formato non valido.", "Seleziona un file PDF (estensione .pdf).", true);
    return;
  }
  $("file").value = "";
  setFile(f);
});

// File picker
$("file").addEventListener("change", () => {
  const f = $("file").files[0];
  if(!f) return;
  if(!isPdfFile(f)){
    setStatus("Formato non valido.", "Seleziona un file PDF (estensione .pdf).", true);
    $("file").value = "";
    return;
  }
  setFile(f);
});

$("clear").onclick = () => {
  $("file").value = "";
  setFile(null);
};

$("downloadBtn").onclick = () => {
  if(state.downloadUrl) window.location = state.downloadUrl;
};

$("go").onclick = async () => {
  const f = state.file;
  if(!f){
    setStatus("Seleziona un PDF.", "Trascina e rilascia oppure clicca per selezionare.", true);
    return;
  }

  setBusy(true);
  resetResult();
  barIndeterminate();

  try {
    // 1) Presign
    setSteps(0, false);
    setStatus("1/4 • Preparazione upload…", "Richiedo una presigned URL.");
    const pres = await fetchJson("/presign");

    // 2) Upload S3 (fetch PUT: no Content-Type, preserviamo il fix)
    setSteps(1, true);
    setStatus("2/4 • Upload su S3…", "Caricamento del PDF.");
    // (fetch non espone progress nativo senza XHR; usiamo indeterminate)
    const put = await fetch(pres.upload_url, { method: "PUT", body: f });
    if(!put.ok){
      throw new Error("Upload fallito: HTTP " + put.status);
    }

    // 3) Process
    setSteps(2, true);
    setStatus("3/4 • Parsing + generazione Excel…", "Estrazione call/topic e creazione XLSX.");
    const proc = await fetchJson("/process", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ pdf_key: pres.pdf_key, filters: filtersPayload() })
    });

    // 4) Download presigned
    setSteps(3, true);
    setStatus("4/4 • Preparazione download…", "Creo link di download sicuro (temporaneo).");
    const dl = await fetchJson("/download", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ excel_key: proc.excel_key })
    });

    state.downloadUrl = dl.download_url;

    barSet(100);
    [$("s1"),$("s2"),$("s3"),$("s4")].forEach(el => { el.classList.remove("active"); el.classList.add("done"); });

    const rows = (proc && typeof proc.rows === "number") ? proc.rows : null;
    showResult(rows, state.downloadUrl);

    setStatus("Completato ✅", "Download pronto. Se non parte automaticamente, usa il pulsante “Scarica Excel”.");
    // auto-download
    window.location = state.downloadUrl;

  } catch (e) {
    barNone();
    clearSteps();

    const msg = (e && e.message) ? e.message : String(e);
    let sub = "Riprova. Se persiste, controlla CORS S3 / permessi bucket / log Lambda.";
    if(e && e.status){
      sub = "Dettaglio: HTTP " + e.status + ". " + sub;
    }
    setStatus("Errore durante l’elaborazione.", msg + "\\n" + sub, true);
  } finally {
    setBusy(false);
  }
};

// init
renderProgramFilters();
renderActionFilters();
updateBudgetLabel();
setFile(null);
</script>
</body>
</html>
"""


def _resp(status_code: int, body: str, content_type: str = "application/json"):
    return {
        "statusCode": status_code,
        "headers": {
            "content-type": content_type,
            "access-control-allow-origin": "*",
            "access-control-allow-methods": "GET,POST,OPTIONS",
            "access-control-allow-headers": "content-type",
        },
        "body": body,
    }


def _json(obj):
    return json.dumps(obj, ensure_ascii=False)


def handler(event, context):
    try:
        # Supporta invocazioni "dirette" (CLI) e HTTP (Lambda URL)
        if "requestContext" not in event:
            key = event["pdf_key"]
            filters = event.get("filters") if isinstance(event, dict) else None
            return _process_pdf_key(key, filters=filters, context=context)

        method = event.get("requestContext", {}).get("http", {}).get("method", "GET")
        path = event.get("rawPath", "/")

        if method == "OPTIONS":
            return _resp(200, "")

        if method == "GET" and path == "/":
            return _resp(200, HTML, content_type="text/html; charset=utf-8")

        if method == "GET" and path == "/presign":
            _require_bucket()
            pdf_key = f"uploads/{uuid.uuid4()}.pdf"
            upload_url = s3.generate_presigned_url(
                "put_object",
                Params={"Bucket": BUCKET, "Key": pdf_key},
                ExpiresIn=900,
            )
            return _resp(200, _json({"upload_url": upload_url, "pdf_key": pdf_key}))

        if method == "POST" and path == "/process":
            _require_bucket()
            body = event.get("body") or "{}"
            data = json.loads(body)
            pdf_key = data["pdf_key"]
            filters = data.get("filters") if isinstance(data, dict) else None
            result = _process_pdf_key(pdf_key, filters=filters, context=context)
            return _resp(200, _json(result))

        if method == "POST" and path == "/download":
            _require_bucket()
            body = event.get("body") or "{}"
            data = json.loads(body)
            excel_key = data["excel_key"]
            download_url = s3.generate_presigned_url(
                "get_object",
                Params={"Bucket": BUCKET, "Key": excel_key},
                ExpiresIn=900,
            )
            return _resp(200, _json({"download_url": download_url}))

        return _resp(404, _json({"error": "not_found", "path": path, "method": method}))

    except Exception as e:
        # log completo su CloudWatch, ma rispondiamo con messaggio leggibile al browser
        print("ERROR:", repr(e))
        print(traceback.format_exc())

        return _resp(
          500,
          _json({
          "error": "internal",
          "message": str(e),
          "type": e.__class__.__name__,
          "request_id": getattr(context, "aws_request_id", None),
          }),
        )


def _process_pdf_key(key: str, filters=None, context=None):
    _require_bucket()

    local_pdf = f"/tmp/{uuid.uuid4()}.pdf"
    local_xlsx = f"/tmp/{uuid.uuid4()}.xlsx"

    s3.download_file(BUCKET, key, local_pdf)

    text = extract_text(local_pdf)
    rows = parse_calls(text)
    rows = _apply_filters(rows, filters or {})

    # --- OpenAI descriptions (optional) ---
    # Regola anti-timeout: se mancano < 8s, stop OpenAI.
    if OPENAI_API_KEY and context is not None:
        n = 0
        for r in rows:
            if n >= OPENAI_MAX_TOPICS:
                break

            # time budget
            remaining_ms = context.get_remaining_time_in_millis()
            if remaining_ms is not None and remaining_ms < 8000:
                print(f"Stopping OpenAI: low remaining time {remaining_ms}ms")
                break

            body = (r.get("topic_body") or "").strip()
            if not body:
                continue

            cur = (r.get("topic_description") or "").strip()
            if len(cur) >= 80:
                continue

            desc = _openai_topic_description(
                topic_id=r.get("topic_id") or "",
                topic_title=r.get("topic_title") or "",
                body_text=body,
            )
            if desc:
                r["topic_description"] = desc
                n += 1

    # Don't export topic_body to Excel
    for r in rows:
        r.pop("topic_body", None)

    write_xlsx(rows, local_xlsx)

    out_key = key.rsplit(".", 1)[0] + ".xlsx"
    s3.upload_file(local_xlsx, BUCKET, out_key)

    return {"status": "ok", "excel_key": out_key, "rows": len(rows)}
