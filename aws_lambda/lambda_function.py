import os
import uuid
import json
import boto3
from pypdf import PdfReader
from openpyxl import Workbook

from parser_horizon import parse_calls

s3 = boto3.client("s3", region_name="eu-central-1", endpoint_url="https://s3.eu-central-1.amazonaws.com")
BUCKET = os.environ["BUCKET_NAME"]

def extract_text(pdf_path: str) -> str:
    reader = PdfReader(pdf_path)
    return "\n".join([(p.extract_text() or "") for p in reader.pages])

def write_xlsx(rows, xlsx_path: str):
    wb = Workbook()
    ws = wb.active
    ws.title = "calls"

    headers = [
        "cluster","call_id","topic_id","topic_title","action_type",
        "trl","budget_eur_m","opening_date","deadline_date"
    ]
    ws.append(headers)

    for r in rows:
        ws.append([r.get(h) for h in headers])

    wb.save(xlsx_path)

HTML = """<!doctype html>
<html lang="it">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Horizon Call Extractor</title>
  <style>
    body { font-family: -apple-system,BlinkMacSystemFont,Segoe UI,Roboto,Helvetica,Arial,sans-serif; margin: 0; background:#f6f7fb; }
    .wrap { max-width: 720px; margin: 48px auto; padding: 0 16px; }
    .card { background: #fff; border-radius: 16px; padding: 20px; box-shadow: 0 10px 30px rgba(0,0,0,.06); }
    h1 { font-size: 20px; margin: 0 0 8px; }
    p { margin: 0 0 16px; color:#555; }
    input[type=file] { width: 100%; }
    button { margin-top: 12px; padding: 10px 14px; border: 0; border-radius: 10px; cursor: pointer; }
    button:disabled { opacity: .6; cursor: not-allowed; }
    .status { margin-top: 12px; font-size: 13px; color:#333; white-space: pre-wrap; }
    .hint { font-size: 12px; color:#777; margin-top: 10px; }
  </style>
</head>
<body>
  <div class="wrap">
    <div class="card">
      <h1>Horizon Call Extractor</h1>
      <p>Carica un PDF Horizon. L’app genera un Excel scaricabile con le call/topic estratte.</p>

      <input id="file" type="file" accept="application/pdf" />
      <button id="go">Carica e genera Excel</button>
      <div class="status" id="status"></div>
      <div class="hint">Nota: nessun dato viene “inventato”; l’Excel contiene solo ciò che viene trovato nel PDF.</div>
    </div>
  </div>

<script>
const $ = (id) => document.getElementById(id);
const status = (msg) => { $("status").textContent = msg; };

$("go").onclick = async () => {
  const f = $("file").files[0];
  if (!f) { status("Seleziona un PDF."); return; }

  $("go").disabled = true;
  try {
    status("1/4: preparo upload…");
    const pres = await fetch("/presign").then(r => r.json());

    status("2/4: upload su S3…");
    const put = await fetch(pres.upload_url, {
      method: "PUT",
      headers: { "Content-Type": "application/pdf" },
      body: f
    });
    if (!put.ok) throw new Error("Upload fallito: " + put.status);

    status("3/4: parsing + generazione Excel…");
    const proc = await fetch("/process", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ pdf_key: pres.pdf_key })
    }).then(r => r.json());

    status("4/4: preparo download…");
    const dl = await fetch("/download", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ excel_key: proc.excel_key })
    }).then(r => r.json());

    status("Fatto. Download in corso…");
    window.location = dl.download_url;
  } catch (e) {
    status("Errore: " + (e && e.message ? e.message : e));
  } finally {
    $("go").disabled = false;
  }
};
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
    # Supporta sia invocazioni "dirette" (CLI) sia HTTP (Lambda URL)
    if "requestContext" not in event:
        # modalità old: {"pdf_key":"uploads/x.pdf"}
        key = event["pdf_key"]
        return _process_pdf_key(key)

    method = event.get("requestContext", {}).get("http", {}).get("method", "GET")
    path = event.get("rawPath", "/")

    if method == "OPTIONS":
        return _resp(200, "")

    if method == "GET" and path == "/":
        return _resp(200, HTML, content_type="text/html; charset=utf-8")

    if method == "GET" and path == "/presign":
        pdf_key = f"uploads/{uuid.uuid4()}.pdf"
        upload_url = s3.generate_presigned_url(
            "put_object",
            Params={"Bucket": BUCKET, "Key": pdf_key},
            ExpiresIn=900,
        )
        return _resp(200, _json({"upload_url": upload_url, "pdf_key": pdf_key}))

    if method == "POST" and path == "/process":
        body = event.get("body") or "{}"
        data = json.loads(body)
        pdf_key = data["pdf_key"]
        result = _process_pdf_key(pdf_key)
        # _process_pdf_key ritorna dict "normale" in modalità non-HTTP, qui trasformiamo
        if isinstance(result, dict) and "statusCode" not in result:
            return _resp(200, _json(result))
        return result

    if method == "POST" and path == "/download":
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

def _process_pdf_key(key: str):
    local_pdf = f"/tmp/{uuid.uuid4()}.pdf"
    local_xlsx = f"/tmp/{uuid.uuid4()}.xlsx"

    s3.download_file(BUCKET, key, local_pdf)

    text = extract_text(local_pdf)
    rows = parse_calls(text)

    write_xlsx(rows, local_xlsx)

    out_key = key.rsplit(".", 1)[0] + ".xlsx"
    s3.upload_file(local_xlsx, BUCKET, out_key)

    return {"status": "ok", "excel_key": out_key, "rows": len(rows)}
