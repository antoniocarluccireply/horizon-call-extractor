import os
import uuid
import json
import boto3
from pypdf import PdfReader
from openpyxl import Workbook

from parser_horizon import parse_calls

s3 = boto3.client(
    "s3",
    region_name="eu-central-1",
    endpoint_url="https://s3.eu-central-1.amazonaws.com",
)
BUCKET = os.environ["BUCKET_NAME"]


def extract_text(pdf_path: str) -> str:
    reader = PdfReader(pdf_path)
    return "\n".join([(p.extract_text() or "") for p in reader.pages])


def write_xlsx(rows, xlsx_path: str):
    wb = Workbook()
    ws = wb.active
    ws.title = "calls"

    headers = [
        "cluster",
        "call_id",
        "topic_id",
        "topic_title",
        "action_type",
        "trl",
        "budget_eur_m",
        "opening_date",
        "deadline_date",
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

    .header { display: flex; align-items: center; gap: 14px; margin-bottom: 10px; }
    .logo { width: 120px; height: auto; display: block; }
    .title-wrap h1 { margin: 0 0 6px; }
    .title-wrap p { margin: 0 0 16px; color:#555; }

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

      <div class="header">
        <img class="logo" alt="Adeptic"
          src="data:image/png;base64,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" />
        <div class="title-wrap">
          <h1>Horizon Call Extractor</h1>
          <p>Carica un PDF Horizon. L’app genera un Excel scaricabile con le call/topic estratte.</p>
        </div>
      </div>

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
