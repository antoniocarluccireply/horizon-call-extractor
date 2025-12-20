# Horizon Call Extractor

Web app serverless per estrarre call Horizon Europe da PDF e generare Excel.

## Architettura
- AWS Lambda (Python 3.13)
- Lambda Function URL (AuthType: NONE)
- Upload PDF via browser tramite presigned S3 PUT
- Parsing PDF → Excel → download automatico
- Regione: eu-central-1
- Costi: AWS Free Tier

## Componenti principali
- `aws_lambda/lambda_function.py`
  - Serve UI HTML (GET /)
  - API:
    - GET /presign
    - POST /process
    - POST /download
- `aws_lambda/parser_horizon.py`
  - Parsing dei PDF Horizon Europe
  - Estrae **solo informazioni presenti nel documento**
  - Nessuna informazione inventata

## Decisioni tecniche chiave
- Presigned S3 PUT **senza ContentType**
- S3 client con endpoint regionale:
  https://s3.eu-central-1.amazonaws.com
  (evita redirect 307 che rompe PUT da browser)
- CORS abilitato su bucket S3
- Excel generato con openpyxl (no pandas)

## Bucket S3
- Nome: horizon-extractor-antoniocarlucci
- Path input/output: `uploads/`

## Stato attuale
- ✅ Upload PDF da browser
- ✅ Parsing Horizon ed EDF con rilevamento automatico del tipo documento
- ✅ Excel scaricabile solo su click esplicito
- ✅ UI mobile-friendly con tabelle per Horizon e EDF

## Manual test checklist
- Horizon (singolo PDF): upload, rilevamento Horizon, tabella e download Excel.
- Horizon (multi 2–6 PDF): merge risultati in una tabella/Excel, link Topic ID attivi.
- EDF (singolo PDF): rilevamento EDF, tabella EDF, download Excel senza link Topic ID.
- Errore mismatch: caricare PDF EDF su tab Horizon (o viceversa) mostra errore chiaro.
- Mobile: niente auto-download, dopo il parsing scroll automatico alla tabella risultati, nomi file avvolti senza overflow.
