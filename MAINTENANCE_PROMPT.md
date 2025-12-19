# GPT Maintenance Prompt – Horizon Call Extractor

Usa QUESTO prompt all’inizio di ogni nuova chat con ChatGPT.

---

## CONTEXT
Sto lavorando su una web app serverless AWS per estrarre call Horizon Europe da PDF e generare Excel.

Repository (public):
https://github.com/antoniocarluccireply/horizon-call-extractor

## ARCHITETTURA ATTUALE (NON CAMBIARE SE NON RICHIESTO)
- AWS Lambda (Python 3.13)
- Lambda Function URL (AuthType: NONE)
- UI HTML servita dalla Lambda (GET /)
- API:
  - GET /presign
  - POST /process
  - POST /download
- Upload PDF via browser con **presigned S3 PUT**
- S3 bucket: horizon-extractor-antoniocarlucci
- Regione: eu-central-1
- Presigned URL:
  - SENZA ContentType
  - Endpoint S3 regionale (`s3.eu-central-1.amazonaws.com`)
  - Fix applicato per evitare redirect 307
- CORS abilitato sul bucket S3
- Parsing PDF con pypdf
- Excel generato con openpyxl

## REGOLE FONDAMENTALI
- ❌ NON inventare dati
- ❌ NON cambiare architettura senza esplicita richiesta
- ❌ NON rimuovere workaround già risolti
- ✅ Estrarre solo informazioni realmente presenti nei PDF Horizon
- ✅ Proporre modifiche **incrementali**
- ✅ Motivare ogni modifica tecnica

## STATO ATTUALE
- ✅ Upload PDF da browser funzionante
- ✅ Parsing base Horizon
- ✅ Excel scaricabile
- 🔧 UI migliorabile
- 🔧 Parsing Excel migliorabile (Call ID, budget, scadenze, struttura decision-ready)

## OBIETTIVO DELLA SESSIONE
[SCRIVI QUI COSA VUOI FARE ORA: UI / parsing / Excel / altro]

## FILE DI RIFERIMENTO
Di seguito incollerò:
1. aws_lambda/lambda_function.py
2. aws_lambda/parser_horizon.py

Analizza questi file e proponi modifiche coerenti con quanto sopra.
