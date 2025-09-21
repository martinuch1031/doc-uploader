# app_rag_nova.py
# One-file NiceGUI app with:
# - Upload to S3 (presigned PUT) -> row in documents -> SQS message
# - Local SQS poller (simulates Lambda) that: downloads file, extracts text, chunks, embeds (Titan v2 in us-east-1), inserts rows in public.doc_chunks
# - RAG chat endpoints using Amazon Nova Pro (us-east-1) + pgvector retrieval
#
# Requirements (pip):
#   nicegui boto3 botocore httpx fastapi pypdf psycopg
#
# IMPORTANT:
# - Uses local AWS profile "martin" for credentials (no env vars).
# - S3 in ap-southeast-5 (your original), Bedrock models in us-east-1 as requested.
# - PostgreSQL connection details are hardcoded (from your snippet).
#
# Run:
#   python app_rag_nova.py
#
# Then open http://localhost:8080

import os
import io
import json
import uuid
import time
import mimetypes
from datetime import datetime
from typing import List, Dict, Any, Optional
from urllib.parse import quote
import psycopg2
import psycopg2.extras

import httpx
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
from pypdf import PdfReader

import psycopg
from nicegui import ui, app as nice_app
from fastapi import Request, Body, HTTPException

# =========================
# CONFIG (all hardcoded)
# =========================
# Your existing values
S3_BUCKET = 'hackathon-doc-search-bucket'
AWS_REGION_S3 = 'ap-southeast-5'            # where your bucket lives
AWS_PROFILE = 'martin'                      # local profile
SQS_QUEUE_URL = 'https://sqs.ap-southeast-5.amazonaws.com/239974788694/doc-ingest-queue'

# Bedrock config (as requested)
BEDROCK_REGION = 'us-east-1'
BEDROCK_EMBED_MODEL_ID = 'amazon.titan-embed-text-v2:0'
BEDROCK_CHAT_MODEL_ID  = 'amazon.nova-pro-v1:0'

# PostgreSQL (from your snippet)
PGHOST = "database-1.cdguu8qi48bg.ap-southeast-5.rds.amazonaws.com"
PGPORT = 5432
PGUSER = "postgres"
PGPASSWORD = ">6bzBcyB~3*gPE_GpBOl771Bc[nN"
PGDATABASE = "postgres"

# =========================
# AWS clients (single Session)
# =========================
session = boto3.Session(profile_name=AWS_PROFILE)

def get_bucket_region(bucket: str) -> str:
    s3_probe = session.client('s3')
    loc = s3_probe.get_bucket_location(Bucket=bucket)['LocationConstraint']
    return loc or 'us-east-1'

def s3_client_for_bucket(bucket: str):
    region = get_bucket_region(bucket)
    return session.client(
        's3',
        region_name=region,
        config=Config(signature_version='s3v4', s3={'addressing_style': 'virtual'}),
    )

s3 = s3_client_for_bucket(S3_BUCKET)
sqs = session.client('sqs', region_name=AWS_REGION_S3)
bedrock_rt = session.client('bedrock-runtime', region_name=BEDROCK_REGION)

# =========================
# DB helpers
# =========================
def get_conn():
    return psycopg.connect(
        host=PGHOST,
        port=PGPORT,
        user=PGUSER,
        password=PGPASSWORD,
        dbname=PGDATABASE,
        sslmode="require",
    )

# =========================
# Utils
# =========================
def get_base_url():
    return 'http://localhost:8080'

def human_size(n: int) -> str:
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if n < 1024.0:
            return f"{n:.0f} {unit}"
        n /= 1024.0
    return f"{n:.1f} PB"

def safe_ext(name: str) -> str:
    return os.path.splitext(name)[1] or ""

def new_file_key(original: str) -> str:
    today = datetime.utcnow().strftime('%Y/%m/%d')
    return f"uploads/{today}/{uuid.uuid4()}{safe_ext(original)}"

def chunk_text(txt: str, chunk_chars=2000, overlap=200) -> List[str]:
    n = len(txt)
    if n == 0:
        return []
    out, i = [], 0
    while i < n:
        j = min(n, i + chunk_chars)
        c = txt[i:j].strip()
        if c:
            out.append(c)
        i = max(0, j - overlap)
    return out

def extract_text_from_pdf_bytes(b: bytes) -> str:
    reader = PdfReader(io.BytesIO(b))
    parts = []
    for p in reader.pages:
        t = p.extract_text() or ""
        parts.append(t)
    return "\n".join(parts)

# =========================
# Bedrock helpers (Titan embeddings + Nova Pro chat)
# =========================
def titan_embed(text: str):
    # Titan takes a single string, not an array
    body = {"inputText": text}
    resp = bedrock_rt.invoke_model(
        modelId="amazon.titan-embed-text-v1",   # <-- 1536 dims
        body=json.dumps(body),
        contentType="application/json",
        accept="application/json",
    )
    payload = json.loads(resp["body"].read())
    return payload["embedding"]  # single vector of floats

PG_CONN_STR = f"host={PGHOST} port={PGPORT} dbname={PGDATABASE} user={PGUSER} password={PGPASSWORD}"

def run_query(sql: str, params=None):
    conn = psycopg2.connect(PG_CONN_STR)
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, params)
            if cur.description:  # SELECT
                return cur.fetchall()
            else:
                conn.commit()
                return []
    finally:
        conn.close()

def semantic_search(query: str, top_k: int = 3):
    """Embed query and search doc_chunks for nearest neighbors."""
    # 1. Generate embedding with Titan
    q_emb = titan_embed(query)  # -> list[float] (length 1024 if v2)

    # 2. Convert to Postgres vector string
    q_emb_str = "[" + ",".join(f"{x:.6f}" for x in q_emb) + "]"

    # 3. Query Postgres
    rows = run_query("""
        SELECT d.id, d.title, d.s3_key, c.content,
               1 - (c.embedding <=> %s::vector) AS score
        FROM doc_chunks c
        JOIN documents d ON d.id = c.document_id
        ORDER BY c.embedding <=> %s::vector
        LIMIT %s;
    """, (q_emb_str, q_emb_str, top_k))

    return rows

def to_pgvector(v):
    # pgvector expects a string literal: [0.1,0.2,...]
    return "[" + ",".join(f"{x:.6f}" for x in v) + "]"

def nova_pro_answer(system_text: Optional[str], user_text: str, max_tokens=800, temperature=0.2) -> str:
    """
    Nova Pro via Bedrock "messages" style payload.
    """
    body = {
        "messages": [
            {"role": "user", "content": [{"text": user_text}]}
        ],
        "inferenceConfig": {
            "maxTokens": max_tokens,
            "temperature": temperature
        }
    }
    if system_text:
        body["system"] = [{"text": system_text}]

    resp = bedrock_rt.invoke_model(
        modelId=BEDROCK_CHAT_MODEL_ID,
        body=json.dumps(body),
        contentType="application/json",
        accept="application/json",
    )
    data = json.loads(resp["body"].read())
    # response shape: { "output": { "message": { "content": [ {"text": "..."} ] } }, ... }
    try:
        contents = data["output"]["message"]["content"]
        parts = [c.get("text", "") for c in contents if "text" in c]
        return "".join(parts).strip()
    except Exception:
        # Fallback to alternative shapes if any
        return json.dumps(data)

# =========================
# Local "ingestor" (poll SQS and process) — simulates Lambda
# =========================
_INGESTOR_RUNNING = {"flag": False}

def process_sqs_message(body: Dict[str, Any]) -> None:
    """
    Body schema from /api/upload/init:
    {
        "document_id": "...",
        "bucket": "...",
        "s3_key": "...",
        "title": "...",
        "mime_type": "..."
    }
    """
    doc_id = body["document_id"]
    key = body["s3_key"]
    mime = body.get("mime_type") or ""

    # 1) Download object
    obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
    blob = obj["Body"].read()

    # 2) Extract text
    if mime.startswith("application/pdf") or key.lower().endswith(".pdf"):
        text = extract_text_from_pdf_bytes(blob)
    else:
        try:
            text = blob.decode("utf-8", errors="ignore")
        except Exception:
            text = ""

    if not text.strip():
        return

    # 3) Chunk + embed
    chunks = chunk_text(text)
    embeddings: List[List[float]] = []
    B = 16
    for i in range(0, len(chunks), B):
        embeddings.extend(titan_embed(chunks[i:i+B]))

    # 4) Insert into public.doc_chunks
    with get_conn() as conn, conn.cursor() as cur:
        rows = [
            (str(uuid.uuid4()), doc_id, idx, chunks[idx], embeddings[idx], None)
            for idx in range(len(chunks))
        ]
        cur.executemany(
            """
            INSERT INTO public.doc_chunks
                (id, document_id, chunk_index, content, embedding, token_count)
            VALUES (%s,%s,%s,%s,%s,%s)
            """,
            rows,
        )

def poll_sqs_once(max_messages=5, wait_seconds=2):
    resp = sqs.receive_message(
        QueueUrl=SQS_QUEUE_URL,
        MaxNumberOfMessages=max_messages,
        WaitTimeSeconds=wait_seconds
    )
    messages = resp.get('Messages', [])
    for m in messages:
        receipt = m['ReceiptHandle']
        try:
            body = json.loads(m['Body'])
            process_sqs_message(body)
            # delete after success
            sqs.delete_message(QueueUrl=SQS_QUEUE_URL, ReceiptHandle=receipt)
        except Exception as ex:
            print(f"[SQS] error processing: {ex}")

def start_ingestor_loop():
    if _INGESTOR_RUNNING["flag"]:
        return
    _INGESTOR_RUNNING["flag"] = True
    # Use a NiceGUI timer as a cheap scheduler (every 3s)
    def _tick():
        try:
            poll_sqs_once()
        except Exception as e:
            print(f"[SQS] poll error: {e}")
    ui.timer(3.0, _tick)

# =========================
# Backend API
# =========================
file_storage: Dict[str, Any] = {}

@nice_app.post('/api/upload/init')
async def api_upload_init(request: Request):
    try:
        data = await request.json()
        file_name = data.get('file_name') or 'unknown.bin'
        mime_type = data.get('mime_type') or 'application/octet-stream'
        title = data.get('title') or file_name

        key = new_file_key(file_name)
        url = s3.generate_presigned_url(
            'put_object',
            Params={'Bucket': S3_BUCKET, 'Key': key},
            ExpiresIn=3600,
        )

        # Insert into documents table
        doc_id = str(uuid.uuid4())
        with get_conn() as conn, conn.cursor() as cur:
            cur.execute("""
                INSERT INTO public.documents (id, s3_key, title, mime_type)
                VALUES (%s, %s, %s, %s)
            """, (doc_id, key, title, mime_type))

        # Send SQS message for ingestion
        try:
            payload = {
                "document_id": doc_id,
                "bucket": S3_BUCKET,
                "s3_key": key,
                "title": title,
                "mime_type": mime_type
            }
            resp = sqs.send_message(
                QueueUrl=SQS_QUEUE_URL,
                MessageBody=json.dumps(payload)
            )
            print(f"[SQS] sent message_id={resp.get('MessageId')} for doc_id={doc_id}")
        except Exception as ex:
            print(f"[SQS] send failed: {ex}")

        return {
            'upload_url': url,
            'key': key,
            'bucket': S3_BUCKET,
            'document_id': doc_id,
        }
    except ClientError as e:
        return {'error': f'S3 error: {e}'}, 500
    except Exception as e:
        return {'error': f'Server error: {e}'}, 500

@nice_app.get('/api/search')
async def api_search(request: Request):
    try:
        q = (request.query_params.get('q') or '').lower().strip()
        max_keys = int(request.query_params.get('limit', 200))
        prefix = request.query_params.get('prefix', 'uploads/')
        token = request.query_params.get('token')

        list_kwargs = {'Bucket': S3_BUCKET, 'Prefix': prefix, 'MaxKeys': max_keys}
        if token:
            list_kwargs['ContinuationToken'] = token

        resp = s3.list_objects_v2(**list_kwargs)
        contents = resp.get('Contents', [])
        out = []
        for obj in contents:
            key = obj['Key']
            filename = os.path.basename(key)
            if not q or q in filename.lower():
                out.append({
                    'key': key,
                    'filename': filename,
                    'size': obj['Size'],
                    'last_modified': obj['LastModified'].isoformat(),
                })
        return {'items': out, 'next_token': resp.get('NextContinuationToken')}
    except ClientError as e:
        return {'error': f'S3 error: {e}'}, 500
    except Exception as e:
        return {'error': f'Server error: {e}'}, 500

@nice_app.delete('/api/file')
async def api_delete(request: Request):
    try:
        data = await request.json()
        key = data.get('key')
        if not key:
            return {'error': 'Missing key'}, 400
        s3.delete_object(Bucket=S3_BUCKET, Key=key)
        return {'deleted': True}
    except ClientError as e:
        return {'error': f'S3 error: {e}'}, 500
    except Exception as e:
        return {'error': f'Server error: {e}'}, 500

@nice_app.get('/api/search/db')
async def api_search_db(request: Request):
    q = (request.query_params.get('q') or '').strip()
    date_from = request.query_params.get('date_from')
    date_to = request.query_params.get('date_to')
    mime = request.query_params.get('mime')
    dept = request.query_params.get('dept')
    try:
        max_sens = int(request.query_params.get('max_sens', 2))
    except:
        max_sens = 2
    try:
        limit = min(int(request.query_params.get('limit', 50)), 200)
        offset = int(request.query_params.get('offset', 0))
    except:
        limit, offset = 50, 0

    where = ["sensitivity <= %s"]
    args: List[object] = [max_sens]

    if q:
        where.append("(title ILIKE %s OR s3_key ILIKE %s)")
        like = f"%{q}%"
        args.extend([like, like])

    if mime:
        where.append("mime_type = %s")
        args.append(mime)

    if dept:
        where.append("department = %s")
        args.append(dept)

    if date_from:
        where.append("uploaded_at >= %s")
        args.append(date_from)

    if date_to:
        where.append("uploaded_at < (%s::date + INTERVAL '1 day')")
        args.append(date_to)

    sql = f"""
    SELECT id, s3_key, title, mime_type, uploaded_at, owner_email, department, sensitivity, tags
    FROM public.documents
    WHERE {" AND ".join(where)}
    ORDER BY uploaded_at DESC
    LIMIT %s OFFSET %s
    """
    args.extend([limit, offset])

    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, args)
        rows = cur.fetchall()
        cur.execute(f"SELECT COUNT(*) FROM public.documents WHERE {' AND '.join(where)}", args[:-2])
        total = cur.fetchone()[0]

    items = []
    for (id_, s3_key, title, mime_type, uploaded_at, owner_email, department, sensitivity, tags) in rows:
        items.append({
            "id": str(id_),
            "s3_key": s3_key,
            "title": title,
            "mime_type": mime_type,
            "uploaded_at": uploaded_at.isoformat(),
            "owner_email": owner_email,
            "department": department,
            "sensitivity": sensitivity,
            "tags": tags,
        })

    return {"items": items, "total": total, "limit": limit, "offset": offset}

@nice_app.get('/api/presign_get')
async def api_presign_get(request: Request):
    try:
        key = request.query_params.get('key')
        mode = (request.query_params.get('mode') or 'open').lower()  # 'open' or 'download'
        if not key:
            return {'error': 'Missing key'}, 400

        # Look up nice filename + mime from DB
        filename = os.path.basename(key)
        mime = None
        try:
            with get_conn() as conn, conn.cursor() as cur:
                cur.execute("SELECT title, mime_type FROM public.documents WHERE s3_key = %s LIMIT 1", (key,))
                row = cur.fetchone()
                if row:
                    title, mime = row
                    if title:
                        filename = title
        except Exception:
            pass

        # Content-Disposition
        disp_type = "inline" if mode == "open" else "attachment"
        filename_ascii = filename.replace('"', '')
        filename_utf8 = quote(filename)
        content_disp = f'{disp_type}; filename="{filename_ascii}"; filename*=UTF-8\'\'{filename_utf8}'

        params = {
            'Bucket': S3_BUCKET,
            'Key': key,
            'ResponseContentDisposition': content_disp,
        }
        if mime:
            params['ResponseContentType'] = mime

        url = s3.generate_presigned_url('get_object', Params=params, ExpiresIn=3600)
        return {'url': url}
    except ClientError as e:
        return {'error': f'S3 error: {e}'}, 500
    except Exception as e:
        return {'error': f'Server error: {e}'}, 500

# =========================
# CHAT API (sessions, messages, RAG with Nova Pro)
# =========================
@nice_app.post('/api/chat/start')
async def api_chat_start(payload: dict = Body(...)):
    user_email = (payload.get("user_email") or "anonymous@local").strip()
    sid = str(uuid.uuid4())
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            "INSERT INTO public.chat_sessions (id, user_email) VALUES (%s,%s)",
            (sid, user_email)
        )
        cur.execute("SELECT started_at, title FROM public.chat_sessions WHERE id=%s", (sid,))
        row = cur.fetchone()
    return {"session_id": sid, "started_at": row[0].isoformat(), "title": row[1]}

@nice_app.post('/api/chat/{session_id}/send')
async def api_chat_send(session_id: str, request: Request):
    body = await request.json()
    print("DEBUG body:", body)

    # Try all common keys, fallback to first string
    question = (
        body.get("message")
        or body.get("content")
        or body.get("text")
        or next((v for v in body.values() if isinstance(v, str)), None)
    )

    if not question:
        raise HTTPException(status_code=400, detail=f"Invalid request body: {body}")

    # 1. Always run semantic search
    search_rows = semantic_search(question, top_k=3)
    context_text = "\n\n".join([r["content"] for r in search_rows]) or "No relevant context found."

    # 2. Build messages for Nova
    messages = [
        {
            "role": "user",
            "content": [
                {"text": "You are a helpful assistant. Use the provided context to answer user queries about files. "
                        "If context is irrelevant, say you cannot find relevant info."}
            ]
        },
        {
            "role": "user",
            "content": [
                {"text": f"User query: {question}"}
            ]
        },
        {
            "role": "user",
            "content": [
                {"text": f"Database search results:\n{context_text}"}
            ]
        }
    ]

    # 3. Call Nova Pro
    resp = bedrock_rt.invoke_model(
        modelId="amazon.nova-pro-v1:0",
        body=json.dumps({
            "messages": messages,
            "inferenceConfig": {"maxTokens": 300}
        })
    )

    answer = json.loads(resp["body"].read())

    # 4. Store chat message in DB
    run_query(
        "INSERT INTO chat_messages (id, session_id, role, content) VALUES (gen_random_uuid(), %s, %s, %s)",
        (session_id, "user", question)
    )
    run_query(
        "INSERT INTO chat_messages (id, session_id, role, content) VALUES (gen_random_uuid(), %s, %s, %s)",
        (session_id, "assistant", answer["output"]["message"]["content"][0]["text"])
    )

    return {"answer": answer["output"]["message"]["content"][0]["text"]}

# =========================
# Frontend (NiceGUI)
# =========================
@ui.page('/')
def main():
    ui.label('Doc Uploader + RAG Chat (Nova Pro)').classes('text-h4 text-center mb-4')

    # Start local SQS ingestor loop (simulate Lambda)
    start_ingestor_loop()

    with ui.column().classes('w-full max-w-3xl mx-auto gap-6'):

        # ---------- Upload Card ----------
        with ui.card().classes('w-full'):
            ui.label('Upload').classes('text-h6')
            status = ui.label('Select a PDF or any file (≤10MB)').classes('text-gray-600')

            btn_upload = ui.button('Upload to S3').classes('w-full')
            btn_upload.disable()

            def on_file_selected(event):
                if not event or not event.content:
                    status.text = 'No file selected'
                    btn_upload.disable()
                    return
                try:
                    file_content = event.content.read()
                    file_storage['selected_file'] = file_content
                    file_storage['file_name'] = event.name
                    status.text = f'Selected: {event.name} ({human_size(len(file_content))})'
                    btn_upload.enable()
                    ui.notify(f"File ready: {event.name}", type='positive')
                except Exception as e:
                    status.text = f'Error: {e}'
                    btn_upload.disable()

            ui.upload(
                on_upload=on_file_selected,
                max_file_size=10 * 1024 * 1024,
                auto_upload=True,
            ).classes('w-full')

            async def do_upload(_=None):
                if 'selected_file' not in file_storage:
                    ui.notify('No file selected', type='warning')
                    btn_upload.disable()
                    return
                try:
                    btn_upload.disable()
                    file_name = file_storage['file_name']
                    file_content = file_storage['selected_file']
                    mime, _ = mimetypes.guess_type(file_name)
                    if not mime:
                        mime = 'application/octet-stream'

                    async with httpx.AsyncClient() as client:
                        r = await client.post(f'{get_base_url()}/api/upload/init', json={'file_name': file_name, 'mime_type': mime, 'title': file_name})
                        r.raise_for_status()
                        data = r.json()

                        upload_url = data.get('upload_url')
                        if not upload_url:
                            raise Exception("No upload_url in response")

                        put_resp = await client.put(upload_url, content=file_content)
                        put_resp.raise_for_status()

                    status.text = f'✅ Uploaded to s3://{S3_BUCKET}/{data["key"]}'
                    status.classes('text-green-600')

                    file_storage.pop('selected_file', None)
                    file_storage.pop('file_name', None)

                    await run_db_search()
                except Exception as e:
                    status.text = f'❌ Upload failed: {e}'
                    status.classes('text-red-600')
                finally:
                    btn_upload.enable()

            btn_upload.on('click', do_upload)

        # ---------- DB Search ----------
        with ui.card().classes('w-full'):
            ui.label('DB Search (title / key)').classes('text-h6')

            with ui.row().classes('items-center gap-3'):
                q_input = ui.input(placeholder='e.g. "manual" or ".pdf"').classes('w-64')
                date_from_in = ui.input(label='From (YYYY-MM-DD)').classes('w-40')
                date_to_in   = ui.input(label='To (YYYY-MM-DD)').classes('w-40')
                btn_db_search = ui.button('Search')

            db_table = ui.column().classes('w-full')

            async def presign_and_open(key: str):
                async with httpx.AsyncClient() as client:
                    r = await client.get(f'{get_base_url()}/api/presign_get', params={'key': key, 'mode': 'open'})
                    r.raise_for_status()
                    url = r.json()['url']
                ui.run_javascript(f'window.open("{url}", "_blank");')

            def make_async(handler, key):
                async def _h(_=None):
                    await handler(key)
                return _h

            async def run_db_search(_=None):
                try:
                    params = {
                        "q": q_input.value or "",
                        "date_from": date_from_in.value or "",
                        "date_to": date_to_in.value or "",
                        "limit": 50,
                        "max_sens": 2,
                    }
                    params = {k: v for k, v in params.items() if v}

                    async with httpx.AsyncClient() as client:
                        r = await client.get(f'{get_base_url()}/api/search/db', params=params)
                        r.raise_for_status()
                        payload = r.json()

                    db_table.clear()
                    items = payload.get("items", [])
                    if not items:
                        with db_table:
                            ui.label('No matches.').classes('text-gray-600')
                        return

                    with db_table:
                        with ui.row().classes('font-medium text-gray-700'):
                            ui.label('Title').classes('w-2/5')
                            ui.label('MIME').classes('w-1/5')
                            ui.label('Uploaded').classes('w-1/5')
                            ui.label('Actions').classes('w-1/5')

                        for it in items:
                            with ui.row().classes('items-center py-1 border-b border-gray-100'):
                                ui.label(it['title'] or os.path.basename(it['s3_key'])).classes('w-2/5 truncate')
                                ui.label(it['mime_type'] or '-').classes('w-1/5')
                                ui.label(it['uploaded_at'].replace('T', ' ').split('.')[0]).classes('w-1/5')
                                with ui.row().classes('w-1/5 gap-2'):
                                    ui.button('Open', on_click=make_async(presign_and_open, it['s3_key']))

                except Exception as e:
                    db_table.clear()
                    with db_table:
                        ui.label(f'❌ DB search error: {e}').classes('text-red-600')

            btn_db_search.on('click', run_db_search)

        # ---------- Chatbot Card ----------
        with ui.card().classes('w-full'):
            ui.label('Ask your internal documents (Nova Pro, us-east-1)').classes('text-h6')
            ta = ui.textarea(placeholder='Type a question…').props('rows=3').classes('w-full')
            btn_start = ui.button('Start Chat')
            btn_send = ui.button('Send').classes('ml-2')
            btn_send.disable()
            out = ui.column().classes('w-full mt-2')
            state = {"session_id": None}

            async def start_chat(_=None):
                try:
                    async with httpx.AsyncClient() as client:
                        r = await client.post(f'{get_base_url()}/api/chat/start', json={"user_email": "tech@factory.local"})
                        r.raise_for_status()
                        payload = r.json()
                        state["session_id"] = payload["session_id"]
                    out.clear()
                    with out: ui.label(f'✅ Session started: {state["session_id"]}').classes('text-green-600')
                    btn_send.enable()
                except Exception as e:
                    out.clear()
                    with out: ui.label(f'❌ Start error: {e}').classes('text-red-600')

            async def send_msg(_=None):
                try:
                    q = (ta.value or "").strip()
                    if not q:
                        ui.notify('Question is empty', type='warning'); return
                    if not state["session_id"]:
                        ui.notify('Start a chat first', type='warning'); return

                    with out:
                        ui.label(f'You: {q}').classes('font-medium')
                    ta.value = ""

                    async with httpx.AsyncClient() as client:
                        r = await client.post(
                            f'{get_base_url()}/api/chat/{state["session_id"]}/send',
                            json={"q": q, "k": 6}
                        )
                        r.raise_for_status()
                        payload = r.json()

                    with out:
                        ui.markdown(payload["answer"])
                        if payload.get("sources"):
                            ui.label('Sources:').classes('mt-1 text-gray-600')
                            for s in payload["sources"]:
                                row = ui.row().classes('items-center gap-2')
                                ui.label(f"• {s['title']}  (rel ~ {s['score']:.3f})")
                                async def _open(key=s["s3_key"]):
                                    async with httpx.AsyncClient() as client:
                                        rr = await client.get(f'{get_base_url()}/api/presign_get', params={'key': key, 'mode': 'open'})
                                        rr.raise_for_status()
                                        url = rr.json()['url']
                                    ui.run_javascript(f'window.open("{url}", "_blank");')
                                ui.button('Open', on_click=_open).props('flat size=sm')
                except Exception as e:
                    import traceback
                    traceback.print_exc()     # prints full stack in your console
                    return {"error": f"{type(e).__name__}: {e}"}, 500

            btn_start.on('click', start_chat)
            btn_send.on('click', send_msg)

# =========================
# Run
# =========================
if __name__ in {'__main__', '__mp_main__'}:
    ui.run(host='0.0.0.0', port=8080, storage_secret='super-secret-key')
