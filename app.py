import os
import uuid
import mimetypes
from datetime import datetime

from nicegui import ui, app
import boto3
from botocore.exceptions import ClientError
import httpx
from fastapi import Request
from botocore.config import Config
import psycopg
from uuid import uuid4
from typing import Optional, List
from urllib.parse import quote
import json

# =========================
# Config
# =========================
S3_BUCKET = os.getenv('S3_BUCKET', 'hackathon-doc-search-bucket')
AWS_REGION = os.getenv('AWS_REGION', 'ap-southeast-5')

# =========================
# Database Config
# =========================
PGHOST = "database-1.cdguu8qi48bg.ap-southeast-5.rds.amazonaws.com"
PGPORT = 5432
PGUSER = "postgres"
PGPASSWORD = ">6bzBcyB~3*gPE_GpBOl771Bc[nN"
PGDATABASE = "postgres"

def get_conn():
    return psycopg.connect(
        host=PGHOST,
        port=PGPORT,
        user=PGUSER,
        password=PGPASSWORD,
        dbname=PGDATABASE,
        sslmode="require",
    )

# With this:
session = boto3.Session(profile_name='martin')

def get_bucket_region(bucket: str) -> str:
    # S3 returns None for us-east-1
    s3 = session.client('s3')
    loc = s3.get_bucket_location(Bucket=bucket)['LocationConstraint']
    return loc or 'us-east-1'

def s3_client_for_bucket(bucket: str):
    region = get_bucket_region(bucket)
    return session.client(
        's3',
        region_name=region,
        config=Config(signature_version='s3v4', s3={'addressing_style': 'virtual'}),
    )

# Use this client everywhere you presign or call S3 for that bucket:
s3 = s3_client_for_bucket(S3_BUCKET)

file_storage = {}


SQS_QUEUE_URL = 'https://sqs.ap-southeast-5.amazonaws.com/239974788694/doc-ingest-queue'  # <— paste YOUR URL
sqs = session.client('sqs', region_name='ap-southeast-5')

# =========================
# Helpers
# =========================
def get_base_url():
    # This gets your current host and port
    return 'http://localhost:8080'  # Simple version

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

# =========================
# Backend API
# =========================
@app.post('/api/upload/init')
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
        doc_id = str(uuid4())
        with get_conn() as conn, conn.cursor() as cur:
            cur.execute("""
                INSERT INTO documents (id, s3_key, title, mime_type)
                VALUES (%s, %s, %s, %s)
            """, (doc_id, key, title, mime_type))

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


@app.get('/api/search')
async def api_search(request: Request):
    """Server-side list with optional substring filter on filename."""
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

@app.delete('/api/file')
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
    
@app.get('/api/search/db')
async def api_search_db(request: Request):
    """
    Lightweight search over the documents table.
    Filters:
      q            -> substring / fuzzy over title and s3_key (case-insensitive)
      date_from    -> ISO 'YYYY-MM-DD' (uploaded_at >=)
      date_to      -> ISO 'YYYY-MM-DD' (uploaded_at < next day)
      mime         -> exact mime_type match (optional)
      dept         -> department equals (optional)
      max_sens     -> sensitivity <= value (default 2)
      limit, offset
    """
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

    # fuzzy / partial match on title or s3_key
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
        # make it exclusive end (date boundary)
        where.append("uploaded_at < (%s::date + INTERVAL '1 day')")
        args.append(date_to)

    sql = f"""
    SELECT id, s3_key, title, mime_type, uploaded_at, owner_email, department, sensitivity, tags
    FROM documents
    WHERE {" AND ".join(where)}
    ORDER BY uploaded_at DESC
    LIMIT %s OFFSET %s
    """
    args.extend([limit, offset])

    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, args)
        rows = cur.fetchall()

        # total count for pagination
        cur.execute(f"SELECT COUNT(*) FROM documents WHERE {' AND '.join(where)}", args[:-2])
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

@app.get('/api/presign_get')
async def api_presign_get(request: Request):
    try:
        key = request.query_params.get('key')
        mode = (request.query_params.get('mode') or 'open').lower()  # 'open' or 'download'
        if not key:
            return {'error': 'Missing key'}, 400

        # Look up a nice filename + mime from DB
        filename = os.path.basename(key)
        mime = None
        try:
            with get_conn() as conn, conn.cursor() as cur:
                cur.execute("SELECT title, mime_type FROM documents WHERE s3_key = %s LIMIT 1", (key,))
                row = cur.fetchone()
                if row:
                    title, mime = row
                    if title: 
                        filename = title
        except Exception:
            pass  # fallback to basename if DB lookup fails

        # Build safe Content-Disposition (RFC 5987 for UTF-8)
        disp_type = "inline" if mode == "open" else "attachment"
        filename_ascii = filename.replace('"', '')  # basic sanitize
        filename_utf8 = quote(filename)             # e.g. Screenshot%202023-09-27%20131147.png
        content_disp = f'{disp_type}; filename="{filename_ascii}"; filename*=UTF-8\'\'{filename_utf8}'

        params = {
            'Bucket': S3_BUCKET,
            'Key': key,
            'ResponseContentDisposition': content_disp,
        }
        if mime:
            params['ResponseContentType'] = mime  # helps browsers render inline

        url = s3.generate_presigned_url('get_object', Params=params, ExpiresIn=3600)
        return {'url': url}
    except ClientError as e:
        return {'error': f'S3 error: {e}'}, 500
    except Exception as e:
        return {'error': f'Server error: {e}'}, 500

# =========================
# Frontend
# =========================
@ui.page('/')
def main():

    ui.label('Doc Uploader').classes('text-h4 text-center mb-4')

    with ui.column().classes('w-full max-w-3xl mx-auto gap-6'):

        # ---------- Upload Card ----------
        with ui.card().classes('w-full'):
            ui.label('Upload').classes('text-h6')
            status = ui.label('Select a PDF or any file (≤10MB)').classes('text-gray-600')

            btn_upload = ui.button('Upload to S3').classes('w-full')
            btn_upload.disable()

            def on_file_selected(event):
                print(f"DEBUG: File selected: {event.name}")
                
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
                print("DEBUG: do_upload function called")
                
                print(f"DEBUG: Checking file_storage keys: {list(file_storage.keys())}")
                
                if 'selected_file' not in file_storage:
                    print("DEBUG: No selected_file found!")
                    ui.notify('No file selected', type='warning')
                    btn_upload.disable()
                    return
                
                print("DEBUG: File found in storage, proceeding...")
                
                try:
                    print("DEBUG: Entered try block")
                    btn_upload.disable()
                    
                    file_name = file_storage['file_name']
                    file_content = file_storage['selected_file']
                    print(f"DEBUG: Got file - name: {file_name}, size: {len(file_content)}")

                    mime, _ = mimetypes.guess_type(file_name)
                    if not mime:
                        mime = 'application/octet-stream'
                    print(f'DEBUG: Uploading {file_name} as {mime}')

                    print("DEBUG: About to create httpx client")
                    async with httpx.AsyncClient() as client:
                        print("DEBUG: httpx client created, making POST request")
                        
                        r = await client.post(f'{get_base_url()}/api/upload/init', json={'file_name': file_name})
                        print(f"DEBUG: POST response status: {r.status_code}")
                        print(f"DEBUG: POST response text: {r.text}")
                        
                        r.raise_for_status()
                        data = r.json()
                        
                        print(f"DEBUG: Init response: {data}")
                        upload_url = data.get('upload_url')
                        print(f"DEBUG: Upload URL: {upload_url}")
                        
                        if not upload_url:
                            raise Exception("No upload_url in response")

                        print("DEBUG: About to PUT file to S3")
                        put_resp = await client.put(upload_url, content=file_content)
                        print(f"DEBUG: PUT response status: {put_resp.status_code}")
                        put_resp.raise_for_status()

                    print("DEBUG: Upload successful!")
                    status.text = f'✅ Uploaded to s3://{S3_BUCKET}/{data["key"]}'
                    status.classes('text-green-600')

                    file_storage.pop('selected_file', None)
                    file_storage.pop('file_name', None)

                    await run_db_search()

                except Exception as e:
                    print(f"DEBUG: Exception caught: {e}")
                    print(f"DEBUG: Exception type: {type(e)}")
                    import traceback
                    traceback.print_exc()
                    
                    status.text = f'❌ Upload failed: {e}'
                    status.classes('text-red-600')
                finally:
                    print("DEBUG: Finally block - re-enabling button")
                    btn_upload.disable()  # Note: you probably want enable() here if upload fails

            btn_upload.on('click', do_upload)  # async handler is supported

        # ---------- DB Search (by filename/title) ----------
        with ui.card().classes('w-full'):
            ui.label('DB Search (title / key)').classes('text-h6')

            with ui.row().classes('items-center gap-3'):
                q_input = ui.input(placeholder='e.g. "screenshot" or ".pdf"').classes('w-64')
                date_from_in = ui.input(label='From (YYYY-MM-DD)').classes('w-40')
                date_to_in   = ui.input(label='To (YYYY-MM-DD)').classes('w-40')
                btn_db_search = ui.button('Search')

            db_table = ui.column().classes('w-full')

            # ---- local helpers so we don't rely on outer scope ----
            async def presign_and_open(key: str):
                async with httpx.AsyncClient() as client:
                    r = await client.get(f'{get_base_url()}/api/presign_get', params={'key': key, 'mode': 'open'})
                    r.raise_for_status()
                    url = r.json()['url']
                ui.run_javascript(f'window.open("{url}", "_blank");')

            async def presign_and_download(key: str):
                async with httpx.AsyncClient() as client:
                    r = await client.get(f'{get_base_url()}/api/presign_get', params={'key': key, 'mode': 'download'})
                    r.raise_for_status()
                    url = r.json()['url']
                ui.run_javascript(f'window.open("{url}", "_blank");')

            def make_async(handler, key):
                async def _h(_=None):
                    await handler(key)
                return _h
            # -------------------------------------------------------

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
                                    ui.button('Download', on_click=make_async(presign_and_download, it['s3_key']))

                except Exception as e:
                    db_table.clear()
                    with db_table:
                        ui.label(f'❌ DB search error: {e}').classes('text-red-600')

            btn_db_search.on('click', run_db_search)


        # # ---------- Browser / Search Card ----------
        # with ui.card().classes('w-full'):
        #     ui.label('Files').classes('text-h6')

        #     with ui.row().classes('items-center gap-3'):
        #         prefix_input = ui.input(label='Prefix', value='uploads/').classes('w-64')
        #         search_input = ui.input(placeholder='Search filename... (filters current page)')
        #         btn_refresh = ui.button('Refresh')

        #     table_container = ui.column().classes('w-full')
        #     state = {'next_token': None, 'rows': []}

        #     async def load_page(reset: bool = True):
        #         try:
        #             params = {'prefix': prefix_input.value or 'uploads/', 'limit': 200}
        #             if not reset and state['next_token']:
        #                 params['token'] = state['next_token']

        #             async with httpx.AsyncClient() as client:
        #                 r = await client.get('/api/search', params=params)
        #                 r.raise_for_status()
        #                 payload = r.json()

        #             state['rows'] = payload.get('items', []) if reset else state['rows'] + payload.get('items', [])
        #             state['next_token'] = payload.get('next_token')
        #             draw_table()

        #         except Exception as e:
        #             table_container.clear()
        #             with table_container:
        #                 ui.label(f'❌ List error: {e}').classes('text-red-600')

        #     def draw_table():
        #         table_container.clear()
        #         needle = (search_input.value or '').lower().strip()
        #         rows = state['rows']
        #         if needle:
        #             rows = [r for r in rows if needle in r['filename'].lower()]

        #         if not rows:
        #             with table_container:
        #                 ui.label('No files found for the current page.').classes('text-gray-600')
        #             return

        #         with table_container:
        #             with ui.row().classes('font-medium text-gray-700'):
        #                 ui.label('File').classes('w-2/5')
        #                 ui.label('Size').classes('w-1/5')
        #                 ui.label('Last Modified').classes('w-1/5')
        #                 ui.label('Actions').classes('w-1/5')

        #             # helper to build async click handlers with captured key
        #             def make_async(handler, key):
        #                 async def _h(_=None):
        #                     await handler(key)
        #                 return _h

        #             for r in rows:
        #                 with ui.row().classes('items-center py-1 border-b border-gray-100'):
        #                     ui.label(r['filename']).classes('w-2/5 truncate')
        #                     ui.label(human_size(r['size'])).classes('w-1/5')
        #                     lm = r['last_modified'].replace('T', ' ').split('.')[0]
        #                     ui.label(lm).classes('w-1/5')

        #                     with ui.row().classes('w-1/5 gap-2'):
        #                         ui.button('Link', on_click=make_async(copy_link, r['key']))
        #                         ui.button('Download', on_click=make_async(open_link, r['key']))
        #                         ui.button('Delete', on_click=make_async(delete_key, r['key'])).props('color=negative flat')

        #         with table_container:
        #             with ui.row().classes('justify-between items-center mt-2'):
        #                 ui.label('Page size: 200').classes('text-gray-500')
        #                 if state['next_token']:
        #                     async def load_more(_=None):
        #                         await load_page(reset=False)
        #                     ui.button('Load more', on_click=load_more)

        #     async def refresh_list(_=None):
        #         await load_page(reset=True)

        #     async def copy_link(key: str):
        #         try:
        #             async with httpx.AsyncClient() as client:
        #                 r = await client.get('/api/presign_get', params={'key': key})
        #                 r.raise_for_status()
        #                 url = r.json()['url']
        #             ui.run_javascript(f'navigator.clipboard.writeText("{url}")')
        #             ui.notify('Presigned URL copied', type='positive')
        #         except Exception as e:
        #             ui.notify(f'Failed to copy link: {e}', type='negative')

        #     async def open_link(key: str):
        #         try:
        #             async with httpx.AsyncClient() as client:
        #                 r = await client.get('/api/presign_get', params={'key': key})
        #                 r.raise_for_status()
        #                 url = r.json()['url']
        #             ui.open(url)
        #         except Exception as e:
        #             ui.notify(f'Download failed: {e}', type='negative')

        #     async def delete_key(key: str):
        #         try:
        #             async with httpx.AsyncClient() as client:
        #                 r = await client.delete('/api/file', json={'key': key})
        #                 r.raise_for_status()
        #             ui.notify('Deleted', type='positive')
        #             await refresh_list()
        #         except Exception as e:
        #             ui.notify(f'Delete failed: {e}', type='negative')

        #     # wire up events (async handlers supported)
        #     btn_refresh.on('click', refresh_list)
        #     search_input.on('input', draw_table)
        #     prefix_input.on('change', refresh_list)

        #     # auto-load on page ready (one-shot)
        #     ui.timer(0.01, refresh_list, once=True)  # run once after render

if __name__ == "__main__":
    # local dev only
    ui.run(host="0.0.0.0", port=8080, storage_secret="super-secret-key")
