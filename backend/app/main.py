import json
import re
from contextlib import asynccontextmanager
from pathlib import Path
from uuid import uuid4
import pyarrow as pa
import pyarrow.csv as pacsv
import pyarrow.parquet as pq
from fastapi import FastAPI, Depends, HTTPException, UploadFile, File, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import select
from sqlalchemy.orm import Session
from .config import get_settings
from .db import Base, engine, get_db
from .models import User, Organization, Dataset, AIKey, SavedQuery
from .security import hash_password, check_password, make_token, current_user, cipher
from .schemas import Signup, Login, QueryInput, SaveQuery, AIConfigInput, AskInput
from .analytics import query_data, safe_sql, NAME_RE
from .ai import generate_sql
from .lakehouse import run_lakehouse_query, spark_maintenance
from .models import ScheduledJob, JobRun
from .schemas import EngineQueryInput, ScheduleInput
from datetime import timedelta, datetime, timezone

settings = get_settings()

@asynccontextmanager
async def lifespan(app: FastAPI):
    if settings.secret_key.startswith('CHANGE_ME'):
        raise RuntimeError('SECRET_KEY must be configured')
    Path(settings.data_dir).mkdir(parents=True, exist_ok=True)
    Base.metadata.create_all(bind=engine)  # Initial bootstrap. For schema changes, introduce Alembic migrations.
    yield

app = FastAPI(title='OpenLake AI API', version='0.2.1', lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=[settings.frontend_origin], allow_credentials=True,
                   allow_methods=['GET','POST','DELETE'], allow_headers=['Content-Type','X-Requested-With'])

@app.middleware('http')
async def browser_origin_guard(request: Request, call_next):
    if request.method in {'POST','PUT','PATCH','DELETE'} and request.url.path.startswith('/api/'):
        origin = request.headers.get('origin')
        if origin and origin != settings.frontend_origin:
            return Response('Forbidden origin', status_code=403)
        # Browsers sending a session cookie must include this non-simple header for CSRF protection.
        if request.cookies.get('openlake_session') and request.headers.get('x-requested-with') != 'openlake-web':
            return Response('Missing CSRF header', status_code=403)
    return await call_next(request)

def info(user: User):
    return {'id':user.id,'email':user.email,'organization_id':user.organization_id}

def session_cookie(response: Response, value: str):
    response.set_cookie('openlake_session', value, httponly=True, secure=settings.cookie_secure,
                        samesite='strict', max_age=12*3600, path='/')

@app.get('/api/health')
def health():
    return {'status':'ok','service':'openlake-api'}

@app.post('/api/auth/signup', status_code=201)
def signup(body: Signup, response: Response, db: Session=Depends(get_db)):
    if db.scalar(select(User).where(User.email == body.email.lower())):
        raise HTTPException(409,'Account already exists')
    org = Organization(name=body.organization.strip())
    db.add(org)
    db.flush()
    user = User(email=body.email.lower(), password_hash=hash_password(body.password), organization_id=org.id)
    db.add(user)
    db.commit()
    session_cookie(response, make_token(user))
    return info(user)

@app.post('/api/auth/login')
def login(body: Login, response: Response, db: Session=Depends(get_db)):
    user = db.scalar(select(User).where(User.email == body.email.lower()))
    if not user or not check_password(user.password_hash, body.password):
        raise HTTPException(401,'Invalid email or password')
    session_cookie(response, make_token(user))
    return info(user)

@app.post('/api/auth/logout')
def logout(response: Response):
    response.delete_cookie('openlake_session', path='/')
    return {'ok':True}

@app.get('/api/auth/me')
def me(user: User=Depends(current_user)):
    return info(user)

@app.get('/api/overview')
def overview(user: User=Depends(current_user), db: Session=Depends(get_db)):
    data = db.scalars(select(Dataset).where(Dataset.organization_id == user.organization_id)).all()
    saved = db.scalars(select(SavedQuery).where(SavedQuery.organization_id == user.organization_id)).all()
    return {'datasets':len(data),'rows':sum(d.rows for d in data),'saved_queries':len(saved), 'ai_ready':bool(db.scalar(select(AIKey.id).where(AIKey.organization_id == user.organization_id)))}

@app.get('/api/datasets')
def list_datasets(user: User=Depends(current_user), db: Session=Depends(get_db)):
    data = db.scalars(select(Dataset).where(Dataset.organization_id == user.organization_id).order_by(Dataset.created_at.desc())).all()
    return [{'id':d.id,'name':d.name,'filename':d.filename,'rows':d.rows,'columns':json.loads(d.columns_json),'created_at':d.created_at.isoformat()} for d in data]

@app.post('/api/datasets', status_code=201)
async def upload_dataset(name: str, file: UploadFile=File(...), user: User=Depends(current_user), db: Session=Depends(get_db)):
    name = name.strip().lower()
    if not NAME_RE.fullmatch(name):
        raise HTTPException(422,'Dataset name must start with a letter, using lowercase letters, digits and underscores')
    if db.scalar(select(Dataset.id).where(Dataset.organization_id == user.organization_id, Dataset.name == name)):
        raise HTTPException(409,'Dataset name already exists')
    suffix = Path(file.filename or '').suffix.lower()
    if suffix not in {'.csv','.parquet'}:
        raise HTTPException(415,'Only CSV and Parquet files are supported')
    content = await file.read(settings.max_upload_mb*1024*1024+1)
    await file.close()
    if len(content) > settings.max_upload_mb*1024*1024:
        raise HTTPException(413,'File exceeds upload limit')
    try:
        if suffix == '.csv':
            table = pacsv.read_csv(pa.BufferReader(content))
        else:
            table = pq.read_table(pa.BufferReader(content))
        if not table.column_names or not table.num_rows:
            raise ValueError('Dataset is empty')
        if len(set(table.column_names)) != len(table.column_names):
            raise ValueError('Duplicate column names')
    except (pa.ArrowException, ValueError, OSError) as exc:
        raise HTTPException(422, f'Cannot parse dataset: {str(exc)[:180]}') from None
    folder = Path(settings.data_dir)/user.organization_id
    folder.mkdir(parents=True, exist_ok=True)
    path = folder/f'{uuid4().hex}.parquet'
    pq.write_table(table,path,compression='zstd')
    d = Dataset(organization_id=user.organization_id,name=name, filename=Path(file.filename or 'upload').name[:250],
                storage_path=str(path),rows=table.num_rows,columns_json=json.dumps([{'name':f.name,'type':str(f.type)} for f in table.schema]))
    db.add(d)
    db.commit()
    return {'id':d.id,'name':name,'rows':d.rows,'columns':json.loads(d.columns_json)}

@app.delete('/api/datasets/{dataset_id}')
def delete_dataset(dataset_id: str, user: User=Depends(current_user), db: Session=Depends(get_db)):
    d = db.scalar(select(Dataset).where(Dataset.id==dataset_id, Dataset.organization_id==user.organization_id))
    if not d:
        raise HTTPException(404,'Dataset not found')
    path=d.storage_path
    db.delete(d)
    db.commit()
    Path(path).unlink(missing_ok=True)
    return {'ok':True}

@app.post('/api/query')
def run_query(body: EngineQueryInput, user: User=Depends(current_user), db: Session=Depends(get_db)):
    return run_lakehouse_query(db,user.organization_id,body.sql,body.engine,body.use_cache)

@app.get('/api/queries')
def get_queries(user: User=Depends(current_user),db: Session=Depends(get_db)):
    rows=db.scalars(select(SavedQuery).where(SavedQuery.organization_id==user.organization_id).order_by(SavedQuery.created_at.desc())).all()
    return [{'id':q.id,'name':q.name,'sql':q.sql} for q in rows]

@app.post('/api/queries',status_code=201)
def save_query(body: SaveQuery,user: User=Depends(current_user),db: Session=Depends(get_db)):
    names={d.name for d in db.scalars(select(Dataset).where(Dataset.organization_id==user.organization_id))}
    safe_sql(body.sql,names)
    q=SavedQuery(organization_id=user.organization_id,name=body.name,sql=body.sql)
    db.add(q);db.commit()
    return {'id':q.id,'name':q.name,'sql':q.sql}

@app.get('/api/ai/config')
def ai_config(user: User=Depends(current_user),db: Session=Depends(get_db)):
    rows=db.scalars(select(AIKey).where(AIKey.organization_id==user.organization_id)).all()
    return [{'provider':r.provider,'model':r.model,'configured':True} for r in rows]

@app.post('/api/ai/config')
def configure_ai(body: AIConfigInput,user: User=Depends(current_user),db: Session=Depends(get_db)):
    if body.provider not in {'openai','anthropic'}:
        raise HTTPException(422,'Unsupported provider')
    existing=db.scalar(select(AIKey).where(AIKey.organization_id==user.organization_id,AIKey.provider==body.provider))
    if existing:
        existing.encrypted_key=cipher().encrypt(body.api_key.encode()).decode()
        existing.model=body.model
    else:
        db.add(AIKey(organization_id=user.organization_id,provider=body.provider,
                     encrypted_key=cipher().encrypt(body.api_key.encode()).decode(),model=body.model))
    db.commit()
    return {'provider':body.provider,'model':body.model,'configured':True}

@app.delete('/api/ai/config/{provider}')
def remove_ai(provider: str,user: User=Depends(current_user),db: Session=Depends(get_db)):
    existing=db.scalar(select(AIKey).where(AIKey.organization_id==user.organization_id, AIKey.provider==provider))
    if not existing:
        raise HTTPException(404,'Provider not found')
    db.delete(existing);db.commit()
    return {'ok':True}

@app.post('/api/ai/ask')
async def ask_ai(body: AskInput,user: User=Depends(current_user),db: Session=Depends(get_db)):
    key=db.scalar(select(AIKey).where(AIKey.organization_id==user.organization_id,AIKey.provider==body.provider))
    if not key:
        raise HTTPException(404,'Configure this AI provider first')
    datasets=db.scalars(select(Dataset).where(Dataset.organization_id==user.organization_id)).all()
    if not datasets:
        raise HTTPException(422,'Upload a dataset first')
    schema='\n'.join(f'{d.name}({", ".join(f["name"]+" "+f["type"] for f in json.loads(d.columns_json))})' for d in datasets)
    generated=await generate_sql(key.provider,cipher().decrypt(key.encrypted_key.encode()).decode(),key.model,
                                 body.prompt,schema,settings.ai_max_output_tokens)
    generated=re.sub(r'^```(?:sql)?\s*|\s*```$', '',generated.strip(),flags=re.I).strip()
    names={d.name for d in datasets}
    safe_sql(generated,names)
    # User must review and explicitly run the SQL. No autonomous execution.
    return {'sql':generated,'provider':key.provider,'model':key.model}


@app.get('/api/engines')
def engines(user: User=Depends(current_user)):
    return {'duckdb':True, 'spark':bool(settings.spark_url), 'iceberg':bool(settings.spark_url),
            'auto_threshold_bytes':settings.spark_min_bytes, 'cache_enabled':bool(settings.redis_url)}

@app.post('/api/datasets/{dataset_id}/iceberg/{action}')
def manage_iceberg(dataset_id: str, action: str, user: User=Depends(current_user), db: Session=Depends(get_db)):
    d=db.scalar(select(Dataset).where(Dataset.id==dataset_id, Dataset.organization_id==user.organization_id))
    if not d:
        raise HTTPException(404,'Dataset not found')
    return spark_maintenance(user.organization_id,d,action)

@app.get('/api/schedules')
def schedules(user: User=Depends(current_user), db: Session=Depends(get_db)):
    jobs=db.scalars(select(ScheduledJob).where(ScheduledJob.organization_id==user.organization_id)).all()
    return [{'id':j.id,'name':j.name,'sql':j.sql,'engine':j.engine,
             'interval_minutes':j.interval_minutes, 'enabled':j.enabled,
             'next_run':j.next_run.isoformat(),'last_status':j.last_status} for j in jobs]

@app.post('/api/schedules', status_code=201)
def create_schedule(body: ScheduleInput, user: User=Depends(current_user),db: Session=Depends(get_db)):
    datasets=db.scalars(select(Dataset).where(Dataset.organization_id==user.organization_id)).all()
    safe_sql(body.sql,{d.name for d in datasets})
    j=ScheduledJob(organization_id=user.organization_id,name=body.name,sql=body.sql,engine=body.engine,
                   interval_minutes=body.interval_minutes, next_run=datetime.now(timezone.utc)+timedelta(minutes=body.interval_minutes))
    db.add(j);db.commit()
    return {'id':j.id,'name':j.name,'next_run':j.next_run.isoformat()}

@app.delete('/api/schedules/{job_id}')
def delete_schedule(job_id: str,user: User=Depends(current_user),db: Session=Depends(get_db)):
    j=db.scalar(select(ScheduledJob).where(ScheduledJob.id==job_id,ScheduledJob.organization_id==user.organization_id))
    if not j:
        raise HTTPException(404,'Schedule not found')
    db.delete(j);db.commit()
    return {'ok':True}

@app.get('/api/schedules/{job_id}/runs')
def schedule_runs(job_id: str,user: User=Depends(current_user),db: Session=Depends(get_db)):
    j=db.scalar(select(ScheduledJob).where(ScheduledJob.id==job_id,ScheduledJob.organization_id==user.organization_id))
    if not j:
        raise HTTPException(404,'Schedule not found')
    runs=db.scalars(select(JobRun).where(JobRun.job_id==job_id,JobRun.organization_id==user.organization_id)
                    .order_by(JobRun.created_at.desc()).limit(50)).all()
    return [{'id':r.id,'status':r.status,'engine':r.engine,'rows':r.row_count,'error':r.error,
             'created_at':r.created_at.isoformat()} for r in runs]
