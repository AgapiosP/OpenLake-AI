"""Durable interval scheduler using PostgreSQL advisory lock and job history."""
import logging
import time
from datetime import datetime, timedelta, timezone
from sqlalchemy import select, text
from .db import SessionLocal, Base, engine
from .models import ScheduledJob, JobRun, Dataset, IcebergMaintenanceState
from .config import get_settings
from .lakehouse import spark_maintenance
from pathlib import Path
from .lakehouse import run_lakehouse_query
log = logging.getLogger('scheduler')
logging.basicConfig(level=logging.INFO)

def tick():
    with SessionLocal() as db:
        # One scheduler instance acquires the same advisory lock per tick.
        if not db.scalar(text('SELECT pg_try_advisory_xact_lock(42734921)')):
            return
        now = datetime.now(timezone.utc)
        jobs = db.scalars(select(ScheduledJob).where(ScheduledJob.enabled.is_(True),
                   ScheduledJob.next_run <= now).order_by(ScheduledJob.next_run).limit(20)).all()
        for job in jobs:
            job.next_run = now + timedelta(minutes=job.interval_minutes)
            job.last_run = now
            run = JobRun(job_id=job.id, organization_id=job.organization_id,
                         status='running', engine=job.engine)
            db.add(run)
            db.flush()
            try:
                result = run_lakehouse_query(db, job.organization_id, job.sql, job.engine)
                run.status = 'success'
                run.engine = result['engine']
                run.row_count = result['count']
                job.last_status = 'success'
            except Exception as exc:
                run.status = 'failed'
                run.error = type(exc).__name__ + ': ' + str(exc)[:350]
                job.last_status = 'failed'
                log.exception('Scheduled job failed %s', job.id)
            db.commit()
        settings = get_settings()
        if settings.iceberg_auto_maintenance and settings.spark_url:
            sources = db.scalars(select(Dataset).order_by(Dataset.created_at).limit(500)).all()
            for dataset in sources:
                state = None
                try:
                    path = Path(dataset.storage_path)
                    if not path.is_file():
                        continue
                    info = path.stat()
                    fp = f'{info.st_size}:{info.st_mtime_ns}'
                    state = db.get(IcebergMaintenanceState, dataset.id)
                    if state is None:
                        state = IcebergMaintenanceState(dataset_id=dataset.id,
                                organization_id=dataset.organization_id)
                        db.add(state)
                    eligible = state.last_success is None or (now - state.last_success).total_seconds() >= settings.iceberg_maintenance_interval_minutes * 60
                    if not eligible:
                        continue
                    if state.source_fingerprint != fp:
                        spark_maintenance(dataset.organization_id, dataset, 'sync')
                        state.source_fingerprint = fp
                    spark_maintenance(dataset.organization_id, dataset, 'compact')
                    state.last_success = now
                    state.last_status = 'success'
                    db.commit()
                except Exception as exc:
                    if state is not None:
                        state.last_status = type(exc).__name__[:400]
                        db.commit()
                    log.exception('Iceberg maintenance failed %s', dataset.id)

if __name__ == '__main__':
    Base.metadata.create_all(engine)
    while True:
        try:
            tick()
        except Exception:
            log.exception('Scheduler tick failed')
        time.sleep(15)
