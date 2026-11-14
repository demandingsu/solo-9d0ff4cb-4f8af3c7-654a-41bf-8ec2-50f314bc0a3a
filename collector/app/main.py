"""Collector API: trigger/resume sync jobs and query the paper library.

Endpoints
---------
POST   /sync/jobs              create a sync job (full or incremental)
GET    /sync/jobs              list jobs
GET    /sync/jobs/{id}         job status incl. durable checkpoint details
POST   /sync/jobs/{id}/resume  resume a failed/interrupted job
GET    /sync/state             last completed interval and completion time
GET    /papers                 query library (retracted hidden by default)
GET    /papers/{paper_id}      lookup one paper (410 when retracted)
"""
from __future__ import annotations

import os
import sqlite3
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Query

from . import db, sync
from .schemas import CreateJobRequest, JobResponse, PaperResponse, StateResponse

DB_PATH = os.getenv("COLLECTOR_DB", "/data/collector.db")
DEFAULT_SOURCE_URL = os.getenv("SOURCE_URL", "http://source:8000")


@asynccontextmanager
async def lifespan(app: FastAPI):
    db.init_db(DB_PATH)
    yield


app = FastAPI(
    title="Institutional Repository Sync Collector",
    version="1.0.0",
    lifespan=lifespan,
)


def _conn() -> sqlite3.Connection:
    return db.connect(DB_PATH)


def _job_response(conn: sqlite3.Connection, row: sqlite3.Row) -> JobResponse:
    return JobResponse(
        id=row["id"],
        source_url=row["source_url"],
        interval_start=row["interval_start"],
        interval_end=row["interval_end"],
        status=row["status"],
        cursor=row["cursor"],
        next_offset=row["next_offset"],
        pages_fetched=row["pages_fetched"],
        fetch_done=bool(row["fetch_done"]),
        cursor_resets=row["cursor_resets"],
        staged_count=db.staged_count(conn, row["id"]),
        error=row["error"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


@app.get("/health")
def health() -> dict:
    return {"status": "ok"}


def _mark_unstarted_failed(job_id: int, reason: str) -> None:
    """Turn a committed-but-unexecuted job into an explicitly resumable one.

    Only ever called for a job this request created, i.e. while still holding
    the execution claim. Uses a short-lived connection so a failure here can
    never be confused with the request's own transaction.
    """
    conn = _conn()
    try:
        with db.transaction(conn) as tx:
            db.mark_status(tx, job_id, "failed", reason)
    finally:
        conn.close()


# --------------------------------------------------------------------------- #
# sync jobs
# --------------------------------------------------------------------------- #
@app.post("/sync/jobs", status_code=201, response_model=JobResponse)
def create_job(req: CreateJobRequest) -> JobResponse:
    try:
        end = sync._normalize_iso(req.end)
        start_override = (
            sync._normalize_iso(req.start) if req.start is not None else None
        )
    except sync.SyncError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    source_url = (req.source_url or DEFAULT_SOURCE_URL).rstrip("/")
    if not source_url.startswith(("http://", "https://")):
        raise HTTPException(status_code=422, detail="source_url must be http(s) URL")

    # Claim the single execution slot BEFORE touching the database. A request
    # that loses the concurrency race gets 409 without creating a job row or
    # modifying any checkpoint — no orphaned 'collecting' task can be left
    # behind for a later resume/retry to trip over.
    if not sync.try_claim():
        raise HTTPException(
            status_code=409,
            detail="another synchronization is in progress in this API process; "
                   "retry after it finishes",
        )

    # Once start_background is called it always consumes the claim (either
    # handing it to the worker or releasing it); every earlier exit path must
    # release it here. If the job row was already committed but no worker took
    # it, it is flipped to 'failed' (resumable) — never left in 'collecting'.
    job_id: int | None = None
    started = False
    start_attempted = False
    conn = None
    try:
        conn = _conn()
        active = db.has_active_job(conn)
        if active is not None:
            raise HTTPException(
                status_code=409,
                detail=(
                    f"job {active['id']} is {active['status']}; resume it before "
                    "starting another job (overlapping intervals are rejected)"
                ),
            )

        state = db.get_state(conn)
        if start_override is not None:
            start = start_override
        elif state["completed_at"]:
            # Incremental: begin at the last completed time. Intervals are
            # closed on both sides, so the boundary event is re-fetched and
            # deduped by revision — no change at the boundary is ever missed.
            start = state["completed_at"]
        else:
            # First run: full fetch from the epoch.
            start = "1970-01-01T00:00:00Z"

        if start > end:
            raise HTTPException(status_code=422, detail="start must be <= end")

        job_id = db.create_job(conn, source_url, start, end)
        conn.commit()
        row = db.get_job(conn, job_id)
        response = _job_response(conn, row)

        # Hand the already-held claim to the worker thread. The claim is
        # transferred, not re-acquired, so there is no window in which another
        # request could slip in between "job accepted" and "worker started".
        start_attempted = True
        try:
            started = sync.start_background(
                job_id, DB_PATH, allowed_statuses=("collecting",), claim_held=True
            )
        except Exception:
            # start_background released the claim before re-raising.
            started = False
    finally:
        if conn is not None:
            conn.close()
        if not start_attempted:
            if job_id is not None:
                # The row is committed but still under our claim; make it
                # explicitly resumable instead of an unexecuted 'collecting'
                # row that would block every future sync. Never mask an
                # exception already propagating out of the try block.
                try:
                    _mark_unstarted_failed(
                        job_id, "worker could not be prepared; resume this job"
                    )
                except Exception:
                    pass
            sync.release_claim()

    if not started:
        # Defensive: the row exists but no worker owns it. Make it explicitly
        # resumable instead of leaving it stuck in 'collecting', which would
        # block every future job.
        _mark_unstarted_failed(job_id, "worker could not be started; resume this job")
        raise HTTPException(
            status_code=503,
            detail=f"job {job_id} was created but its worker could not start; "
                   f"resume it via POST /sync/jobs/{job_id}/resume",
        )
    return response


@app.get("/sync/jobs", response_model=list[JobResponse])
def list_jobs() -> list[JobResponse]:
    conn = _conn()
    try:
        return [_job_response(conn, row) for row in db.list_jobs(conn)]
    finally:
        conn.close()


@app.get("/sync/jobs/{job_id}", response_model=JobResponse)
def get_job(job_id: int) -> JobResponse:
    conn = _conn()
    try:
        row = db.get_job(conn, job_id)
        if row is None:
            raise HTTPException(status_code=404, detail="job not found")
        return _job_response(conn, row)
    finally:
        conn.close()


@app.post("/sync/jobs/{job_id}/resume", response_model=JobResponse)
def resume_job(job_id: int) -> JobResponse:
    # Claim before reading: a losing resume must not report success nor alter
    # the job it no longer owns. The winner hands the claim to the worker.
    if not sync.try_claim():
        raise HTTPException(
            status_code=409,
            detail="another synchronization is in progress in this API process; "
                   "retry after it finishes",
        )

    start_attempted = False
    conn = None
    try:
        conn = _conn()
        row = db.get_job(conn, job_id)
        if row is None:
            raise HTTPException(status_code=404, detail="job not found")
        if row["status"] not in ("failed", "interrupted"):
            raise HTTPException(
                status_code=409,
                detail=f"job is {row['status']}; only failed/interrupted jobs can resume",
            )
        response = _job_response(conn, row)

        # Same original closed interval is reused; the durable cursor
        # determines where fetching continues (or re-starts from offset 0
        # after cursor expiry). The status is re-checked under the held claim,
        # so a job that meanwhile completed/moved on is not resurrected.
        start_attempted = True
        try:
            started = sync.start_background(
                job_id, DB_PATH,
                allowed_statuses=("failed", "interrupted"),
                claim_held=True,
            )
        except Exception:
            # start_background released the claim before re-raising.
            started = False
    finally:
        if conn is not None:
            conn.close()
        if not start_attempted:
            sync.release_claim()

    if not started:
        raise HTTPException(
            status_code=409,
            detail="job changed state before it could be resumed; re-check "
                   "GET /sync/jobs/{id} and retry if it is failed/interrupted",
        )
    return response


@app.get("/sync/state", response_model=StateResponse)
def get_sync_state() -> StateResponse:
    conn = _conn()
    try:
        row = db.get_state(conn)
        return StateResponse(
            source_url=row["source_url"],
            last_start=row["last_start"],
            last_end=row["last_end"],
            completed_at=row["completed_at"],
        )
    finally:
        conn.close()


# --------------------------------------------------------------------------- #
# paper library (query side)
# --------------------------------------------------------------------------- #
@app.get("/papers", response_model=list[PaperResponse])
def list_papers(include_retracted: bool = Query(False)) -> list[PaperResponse]:
    conn = _conn()
    try:
        rows = db.list_papers(conn, include_retracted)
        return [PaperResponse(**dict(r)) for r in rows]
    finally:
        conn.close()


@app.get("/papers/{paper_id}", response_model=PaperResponse)
def get_paper(
    paper_id: str, include_retracted: bool = Query(False)
) -> PaperResponse:
    conn = _conn()
    try:
        row = db.get_paper(conn, paper_id)
        if row is None:
            raise HTTPException(status_code=404, detail="paper not found")
        if row["is_retracted"] and not include_retracted:
            raise HTTPException(
                status_code=410,
                detail="paper has been retracted and is hidden from retrieval; "
                       "use ?include_retracted=true to inspect it",
            )
        return PaperResponse(**dict(row))
    finally:
        conn.close()


@app.get("/papers-count")
def papers_count(include_retracted: bool = Query(False)) -> dict:
    conn = _conn()
    try:
        return {"count": db.count_papers(conn, include_retracted)}
    finally:
        conn.close()
