"""Sync worker: paginated fetch with durable checkpoints and atomic publish."""
from __future__ import annotations

import sqlite3
import threading
from datetime import datetime, timezone

import httpx

from . import db

# Process-wide execution slot: at most one sync job advances in this API
# process.  The slot is claimed by the HTTP request thread *before* a job row
# is created/resumed, then handed over to the worker thread, which releases it
# in a finally block.  Claiming first is what makes "409" side-effect free:
# a request that loses the race never touches the database, so it can neither
# create an unexecuted ``collecting`` job nor change an existing checkpoint.
_running = threading.Lock()


class SyncError(RuntimeError):
    """Any fetch/transport failure; the job stays resumable."""


def _normalize_iso(value: str) -> str:
    """Accept ...Z / +00:00 / naive; emit canonical UTC 'YYYY-MM-DDTHH:MM:SSZ'."""
    text = value.strip()
    try:
        if text.endswith("Z"):
            dt = datetime.fromisoformat(text[:-1]).replace(tzinfo=timezone.utc)
        else:
            dt = datetime.fromisoformat(text)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            dt = dt.astimezone(timezone.utc)
    except ValueError as exc:
        raise SyncError(f"invalid ISO-8601 timestamp: {value!r}") from exc
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def _fetch_page(client: httpx.Client, source_url: str, start: str, end: str,
                cursor: str | None) -> dict:
    params = {"start": start, "end": end}
    if cursor:
        params["cursor"] = cursor
    try:
        resp = client.get(f"{source_url.rstrip('/')}/changes", params=params)
    except httpx.HTTPError as exc:
        raise SyncError(f"source request failed: {exc}") from exc
    if resp.status_code != 200:
        raise SyncError(
            f"source returned HTTP {resp.status_code}: {resp.text[:300]}"
        )
    return resp.json()


def try_claim() -> bool:
    """Try to reserve the process-wide execution slot (non-blocking).

    HTTP handlers claim this *before* creating/resuming a job, so a losing
    request can return 409 without having written anything. A successful
    claim must be handed to :func:`start_background` (``claim_held=True``)
    or explicitly returned via :func:`release_claim`.
    """
    return _running.acquire(blocking=False)


def release_claim() -> None:
    """Return a claim that was not handed over to a worker."""
    _running.release()


def run_sync(job_id: int, db_path: str | None = None,
             lock_held: bool = False) -> None:
    """Execute (or resume) one sync job until completion or failure.

    On every page: persist rows + next cursor in ONE transaction before the
    next request. A 410 (expired cursor) discards this job's staging and
    re-fetches the *original* interval — never a newer cutoff.

    ``lock_held`` is set by :func:`start_background`, which claims the
    process-wide execution slot *before* spawning this thread. Ownership is
    therefore handed over atomically: the request thread can no longer release
    it, and this worker releases it in ``finally`` on every exit path
    (completion, source failure, unexpected exception, early return).
    """
    if not lock_held:
        if not _running.acquire(blocking=False):
            return  # another worker is active in this process
    conn: sqlite3.Connection | None = None
    try:
        conn = db.connect(db_path)
        job = db.get_job(conn, job_id)
        if job is None or job["status"] not in ("collecting", "interrupted", "failed"):
            return
        db.mark_status(conn, "collecting", job_id)
        conn.commit()

        source_url = job["source_url"]
        start = job["interval_start"]
        end = job["interval_end"]
        cursor = job["cursor"]
        pages = job["pages_fetched"]
        fetch_done = bool(job["fetch_done"])

        with httpx.Client(timeout=10.0) as client:
            while not fetch_done:
                try:
                    page = _fetch_page(client, source_url, start, end, cursor)
                except SyncError as exc:
                    msg = str(exc)
                    if "HTTP 410" in msg:
                        # Cursor expired/invalid: restart the ORIGINAL interval.
                        with db.transaction(conn) as tx:
                            db.clear_staged(tx, job_id)
                            resets = db.increment_cursor_resets(tx, job_id)
                            db.mark_status(
                                tx, job_id, "failed",
                                f"cursor expired; interval re-fetch required "
                                f"(reset #{resets})",
                            )
                        return
                    with db.transaction(conn) as tx:
                        db.mark_status(tx, job_id, "failed", msg)
                    return

                # Durable checkpoint: page rows and continuation cursor commit
                # together, so resume never loses or skips a page.
                with db.transaction(conn) as tx:
                    for event in page["changes"]:
                        db.upsert_staged(tx, job_id, event)
                    cursor = page["next_cursor"]
                    pages += 1
                    fetch_done = bool(page["done"])
                    db.update_cursor(
                        tx, job_id, cursor, page["next_offset"],
                        pages, fetch_done,
                    )

        # Whole interval collected: publish staging and completed time in one
        # atomic transaction. Any error rolls back; old library stays queryable
        # and staging remains private until a successful resume.
        with db.transaction(conn) as tx:
            db.mark_status(tx, job_id, "committing")
            published_at = db.utc_now_iso()
            for row in db.list_staged(tx, job_id):
                db.upsert_paper(tx, row)
            # completed_at is the change-time baseline for the next incremental
            # fetch, so it must equal this interval's closed end (not wall-clock
            # time); otherwise a historical cutoff would never be re-fetchable.
            tx.execute(
                "UPDATE sync_state SET source_url=?, last_start=?, last_end=?, "
                "completed_at=? WHERE id=1",
                (source_url, start, end, end),
            )
            tx.execute(
                "UPDATE jobs SET updated_at=? WHERE id=?", (published_at, job_id)
            )
            db.mark_status(tx, job_id, "completed")
    except Exception as exc:  # pragma: no cover - defensive guard
        # Never leave a job silently occupying the slot as 'collecting': if
        # this process is still alive, make it explicitly resumable. If even
        # opening a connection fails, the next startup flips it to
        # 'interrupted' — either way the execution slot is released below.
        try:
            if conn is None:
                conn = db.connect(db_path)
            conn.rollback()
            with db.transaction(conn) as tx:
                db.mark_status(tx, job_id, "failed", f"unexpected error: {exc}")
        except Exception:
            pass
    finally:
        try:
            if conn is not None:
                conn.close()
        finally:
            _running.release()


def _job_has_status(job_id: int, db_path: str | None, allowed: tuple[str, ...]) -> bool:
    conn = db.connect(db_path)
    try:
        job = db.get_job(conn, job_id)
        return job is not None and job["status"] in allowed
    finally:
        conn.close()


def start_background(
    job_id: int,
    db_path: str | None = None,
    *,
    allowed_statuses: tuple[str, ...] = ("collecting", "interrupted", "failed"),
    claim_held: bool = False,
) -> bool:
    """Atomically claim the process-wide slot and spawn a daemon worker.

    The claim happens *before* the job's runnability is re-checked and before
    the thread exists, so two callers racing for the same (or different) job
    can never both start: exactly one gets ``True``, the other ``False`` and no
    state is modified. Job status is re-read while the claim is held, which
    also closes the "previous job just completed" race: a caller that arrived
    with a stale ``failed`` observation gets ``False`` instead of spawning a
    worker that would silently no-op.

    ``claim_held=True`` means the caller already called :func:`try_claim` and
    hands that claim over to the worker thread; otherwise the claim is taken
    here. On ``False`` any claim is released before returning.

    Returns ``False`` only when the slot is busy or the job is no longer in an
    allowed state.
    """
    if claim_held:
        owns_claim = True
    else:
        owns_claim = _running.acquire(blocking=False)
        if not owns_claim:
            return False

    def _abort() -> None:
        if owns_claim:
            _running.release()

    try:
        if not _job_has_status(job_id, db_path, allowed_statuses):
            _abort()
            return False
        thread = threading.Thread(
            target=run_sync, args=(job_id, db_path),
            kwargs={"lock_held": True}, daemon=True,
            name=f"sync-job-{job_id}",
        )
        thread.start()
    except BaseException:
        # Never leak the claim if validation or thread creation fails.
        _abort()
        raise
    return True

