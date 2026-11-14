"""Collector tests against an in-memory mock source (httpx MockTransport).

Run:  python -m pytest -q   (from a venv with the collector requirements)
"""
from __future__ import annotations

import json
import os
import tempfile
import threading
import time
import uuid

import httpx
import pytest

# Point the collector at a throwaway DB before importing the app.
_TMP = tempfile.TemporaryDirectory()
os.environ["COLLECTOR_DB"] = os.path.join(_TMP.name, "test.db")
os.environ["SOURCE_URL"] = "http://fake-source"

from fastapi.testclient import TestClient  # noqa: E402

from collector.app import db, sync  # noqa: E402
from collector.app.main import app  # noqa: E402


# --------------------------------------------------------------------------- #
# Fake source: closed-interval paging, duplicate/out-of-order rows, faults,
# cursor expiry — enough behavior to exercise every collector guarantee.
# --------------------------------------------------------------------------- #
class FakeSource:
    PAGE_SIZE = 2

    def __init__(self) -> None:
        self.events: list[dict] = []
        self.fail_next = 0
        self.generation = 0
        self.calls = 0
        self.page_delay = 0.0
        self.page_gate: threading.Event | None = None
        self.page_entered = threading.Event()
        self._lock = threading.Lock()

    def add(self, paper_id, rev, retracted, changed_at, title=None, authors=None):
        self.events.append({
            "paper_id": paper_id, "rev": rev,
            "title": title or f"title-{paper_id}-{rev}",
            "authors": authors or "author",
            "is_retracted": 1 if retracted else 0,
            "changed_at": changed_at,
        })

    def _cursor(self, start, end, offset):
        return f"{self.generation}.{json.dumps([start, end, offset])}"

    def _parse(self, cursor, start, end):
        gen, payload = cursor.split(".", 1)
        if int(gen) != self.generation:
            return httpx.Response(410, json={"detail": "cursor invalid or expired"})
        s, e, offset = json.loads(payload)
        if (s, e) != (start, end):
            return httpx.Response(410, json={"detail": "cursor invalid or expired"})
        return offset

    def handler(self, request: httpx.Request) -> httpx.Response:
        with self._lock:
            self.calls += 1
            if self.fail_next > 0:
                self.fail_next -= 1
                return httpx.Response(503, json={"detail": "injected failure"})
            delay = self.page_delay

        # Slow page: block the FETCHING THREAD only (never the test thread),
        # so the API remains free to answer status queries meanwhile.
        self.page_entered.set()
        if self.page_gate is not None:
            self.page_gate.wait(timeout=10)
        if delay:
            time.sleep(delay)

        params = request.url.params
        start, end = params["start"], params["end"]
        cursor = params.get("cursor")
        offset = 0
        if cursor is not None:
            parsed = self._parse(cursor, start, end)
            if isinstance(parsed, httpx.Response):
                return parsed
            offset = parsed

        matched = [e for e in self.events if start <= e["changed_at"] <= end]
        rows = matched[offset:offset + self.PAGE_SIZE]
        next_offset = offset + len(rows)
        done = next_offset >= len(matched)
        return httpx.Response(200, json={
            "changes": rows,
            "next_cursor": None if done else self._cursor(start, end, next_offset),
            "next_offset": next_offset,
            "page": offset // self.PAGE_SIZE + 1,
            "page_size": self.PAGE_SIZE,
            "total_matched": len(matched),
            "done": done,
        })


@pytest.fixture
def source():
    return FakeSource()


@pytest.fixture
def client(source, monkeypatch):
    transport = httpx.MockTransport(source.handler)

    real_client = httpx.Client

    def patched_client(*args, **kwargs):
        kwargs["transport"] = transport
        return real_client(*args, **kwargs)

    monkeypatch.setattr(httpx, "Client", patched_client)
    # Fresh DB per test.
    db_path = os.path.join(_TMP.name, f"test_{uuid.uuid4().hex}.db")
    monkeypatch.setattr("collector.app.main.DB_PATH", db_path)

    # Do not spawn API-level background threads; tests drive the real worker
    # synchronously via sync.run_sync (same code path). The handler claims the
    # process slot before calling start_background, so the fake must consume
    # that claim (otherwise the synchronous run below could not acquire it).
    def fake_start_background(job_id, _db_path=None, *, allowed_statuses=(), claim_held=False):
        if not claim_held:
            sync.try_claim()
        sync.release_claim()
        return True

    monkeypatch.setattr(
        "collector.app.main.sync.start_background", fake_start_background
    )
    db.init_db(db_path)
    with TestClient(app) as c:
        c.db_path = db_path
        yield c
        # Drain any worker thread so its run lock cannot leak into next test.
        for thread in threading.enumerate():
            if thread.name.startswith("sync-job-") and thread.is_alive():
                thread.join(timeout=10)
        assert not sync._running.locked(), "worker run lock leaked across tests"


def _run(client, job_id, db_path):
    """Run the worker synchronously (same code path as the background thread)."""
    sync.run_sync(job_id, db_path)


# Captured before the fixture replaces it with a no-thread stub; tests that
# need the genuine claim->thread handoff use this.
_REAL_START_BACKGROUND = sync.start_background


def _job(client, job_id):
    return client.get(f"/sync/jobs/{job_id}").json()


def _wait_for_worker(job_id, timeout=10.0):
    """Wait until the background worker for ``job_id`` has finished."""
    name = f"sync-job-{job_id}"
    for thread in threading.enumerate():
        if thread.name == name:
            thread.join(timeout=timeout)
            assert not thread.is_alive(), "worker thread did not finish"
            return
    raise AssertionError(f"worker thread {name} was never started")


def _hold_slot():
    """Simulate a worker currently owning the process-wide execution slot."""
    assert sync.try_claim()


# --------------------------------------------------------------------------- #
# Tests
# --------------------------------------------------------------------------- #
def test_full_sync_dedupes_keeps_highest_rev_and_hides_retracted(client, source):
    # P1: rev1 -> rev2 -> retracted rev3, with a stale rev1 redelivered late.
    source.add("P1", 1, False, "2026-11-01T08:00:00Z")
    source.add("P1", 2, False, "2026-11-02T08:00:00Z")
    source.add("P1", 3, True, "2026-11-03T08:00:00Z")
    # P2 with an exact duplicate row.
    source.add("P2", 1, False, "2026-11-01T09:00:00Z")
    source.add("P2", 1, False, "2026-11-01T09:00:00Z")
    # Stale revision of P1 arriving after the retraction row.
    source.add("P1", 1, False, "2026-11-01T08:00:00Z")

    job = client.post("/sync/jobs", json={"end": "2026-11-05T00:00:00Z"}).json()
    _run(client, job["id"], client.db_path)

    assert _job(client, job["id"])["status"] == "completed"
    state = client.get("/sync/state").json()
    assert state["completed_at"] is not None
    assert state["last_end"] == "2026-11-05T00:00:00Z"

    # Only P2 visible; P1 retracted.
    visible = client.get("/papers").json()
    assert [p["paper_id"] for p in visible] == ["P2"]

    r = client.get("/papers/P1")
    assert r.status_code == 410
    p1 = client.get("/papers/P1?include_retracted=true").json()
    assert p1["rev"] == 3 and p1["is_retracted"] == 1

    # Stale rev1 did not overwrite the retraction during commit.
    p2 = client.get("/papers/P2").json()
    assert p2["rev"] == 1


def test_failure_keeps_old_library_hides_staging_and_resumes(client, source):
    source.add("P1", 1, False, "2026-11-01T08:00:00Z")
    job = client.post("/sync/jobs", json={"end": "2026-11-05T00:00:00Z"}).json()
    _run(client, job["id"], client.db_path)
    assert _job(client, job["id"])["status"] == "completed"

    # Incremental window: 3 matched rows (PAGE_SIZE=2 -> 2 pages), fail once.
    source.add("P2", 1, False, "2026-11-06T08:00:00Z")
    source.add("P3", 1, False, "2026-11-07T08:00:00Z")
    source.add("P4", 1, False, "2026-11-08T08:00:00Z")
    source.fail_next = 1

    job2 = client.post("/sync/jobs", json={"end": "2026-11-10T00:00:00Z"}).json()
    # Incremental start must be the previous completed time.
    assert job2["interval_start"] == client.get("/sync/state").json()["completed_at"]
    _run(client, job2["id"], client.db_path)

    failed = _job(client, job2["id"])
    assert failed["status"] == "failed"
    assert failed["error"] and "503" in failed["error"]

    # Old library queryable; staged content invisible.
    assert client.get("/papers/P2").status_code == 404
    assert client.get("/papers/P4").status_code == 404
    assert [p["paper_id"] for p in client.get("/papers").json()] == ["P1"]

    # Resume from checkpoint (or from offset 0 when the very first request
    # failed) and finish the same interval.
    resp = client.post(f"/sync/jobs/{job2['id']}/resume")
    assert resp.status_code == 200
    _run(client, job2["id"], client.db_path)
    done = _job(client, job2["id"])
    assert done["status"] == "completed"
    assert done["cursor_resets"] == 0
    assert sorted(p["paper_id"] for p in client.get("/papers").json()) == [
        "P1", "P2", "P3", "P4"
    ]


def test_expired_cursor_refetches_original_interval_not_new_cutoff(client, source):
    source.add("P1", 1, False, "2026-11-01T08:00:00Z")
    source.add("P2", 1, False, "2026-11-02T08:00:00Z")
    source.add("P3", 1, False, "2026-11-03T08:00:00Z")

    job = client.post("/sync/jobs", json={"end": "2026-11-05T00:00:00Z"}).json()
    job_id = job["id"]
    original_end = job["interval_end"]

    # Invalidate after the first page is checkpointed; the next request 410s.
    orig_fetch = sync._fetch_page

    def flaky_fetch(http_client, url, start, end, cursor):
        result = orig_fetch(http_client, url, start, end, cursor)
        if cursor is None:
            source.generation += 1  # kill the cursor returned with page 1
        return result

    sync._fetch_page = flaky_fetch
    try:
        _run(client, job_id, client.db_path)
    finally:
        sync._fetch_page = orig_fetch

    failed = _job(client, job_id)
    assert failed["status"] == "failed"
    assert failed["cursor_resets"] == 1
    assert failed["staged_count"] == 0       # staging was discarded
    assert failed["pages_fetched"] == 0      # counters restart for re-fetch
    assert failed["interval_end"] == original_end

    # Resume re-fetches from offset 0 within the SAME closed interval.
    client.post(f"/sync/jobs/{job_id}/resume")
    _run(client, job_id, client.db_path)
    done = _job(client, job_id)
    assert done["status"] == "completed"
    assert done["interval_end"] == original_end  # never jumped to a newer cutoff
    assert sorted(p["paper_id"] for p in client.get("/papers").json()) == [
        "P1", "P2", "P3"
    ]


def test_retraction_is_sticky_across_commits(client, source):
    source.add("P1", 1, False, "2026-11-01T08:00:00Z")
    job = client.post("/sync/jobs", json={"end": "2026-11-05T00:00:00Z"}).json()
    _run(client, job["id"], client.db_path)

    # Later window redelivers an OLD non-retracted revision (lower rev).
    source.add("P1", 1, False, "2026-11-01T08:00:00Z")
    job2 = client.post("/sync/jobs", json={"end": "2026-11-10T00:00:00Z"}).json()
    _run(client, job2["id"], client.db_path)
    assert _job(client, job2["id"])["status"] == "completed"
    p1 = client.get("/papers/P1").json()
    assert p1["rev"] == 1 and p1["is_retracted"] == 0

    # Retract, then confirm an even later old row cannot un-retract.
    source.add("P1", 2, True, "2026-11-11T08:00:00Z")
    source.add("P1", 1, False, "2026-11-01T08:00:00Z")
    job3 = client.post("/sync/jobs", json={"end": "2026-11-12T00:00:00Z"}).json()
    _run(client, job3["id"], client.db_path)
    assert client.get("/papers/P1").status_code == 410
    p1 = client.get("/papers/P1?include_retracted=true").json()
    assert p1["rev"] == 2 and p1["is_retracted"] == 1


def test_restart_marks_job_interrupted_and_resumes(client, source, monkeypatch):
    source.add("P1", 1, False, "2026-11-01T08:00:00Z")
    source.add("P2", 1, False, "2026-11-02T08:00:00Z")

    job = client.post("/sync/jobs", json={"end": "2026-11-05T00:00:00Z"}).json()
    # Simulate process restart while the job is still collecting.
    db.init_db(client.db_path)
    restarted = _job(client, job["id"])
    assert restarted["status"] == "interrupted"

    # Cannot start a competing job while one is interrupted.
    conflict = client.post("/sync/jobs", json={"end": "2026-11-09T00:00:00Z"})
    assert conflict.status_code == 409

    client.post(f"/sync/jobs/{job['id']}/resume")
    _run(client, job["id"], client.db_path)
    assert _job(client, job["id"])["status"] == "completed"
    assert sorted(p["paper_id"] for p in client.get("/papers").json()) == ["P1", "P2"]


def test_background_endpoint_runs_full_flow(client, source, monkeypatch):
    # Use the genuine claim -> thread handoff (the fixture stubs it out).
    monkeypatch.setattr(sync, "start_background", _REAL_START_BACKGROUND)
    source.add("P1", 1, False, "2026-11-01T08:00:00Z")
    resp = client.post("/sync/jobs", json={"end": "2026-11-05T00:00:00Z"})
    assert resp.status_code == 201
    job_id = resp.json()["id"]

    _wait_for_worker(job_id)
    assert _job(client, job_id)["status"] == "completed"
    assert client.get("/papers/P1").status_code == 200


# --------------------------------------------------------------------------- #
# Concurrency / handoff regressions
# --------------------------------------------------------------------------- #
def test_rejected_create_does_not_leave_orphan_collecting_job(client, source):
    """A 409 while another job owns the slot must not add any job row."""
    # A job exists and a worker is advancing it right now.
    job = client.post("/sync/jobs", json={"end": "2026-11-05T00:00:00Z"}).json()
    _hold_slot()  # the worker holds the process-wide slot
    try:
        before = client.get("/sync/jobs").json()
        resp = client.post("/sync/jobs", json={"end": "2026-11-09T00:00:00Z"})
        assert resp.status_code == 409
        after = client.get("/sync/jobs").json()
        assert after == before, "rejected request created an orphan job"
        assert [j["id"] for j in after] == [job["id"]]
        assert after[0]["status"] == "collecting"
    finally:
        sync.release_claim()

    # The blocked job is still the only one and is still runnable.
    _run(client, job["id"], client.db_path)
    assert _job(client, job["id"])["status"] == "completed"
    # Slot free again: the very next create is accepted.
    resp = client.post("/sync/jobs", json={"end": "2026-11-09T00:00:00Z"})
    assert resp.status_code == 201


def test_validation_errors_release_the_claim(client, source):
    """A rejected payload must not leave the execution slot occupied."""
    assert client.post(
        "/sync/jobs", json={"end": "not-a-timestamp"}
    ).status_code == 422
    assert client.post(
        "/sync/jobs", json={"start": "2026-11-09T00:00:00Z",
                            "end": "2026-11-05T00:00:00Z"}
    ).status_code == 422
    assert not sync._running.locked(), "a 422 leaked the execution slot"

    resp = client.post("/sync/jobs", json={"end": "2026-11-05T00:00:00Z"})
    assert resp.status_code == 201
    assert resp.json()["status"] == "collecting"


def test_rejected_create_cannot_touch_existing_progress(client, source, monkeypatch):
    monkeypatch.setattr(sync, "start_background", _REAL_START_BACKGROUND)
    source.add("P1", 1, False, "2026-11-01T08:00:00Z")
    source.add("P2", 1, False, "2026-11-02T08:00:00Z")
    # Make the first page slow so the job is genuinely in flight.
    source.page_gate = threading.Event()

    first = client.post("/sync/jobs", json={"end": "2026-11-05T00:00:00Z"})
    assert first.status_code == 201
    job_id = first.json()["id"]
    assert source.page_entered.wait(timeout=10), "worker never started fetching"

    in_flight = _job(client, job_id)
    assert in_flight["status"] == "collecting"

    # A competing create is rejected and leaves the running job untouched.
    conflict = client.post("/sync/jobs", json={"end": "2026-11-09T00:00:00Z"})
    assert conflict.status_code == 409
    assert _job(client, job_id) == in_flight
    assert len(client.get("/sync/jobs").json()) == 1

    source.page_gate.set()
    _wait_for_worker(job_id)
    done = _job(client, job_id)
    assert done["status"] == "completed"
    assert sorted(p["paper_id"] for p in client.get("/papers").json()) == ["P1", "P2"]


def test_rejected_resume_does_not_touch_job(client, source):
    source.add("P1", 1, False, "2026-11-01T08:00:00Z")
    job = client.post("/sync/jobs", json={"end": "2026-11-05T00:00:00Z"}).json()
    source.fail_next = 1
    _run(client, job["id"], client.db_path)
    failed = _job(client, job["id"])
    assert failed["status"] == "failed"

    _hold_slot()
    try:
        resp = client.post(f"/sync/jobs/{job['id']}/resume")
        assert resp.status_code == 409
        assert _job(client, job["id"]) == failed
    finally:
        sync.release_claim()

    # Once the slot is free, resume works normally.
    assert client.post(f"/sync/jobs/{job['id']}/resume").status_code == 200
    _run(client, job["id"], client.db_path)
    assert _job(client, job["id"])["status"] == "completed"


def test_start_background_refuses_non_runnable_job_without_leaking_slot(
    client, source,
):
    source.add("P1", 1, False, "2026-11-01T08:00:00Z")
    job = client.post("/sync/jobs", json={"end": "2026-11-05T00:00:00Z"}).json()
    _run(client, job["id"], client.db_path)
    assert _job(client, job["id"])["status"] == "completed"

    # No worker may be spawned for a completed job (e.g. a resume whose status
    # was observed as failed a moment before the worker finished).
    assert _REAL_START_BACKGROUND(job["id"], client.db_path) is False
    assert not sync._running.locked(), "refused handoff leaked the execution slot"
    assert _job(client, job["id"])["status"] == "completed"


def test_worker_exit_always_releases_slot_and_marks_failure(
    client, source, monkeypatch,
):
    """An unexpected error must not leave an occupied slot or a stuck job."""
    source.add("P1", 1, False, "2026-11-01T08:00:00Z")

    original_list_staged = db.list_staged

    def boom(*_args, **_kwargs):
        raise RuntimeError("disk on fire")

    monkeypatch.setattr(sync.db, "list_staged", boom)
    job = client.post("/sync/jobs", json={"end": "2026-11-05T00:00:00Z"}).json()
    _run(client, job["id"], client.db_path)

    failed = _job(client, job["id"])
    assert failed["status"] == "failed"
    assert "disk on fire" in (failed["error"] or "")
    assert not sync._running.locked()
    # The failed job is resumable once the cause is gone.
    monkeypatch.setattr(sync.db, "list_staged", original_list_staged)
    client.post(f"/sync/jobs/{job['id']}/resume")
    _run(client, job["id"], client.db_path)
    assert _job(client, job["id"])["status"] == "completed"


def test_slow_page_keeps_status_and_paper_queries_responsive(
    client, source, monkeypatch,
):
    monkeypatch.setattr(sync, "start_background", _REAL_START_BACKGROUND)
    source.add("P1", 1, False, "2026-11-01T08:00:00Z")
    source.add("P2", 1, False, "2026-11-02T08:00:00Z")
    source.page_gate = threading.Event()

    job = client.post("/sync/jobs", json={"end": "2026-11-05T00:00:00Z"}).json()
    assert source.page_entered.wait(timeout=10)

    # The worker is blocked fetching a slow page; queries must still answer.
    started = time.monotonic()
    assert _job(client, job["id"])["status"] == "collecting"
    assert client.get("/papers").json() == []
    assert time.monotonic() - started < 2.0

    source.page_gate.set()
    _wait_for_worker(job["id"])
    assert _job(client, job["id"])["status"] == "completed"


def test_accepted_job_survives_worker_completion_and_next_submit(client, source,
                                                               monkeypatch):
    """Completion and a new submission interleaving must not lose execution."""
    monkeypatch.setattr(sync, "start_background", _REAL_START_BACKGROUND)
    source.add("P1", 1, False, "2026-11-01T08:00:00Z")
    first = client.post("/sync/jobs", json={"end": "2026-11-05T00:00:00Z"}).json()
    _wait_for_worker(first["id"])
    assert _job(client, first["id"])["status"] == "completed"

    # Immediately submit the next incremental job; the previous worker has
    # exited and released its claim, so it must be accepted and executed.
    source.add("P2", 1, False, "2026-11-06T08:00:00Z")
    second = client.post("/sync/jobs", json={"end": "2026-11-10T00:00:00Z"})
    assert second.status_code == 201
    _wait_for_worker(second.json()["id"])
    assert _job(client, second.json()["id"])["status"] == "completed"
    assert sorted(p["paper_id"] for p in client.get("/papers").json()) == [
        "P1", "P2"
    ]
