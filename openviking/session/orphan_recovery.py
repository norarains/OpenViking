# Copyright (c) 2026 Beijing Volcano Engine Technology Co., Ltd.
# SPDX-License-Identifier: AGPL-3.0
"""Recover orphaned session-commit archives left behind by crashed/killed
phase-2 workers.

Background
----------
``Session.commit_async`` returns 200 immediately after archiving the
messages (phase 1) and spawns ``_run_memory_extraction`` as a fire-and-
forget asyncio task (phase 2).  Phase 2 writes either ``.done`` (success)
or ``.failed.json`` (any error) to the archive directory before exiting.

If the phase-2 coroutine is killed before either marker is written —
process kill -9, OOM, server restart mid-extraction, or an exception
path that doesn't write the marker — the archive directory is left
with neither file.  Two consequences:

1. ``_wait_for_previous_archive_done`` (in session.py) ``while True``
   polls forever for a marker that no live coroutine will ever produce,
   deadlocking the entire commit chain for that session.
2. ``_get_blocking_failed_archive_ref`` would treat any ``.failed.json``
   as an unresolved hard block, which is correct for a real failure but
   wrong for an orphan we're sweeping passively at startup.

So this module writes ``.done`` (the "ack and move on" marker), not
``.failed.json``: phase 2 never ran for these archives, the messages
inside their ``messages.jsonl`` were never extracted into long-term
memory, but the commit pipeline can resume without manual intervention.
The raw messages are still on disk if the operator wants to manually
re-extract via ``POST /api/v1/sessions/{sid}/extract``.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path

logger = logging.getLogger(__name__)


def _read_message_id_at(line: str) -> str:
    try:
        return json.loads(line).get("id", "") or ""
    except (ValueError, TypeError):
        return ""


def _build_done_payload(messages_jsonl: Path) -> str:
    """Build a ``.done`` marker payload with first/last message IDs from
    ``messages.jsonl``.  Returns a payload even when the file is empty/
    missing (with empty IDs) so the marker still gets written."""
    first_id = ""
    last_id = ""
    if messages_jsonl.is_file():
        try:
            with messages_jsonl.open("r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    if not first_id:
                        first_id = _read_message_id_at(line)
                    last_id = _read_message_id_at(line) or last_id
        except OSError:
            pass
    return json.dumps(
        {
            "starting_message_id": first_id,
            "ending_message_id": last_id,
            "orphaned": True,
            "orphaned_at": datetime.now(timezone.utc).isoformat(),
        },
        ensure_ascii=False,
    )


def recover_orphan_archives(workspace: str | Path) -> int:
    """Scan ``<workspace>/viking/<account>/session/<sid>/history/archive_*/``
    and write a ``.done{orphaned: true}`` marker into every archive that
    has neither ``.done`` nor ``.failed.json``.

    We use ``.done`` (not ``.failed.json``) because ``commit_async``
    treats any unresolved ``.failed.json`` as a hard block (HTTP 412),
    which is the right behavior for genuine extraction failures the
    operator should look at — but wrong for orphans we're sweeping
    passively.  Net effect for an orphan: messages inside its
    ``messages.jsonl`` were never extracted into long-term memory, but
    the commit pipeline keeps moving.  Operator can manually re-extract
    via ``POST /api/v1/sessions/{sid}/extract`` if needed.

    Returns the count of archives marked.  Synchronous + stdlib only so
    it's safe to call from the lifespan hook before the asyncio loop
    starts servicing requests.
    """
    root = Path(workspace) / "viking"
    if not root.is_dir():
        return 0

    marked = 0
    for archive_dir in root.glob("*/session/*/history/archive_*"):
        if not archive_dir.is_dir():
            continue
        if (archive_dir / ".done").exists():
            continue
        if (archive_dir / ".failed.json").exists():
            continue
        try:
            payload = _build_done_payload(archive_dir / "messages.jsonl")
            (archive_dir / ".done").write_text(payload, encoding="utf-8")
            marked += 1
            logger.warning(
                "[orphan_recovery] marked %s as done {orphaned:true} — "
                "messages were not extracted to memory",
                archive_dir,
            )
        except OSError as exc:
            logger.warning(
                "[orphan_recovery] failed to write .done into %s: %s",
                archive_dir, exc,
            )

    if marked:
        logger.info("[orphan_recovery] marked %d orphaned archive(s)", marked)
    return marked
