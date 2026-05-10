# Copyright (c) 2026 Beijing Volcano Engine Technology Co., Ltd.
# SPDX-License-Identifier: AGPL-3.0
"""Best-effort Sparrow operator event reporting.

Hermes gateway starts a localhost-only HTTP sink and exposes its endpoint
through environment variables.  OpenViking uses this tiny client to report
high-level events without importing Hermes modules or changing its own logs.
"""

from __future__ import annotations

import json
import os
import urllib.request
from typing import Any


_ENDPOINT_ENV = "SPARROW_LOG_ENDPOINT"
_TOKEN_ENV = "SPARROW_LOG_TOKEN"
_TIMEOUT_SECONDS = 0.5


def emit_sparrow_event(event: str, **fields: Any) -> None:
    """POST one event to Hermes' Sparrow log sink if configured.

    This is intentionally best-effort: OpenViking memory work must never fail
    because the operator log sink is absent, slow, or already shutting down.
    """
    endpoint = os.environ.get(_ENDPOINT_ENV, "").strip()
    token = os.environ.get(_TOKEN_ENV, "").strip()
    if not endpoint or not token:
        return

    payload = {"event": event, "fields": fields}
    try:
        data = json.dumps(payload, ensure_ascii=False, default=str).encode("utf-8")
        request = urllib.request.Request(
            endpoint,
            data=data,
            method="POST",
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
            },
        )
        with urllib.request.urlopen(request, timeout=_TIMEOUT_SECONDS):
            pass
    except Exception:
        return
