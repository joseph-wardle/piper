"""Flat row model for the silver_events DuckDB table.

:class:`SilverRow` is a frozen dataclass that mirrors the ``silver_events``
schema exactly.  It is the intermediate representation produced by
:func:`~piper.ingest.ingest_file` after an :class:`~piper.models.envelope.Envelope`
has been validated and normalized.

``SilverRow.from_envelope(envelope, source_file=..., source_line=...)`` is
the single constructor.  ``as_params()`` returns the column values in INSERT
order, ready to pass directly to DuckDB's ``executemany``.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from piper.models.envelope import Envelope


@dataclass(frozen=True)
class SilverRow:
    """One flattened row destined for ``silver_events``.

    Fields follow the schema defined in ``sql/schema/001_init.sql``.
    ``ingested_at_utc`` is omitted here and filled by the DB DEFAULT.
    ``payload`` and ``metrics`` are stored as JSON strings.
    """

    # Identity
    event_id: str
    schema_version: str
    event_type: str
    occurred_at_utc: datetime
    status: str

    # Pipeline
    pipeline_name: str
    pipeline_dcc: str | None

    # Host
    host_hostname: str
    host_user: str
    host_os: str | None

    # Session
    session_id: str
    action_id: str | None

    # Scope (all sparse)
    scope_show: str | None
    scope_sequence: str | None
    scope_shot: str | None
    scope_asset: str | None
    scope_department: str | None
    scope_task: str | None

    # Error detail
    error_code: str | None
    error_message: str | None

    # Variable content (serialized JSON)
    payload: str
    metrics: str

    # Source lineage
    source_file: str
    source_line: int

    # ------------------------------------------------------------------
    # Construction
    # ------------------------------------------------------------------

    @classmethod
    def from_envelope(
        cls,
        envelope: Envelope,
        *,
        source_file: Path,
        source_line: int,
    ) -> SilverRow:
        """Flatten *envelope* into a :class:`SilverRow`.

        Args:
            envelope:    Validated :class:`~piper.models.envelope.Envelope`.
            source_file: Path of the originating JSONL file.
            source_line: 1-based line number within *source_file*.
        """
        return cls(
            event_id=envelope.event_id,
            schema_version=envelope.schema_version,
            event_type=envelope.event_type,
            occurred_at_utc=envelope.occurred_at_utc,
            status=envelope.status,
            pipeline_name=envelope.pipeline.name,
            pipeline_dcc=envelope.pipeline.dcc,
            host_hostname=envelope.host.hostname,
            host_user=envelope.host.user,
            host_os=envelope.host.os,
            session_id=envelope.session.session_id,
            action_id=envelope.session.action_id,
            scope_show=envelope.scope.show,
            scope_sequence=envelope.scope.sequence,
            scope_shot=envelope.scope.shot,
            scope_asset=envelope.scope.asset,
            scope_department=envelope.scope.department,
            scope_task=envelope.scope.task,
            error_code=envelope.error.code if envelope.error else None,
            error_message=envelope.error.message if envelope.error else None,
            payload=_to_json(envelope.payload),
            metrics=_to_json(envelope.metrics),
            source_file=str(source_file),
            source_line=source_line,
        )

    # ------------------------------------------------------------------
    # Serialization
    # ------------------------------------------------------------------

    def as_params(self) -> list[Any]:
        """Return column values in INSERT order for use with ``executemany``.

        Order matches the ``INSERT INTO silver_events (...)`` statement in
        :mod:`piper.ingest`.
        """
        return [
            self.event_id,
            self.schema_version,
            self.event_type,
            self.occurred_at_utc,
            self.status,
            self.pipeline_name,
            self.pipeline_dcc,
            self.host_hostname,
            self.host_user,
            self.host_os,
            self.session_id,
            self.action_id,
            self.scope_show,
            self.scope_sequence,
            self.scope_shot,
            self.scope_asset,
            self.scope_department,
            self.scope_task,
            self.error_code,
            self.error_message,
            self.payload,
            self.metrics,
            self.source_file,
            self.source_line,
        ]


def _to_json(obj: Any) -> str:
    """Serialize *obj* to a compact JSON string."""
    return json.dumps(obj, separators=(",", ":"))
