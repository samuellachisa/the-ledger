"""
MCP Resource handlers (query side).

Reads from projections ONLY — never from event streams directly (except audit-trail,
which is a justified exception for auditors).

Imported and registered by server.py.
"""
from __future__ import annotations

import json
import logging
import time
from uuid import UUID

import asyncpg
from mcp.types import ReadResourceResult, Resource, TextContent

from src.event_store import EventStore
from src.integrity.audit_chain import run_integrity_check
from src.observability.metrics import get_metrics
from src.projections.agent_performance import list_agent_performance
from src.projections.application_summary import get_application_summary, list_applications
from src.projections.compliance_audit import get_compliance_history

logger = logging.getLogger(__name__)


def get_resource_definitions() -> list[Resource]:
    return [
        Resource(uri="ledger://ledger/health", name="Health Check", mimeType="application/json"),
        Resource(uri="ledger://applications", name="All Applications", mimeType="application/json"),
        Resource(uri="ledger://applications/{id}", name="Application Summary", mimeType="application/json"),
        Resource(uri="ledger://applications/{id}/audit-trail", name="Audit Trail", mimeType="application/json"),
        Resource(uri="ledger://applications/{id}/compliance", name="Compliance History", mimeType="application/json"),
        Resource(uri="ledger://agents/performance", name="Agent Performance", mimeType="application/json"),
        Resource(uri="ledger://integrity/{id}", name="Integrity Check", mimeType="application/json"),
        Resource(uri="ledger://integrity/alerts", name="Open Integrity Alerts", mimeType="application/json"),
        Resource(uri="ledger://dead-letters", name="Dead Letter Queue", mimeType="application/json"),
        Resource(uri="ledger://metrics", name="System Metrics", mimeType="application/json"),
        Resource(uri="ledger://metrics/prometheus", name="Prometheus Metrics", mimeType="text/plain"),
        Resource(uri="ledger://events/correlation/{id}", name="Correlation Chain", mimeType="application/json"),
        Resource(uri="ledger://events/causation/{id}", name="Causation Chain", mimeType="application/json"),
        Resource(uri="ledger://saga/status", name="Saga Instance Status", mimeType="application/json"),
        Resource(uri="ledger://auth/audit", name="Auth Audit Log", mimeType="application/json"),
    ]


def _json(data) -> ReadResourceResult:
    return ReadResourceResult(contents=[
        TextContent(type="text", text=json.dumps(data, default=str))
    ])


async def dispatch_resource(
    uri: str,
    pool: asyncpg.Pool,
    event_store: EventStore,
    dead_letter=None,
    integrity_monitor=None,
) -> ReadResourceResult:
    """Route a resource URI to the appropriate query."""
    async with pool.acquire() as conn:

        if uri == "ledger://ledger/health":
            t0 = time.monotonic()
            db_ok = await conn.fetchval("SELECT 1") == 1
            latency_ms = (time.monotonic() - t0) * 1000

            dlq_depth = 0
            if dead_letter:
                counts = await dead_letter.count_unresolved()
                dlq_depth = sum(counts.values())

            outbox_backlog = await conn.fetchval(
                "SELECT COUNT(*) FROM outbox WHERE status = 'pending'"
            ) or 0
            saga_stuck = await conn.fetchval(
                "SELECT COUNT(*) FROM saga_instances WHERE step = 'failed'"
            ) or 0

            if not db_ok:
                overall = "critical"
            elif dlq_depth > 100 or saga_stuck > 10 or outbox_backlog > 1000:
                overall = "degraded"
            else:
                overall = "ok"

            return _json({
                "status": overall,
                "db_latency_ms": round(latency_ms, 2),
                "dead_letter_depth": dlq_depth,
                "outbox_backlog": outbox_backlog,
                "saga_stuck_count": saga_stuck,
            })

        elif uri.startswith("ledger://applications") and "/audit-trail" not in uri and "/compliance" not in uri:
            if uri == "ledger://applications" or "?" in uri:
                status_filter = None
                limit = 50
                offset = 0
                if "?" in uri:
                    for part in uri.split("?", 1)[1].split("&"):
                        if "=" in part:
                            k, v = part.split("=", 1)
                            if k == "status":
                                status_filter = v
                            elif k == "limit":
                                limit = int(v)
                            elif k == "offset":
                                offset = int(v)
                data = await list_applications(conn, status=status_filter, limit=limit, offset=offset)
                return _json(data)
            else:
                app_id = UUID(uri.split("/")[-1])
                data = await get_application_summary(conn, app_id)
                return _json(data or {"error": "Not found"})

        elif uri.endswith("/audit-trail"):
            app_id = UUID(uri.split("/")[-2])
            events = await event_store.load_stream("LoanApplication", app_id)
            return _json([
                {
                    "event_type": e.event_type,
                    "occurred_at": e.occurred_at.isoformat() if e.occurred_at else None,
                    "payload": e.to_payload(),
                    "schema_version": e.schema_version,
                }
                for e in events
            ])

        elif uri.endswith("/compliance"):
            app_id = UUID(uri.split("/")[-2])
            return _json(await get_compliance_history(conn, app_id))

        elif uri == "ledger://agents/performance":
            return _json(await list_agent_performance(conn))

        elif uri == "ledger://integrity/alerts":
            if integrity_monitor:
                alerts = await integrity_monitor.get_open_alerts()
            else:
                alerts = await conn.fetch(
                    "SELECT * FROM integrity_alerts WHERE resolved_at IS NULL ORDER BY created_at DESC"
                )
                alerts = [dict(r) for r in alerts]
            return _json({"open_alerts": alerts, "count": len(alerts)})

        elif uri.startswith("ledger://integrity/"):
            app_id = UUID(uri.split("/")[-1])
            return _json(await run_integrity_check(pool, event_store, app_id))

        elif uri == "ledger://dead-letters":
            if dead_letter:
                data = await dead_letter.list_unresolved()
                counts = await dead_letter.count_unresolved()
            else:
                data, counts = [], {}
            return _json({"unresolved": data, "counts_by_processor": counts})

        elif uri == "ledger://metrics":
            return _json(get_metrics().snapshot())

        elif uri == "ledger://metrics/prometheus":
            from src.observability.exporters import PrometheusExporter
            text = PrometheusExporter(get_metrics()).export()
            return ReadResourceResult(contents=[TextContent(type="text", text=text)])

        elif uri.startswith("ledger://events/correlation/"):
            corr_id = UUID(uri.split("/")[-1])
            events = await event_store.load_correlation_chain(corr_id)
            return _json([
                {"event_type": e.event_type, "aggregate_type": e.aggregate_type,
                 "aggregate_id": str(e.aggregate_id), "global_position": e.global_position,
                 "event_id": str(e.event_id)}
                for e in events
            ])

        elif uri.startswith("ledger://events/causation/"):
            root_id = UUID(uri.split("/")[-1])
            events = await event_store.load_causation_chain(root_id)
            return _json([
                {"event_type": e.event_type, "aggregate_type": e.aggregate_type,
                 "aggregate_id": str(e.aggregate_id), "global_position": e.global_position,
                 "event_id": str(e.event_id), "causation_id": str(e.causation_id)}
                for e in events
            ])

        elif uri == "ledger://saga/status":
            rows = await conn.fetch(
                """
                SELECT saga_id, application_id, step, compliance_record_id,
                       retry_count, created_at, updated_at
                FROM saga_instances
                ORDER BY updated_at DESC
                LIMIT 100
                """
            )
            stuck = [r for r in rows if r["step"] == "failed"]
            return _json({
                "total": len(rows),
                "stuck_count": len(stuck),
                "instances": [dict(r) for r in rows],
            })

        elif uri == "ledger://auth/audit":
            rows = await conn.fetch(
                """
                SELECT audit_id, event_type, agent_id, token_id,
                       failure_reason, occurred_at
                FROM auth_audit_log
                ORDER BY occurred_at DESC
                LIMIT 200
                """
            )
            return _json([dict(r) for r in rows])

        else:
            return _json({"error": f"Unknown resource: {uri}"})
