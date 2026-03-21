"""
Prometheus HTTP scrape endpoint.

Runs a lightweight aiohttp server on METRICS_PORT (default 9090) that exposes
/metrics in Prometheus text format. This is separate from the MCP stdio server
so Prometheus can scrape it independently.

Usage:
    server = MetricsHttpServer(get_metrics())
    await server.start()
    # ... later ...
    await server.stop()

Environment:
    METRICS_PORT  — TCP port to listen on (default 9090)
    METRICS_HOST  — Bind address (default 0.0.0.0)
"""
from __future__ import annotations

import asyncio
import logging
import os
from typing import Optional

logger = logging.getLogger(__name__)


class MetricsHttpServer:
    """
    Minimal HTTP server that serves /metrics for Prometheus scraping.
    Uses only stdlib asyncio — no aiohttp dependency required.
    """

    def __init__(self, metrics, host: str | None = None, port: int | None = None, pool=None):
        self._metrics = metrics
        self._host = host or os.environ.get("METRICS_HOST", "0.0.0.0")
        self._port = port or int(os.environ.get("METRICS_PORT", "9090"))
        self._pool = pool  # asyncpg pool for readiness probe
        self._server: Optional[asyncio.AbstractServer] = None

    async def start(self) -> None:
        self._server = await asyncio.start_server(
            self._handle, self._host, self._port
        )
        logger.info("Prometheus metrics endpoint: http://%s:%d/metrics", self._host, self._port)

    async def stop(self) -> None:
        if self._server:
            self._server.close()
            await self._server.wait_closed()
            logger.info("Metrics HTTP server stopped")

    async def _handle(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        try:
            request_line = await reader.readline()
            # Drain remaining headers
            while True:
                line = await reader.readline()
                if line in (b"\r\n", b"\n", b""):
                    break

            path = request_line.decode().split(" ")[1] if request_line else "/"

            if path == "/metrics":
                from src.observability.exporters import PrometheusExporter
                body = PrometheusExporter(self._metrics).export().encode()
                status = b"200 OK"
                content_type = b"text/plain; version=0.0.4; charset=utf-8"
            elif path == "/health":
                # Real readiness probe: check DB pool
                db_ok = False
                if self._pool:
                    try:
                        async with self._pool.acquire() as conn:
                            db_ok = await conn.fetchval("SELECT 1") == 1
                    except Exception:
                        db_ok = False
                else:
                    db_ok = True  # no pool configured — assume ok
                import json
                health_data = {"status": "ok" if db_ok else "degraded", "db": db_ok}
                body = json.dumps(health_data).encode()
                status = b"200 OK" if db_ok else b"503 Service Unavailable"
                content_type = b"application/json"
            else:
                body = b"Not Found"
                status = b"404 Not Found"
                content_type = b"text/plain"

            response = (
                b"HTTP/1.1 " + status + b"\r\n"
                b"Content-Type: " + content_type + b"\r\n"
                b"Content-Length: " + str(len(body)).encode() + b"\r\n"
                b"Connection: close\r\n"
                b"\r\n" + body
            )
            writer.write(response)
            await writer.drain()
        except Exception as e:
            logger.debug("Metrics HTTP handler error: %s", e)
        finally:
            writer.close()
