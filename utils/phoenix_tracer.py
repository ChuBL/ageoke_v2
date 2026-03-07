"""Arize Phoenix instrumentation for MCP and AI calls."""

import atexit

from utils.config import settings


def setup_phoenix():
    """
    Initialize Phoenix tracing only when PHOENIX_COLLECTOR_ENDPOINT is configured.
    Returns the tracer_provider so callers can force_flush/shutdown explicitly.
    Also registers an atexit handler for graceful teardown on normal exit.
    No-op (returns None) if the endpoint is not set.
    """
    if not settings.phoenix_collector_endpoint:
        return None

    from phoenix.otel import register

    # register() only auto-appends /v1/traces when endpoint=None (env-based discovery).
    # When an explicit endpoint is passed it is used verbatim, so we must include
    # the full OTLP path ourselves.
    base = settings.phoenix_collector_endpoint.rstrip("/")
    otlp_endpoint = base if base.endswith("/v1/traces") else f"{base}/v1/traces"

    kwargs = dict(
        project_name="ageoke_trace_lunar",
        endpoint=otlp_endpoint,
        protocol="http/protobuf",
        auto_instrument=True,
    )
    if settings.phoenix_api_key:
        kwargs["api_key"] = settings.phoenix_api_key

    tracer_provider = register(**kwargs)

    def _shutdown():
        tracer_provider.force_flush(timeout_millis=30_000)
        tracer_provider.shutdown()

    atexit.register(_shutdown)
    return tracer_provider