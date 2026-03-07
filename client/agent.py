# client/agent.py
"""
MCP client that connects to geo_server.py via stdio transport.

Provides an async context manager interface for external callers (LangGraph
agents, Claude, interactive sessions) that need to call geo_server tools
through the MCP protocol.

For internal programmatic use (e.g., the CLI pipeline), prefer importing
tool functions directly from servers/tools/ — it's simpler and faster.
"""
from contextlib import asynccontextmanager


@asynccontextmanager
async def get_mcp_client():
    """
    Async context manager that starts geo_server.py as a subprocess and
    returns a live MCP ClientSession.

    Usage:
        async with get_mcp_client() as session:
            result = await session.call_tool(
                "ingest_pdf", {"pdf_path": "data/inputs/report.pdf"}
            )
            print(result)

    The server process is started fresh on each context entry and terminated
    on exit. Environment variables (including .env credentials) are inherited
    from the current process.
    """
    from mcp import ClientSession, StdioServerParameters
    from mcp.client.stdio import stdio_client

    server_params = StdioServerParameters(
        command="python",
        args=["-m", "servers.geo_server"],
        env=None,  # Inherit current environment
    )

    async with stdio_client(server_params) as (read, write):
        async with ClientSession(read, write) as session:
            await session.initialize()
            yield session
