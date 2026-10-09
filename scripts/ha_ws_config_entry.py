"""Use Home Assistant's WebSocket API without exposing an access token.

The token is read from stdin and is never included in output, arguments, or
environment variables.  This helper is intended for controlled deployment
checks from inside the Home Assistant container, where aiohttp is available.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from typing import Any

import aiohttp


WEBSOCKET_URL = "ws://supervisor/core/websocket"


def _emit(payload: dict[str, Any]) -> None:
    print(json.dumps(payload, sort_keys=True, separators=(",", ":")))


async def _authenticate(
    session: aiohttp.ClientSession, token: str
) -> aiohttp.ClientWebSocketResponse:
    websocket = await session.ws_connect(WEBSOCKET_URL, timeout=15)
    auth_required = await websocket.receive_json()
    if auth_required.get("type") != "auth_required":
        await websocket.close()
        raise RuntimeError("unexpected_auth_handshake")
    await websocket.send_json({"type": "auth", "access_token": token})
    auth_result = await websocket.receive_json()
    if auth_result.get("type") != "auth_ok":
        await websocket.close()
        raise RuntimeError("authentication_failed")
    return websocket


async def _run(action: str, entry_id: str | None, token: str) -> int:
    async with aiohttp.ClientSession() as session:
        websocket = await _authenticate(session, token)
        try:
            if action == "probe":
                await websocket.send_json({"id": 1, "type": "auth/current_user"})
                response = await websocket.receive_json()
                result = response.get("result") or {}
                _emit(
                    {
                        "action": "probe",
                        "is_admin": bool(result.get("is_admin")),
                        "is_owner": bool(result.get("is_owner")),
                        "success": response.get("success") is True,
                    }
                )
                return 0 if response.get("success") is True else 4

            if not entry_id:
                raise RuntimeError("entry_id_required")
            await websocket.send_json(
                {
                    "id": 2,
                    "type": "config_entries/disable",
                    "entry_id": entry_id,
                    "disabled_by": (
                        None if action == "enable" else "user"
                    ),
                }
            )
            response = await websocket.receive_json()
            result = response.get("result") or {}
            _emit(
                {
                    "action": action,
                    "require_restart": bool(result.get("require_restart")),
                    "success": response.get("success") is True,
                }
            )
            return 0 if response.get("success") is True else 5
        finally:
            await websocket.close()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=("probe", "enable", "disable"))
    parser.add_argument("--entry-id")
    args = parser.parse_args()
    token = sys.stdin.read().strip()
    if not token:
        _emit({"error": "missing_token", "success": False})
        return 2
    try:
        return asyncio.run(_run(args.action, args.entry_id, token))
    except Exception as exc:  # Do not emit exception text; it can contain data.
        _emit({"error_type": type(exc).__name__, "success": False})
        return 3


if __name__ == "__main__":
    raise SystemExit(main())
