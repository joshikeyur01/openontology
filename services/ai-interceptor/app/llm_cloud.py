"""The live provider adapter — the only module here bound to a vendor.

Everything above this file speaks :class:`~app.llm.LLMProvider`: one request,
one forced tool call, one structured answer. This module translates that
contract into one specific tool-calling HTTP API and translates the reply
back. Supporting a different provider means writing a sibling of this file and
pointing ``OO_LLM_PROVIDER`` at it — no other module changes, because no other
module knows a vendor exists.

Two details are deliberately kept on this side of the boundary rather than in
the generic contract, because they are extensions rather than universals: the
prompt-cache breakpoint on the byte-stable system prompt, and the reasoning
effort hint. A provider that does not offer them still satisfies the protocol.

The SDK import is deferred to construction time so the service stays
importable, and the offline provider stays usable, on a host with no SDK
installed and no network egress.
"""

from __future__ import annotations

import logging
from typing import Any

from .llm import PlannerError, ToolCall

logger = logging.getLogger(__name__)


class CloudLLMClient:
    """Live backend: one forced tool call per planning request."""

    name = "cloud"

    def __init__(self, api_key: str | None = None) -> None:
        try:
            from anthropic import AsyncAnthropic  # noqa: PLC0415 - optional dependency
        except ImportError as exc:  # pragma: no cover - depends on the install
            raise RuntimeError(
                "OO_LLM_PROVIDER=cloud requires the provider SDK listed in "
                "requirements.txt; install it or set OO_LLM_PROVIDER=mock."
            ) from exc

        # An unset key is not an error here: the SDK reads the vendor's own
        # environment variable, which is how a managed runtime usually injects
        # it. A missing credential surfaces as an auth failure on first call.
        self._client: Any = AsyncAnthropic(api_key=api_key) if api_key else AsyncAnthropic()

    async def complete(
        self,
        *,
        model: str,
        system: str,
        user: str,
        tool: dict[str, Any],
        tool_name: str,
        max_tokens: int,
        effort: str,
    ) -> ToolCall:
        response = await self._client.messages.create(
            model=model,
            max_tokens=max_tokens,
            system=[
                {
                    "type": "text",
                    "text": system,
                    # The system prompt is byte-stable across requests, so it is
                    # the natural cache breakpoint for a high-volume tenant.
                    "cache_control": {"type": "ephemeral"},
                }
            ],
            messages=[{"role": "user", "content": user}],
            tools=[tool],
            tool_choice={"type": "tool", "name": tool_name},
            output_config={"effort": effort},
        )

        stop_reason = getattr(response, "stop_reason", None)
        if stop_reason == "refusal":
            raise PlannerError("planner declined to produce a plan for this payload")

        block = _find_tool_use(response, tool_name)
        if block is None:
            raise PlannerError(
                f"planner returned stop_reason={stop_reason!r} without a {tool_name} tool call"
            )

        tool_input = block.input if hasattr(block, "input") else block["input"]
        if not isinstance(tool_input, dict):
            raise PlannerError(f"planner returned a non-object input for {tool_name}")

        usage = getattr(response, "usage", None)
        return ToolCall(
            tool_input=tool_input,
            # The provider is authoritative on which model actually answered:
            # an alias can resolve to a different build than the one requested.
            model=str(getattr(response, "model", None) or model),
            input_tokens=int(getattr(usage, "input_tokens", 0) or 0),
            output_tokens=int(getattr(usage, "output_tokens", 0) or 0),
        )


def _find_tool_use(response: Any, tool_name: str) -> Any | None:
    """Locate the forced tool call, tolerating object or dict content blocks."""
    for block in getattr(response, "content", []) or []:
        block_type = getattr(block, "type", None) or (
            block.get("type") if isinstance(block, dict) else None
        )
        block_name = getattr(block, "name", None) or (
            block.get("name") if isinstance(block, dict) else None
        )
        if block_type == "tool_use" and block_name == tool_name:
            return block
    return None


__all__ = ["CloudLLMClient"]
