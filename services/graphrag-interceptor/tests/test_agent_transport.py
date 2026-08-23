"""Transport selection: two backends behind one contract, one vendor surface.

The deterministic backend is what every other test in this suite exercises.
These cover the seam around it — that the offline default is genuinely the
default, that the live backend is selected and configured by settings alone,
and that ``CloudToolClient`` translates the contract faithfully. A fake SDK
stands in for the vendor client, so the live path is covered with no network
call, no key and no SDK installed.
"""

from __future__ import annotations

import asyncio
import sys
import types
from typing import Any

import pytest

from graphrag_interceptor import (
    DETERMINISTIC_MODEL_ID,
    AgentExecutionError,
    CloudToolClient,
    DeterministicReasoningClient,
    InterceptorSettings,
    build_agent_client,
)

TOOL: dict[str, Any] = {"name": "emit_topology_verdict"}

#: The SDK module CloudToolClient imports. It appears here for the same reason
#: it appears there and nowhere else: monkeypatching has to use the real
#: distribution name.
PROVIDER_SDK_MODULE = "anthropic"


def test_the_default_transport_is_offline_and_names_no_vendor_model():
    settings = InterceptorSettings()

    assert settings.agent_provider == "deterministic"
    assert settings.agent_model == DETERMINISTIC_MODEL_ID
    assert isinstance(build_agent_client(settings), DeterministicReasoningClient)


def test_the_live_transport_is_selected_by_settings_alone(fake_sdk):
    fake_sdk(_response({"verdict": "contained"}))

    client = build_agent_client(
        InterceptorSettings(
            agent_provider="cloud",
            agent_model="some-provider-model-v1",
            agent_api_key="test-key",
        )
    )

    assert isinstance(client, CloudToolClient)
    assert client.provider == "cloud"
    assert client.model == "some-provider-model-v1"


def test_the_live_transport_refuses_to_start_without_a_key():
    settings = InterceptorSettings(
        agent_provider="cloud",
        agent_model="some-provider-model-v1",
    )

    with pytest.raises(AgentExecutionError, match="OO_GRAPHRAG_AGENT_API_KEY"):
        build_agent_client(settings)


def test_the_live_adapter_forces_the_tool_call_and_reports_real_usage(fake_sdk):
    sdk = fake_sdk(_response({"verdict": "contained"}))
    client = CloudToolClient(api_key="test-key", model="some-provider-model-v1", max_tokens=4096)

    result = asyncio.run(
        client.emit(agent="topology", system="sys", briefing={"asset": "PUMP-1"}, tool=TOOL)
    )

    assert result.payload == {"verdict": "contained"}
    assert (result.input_tokens, result.output_tokens) == (128, 64)
    assert sdk.messages.request["tool_choice"] == {"type": "tool", "name": TOOL["name"]}
    assert sdk.messages.request["max_tokens"] == 4096


def test_the_live_adapter_surfaces_a_missing_tool_call(fake_sdk):
    fake_sdk(_Block(stop_reason="max_tokens", content=[_Block(type="text", text="...")]))
    client = CloudToolClient(api_key="test-key", model="some-provider-model-v1", max_tokens=4096)

    with pytest.raises(AgentExecutionError, match="did not call emit_topology_verdict"):
        asyncio.run(
            client.emit(agent="topology", system="sys", briefing={}, tool=TOOL)
        )


# ---------------------------------------------------------------------------
# Vendor stand-in
# ---------------------------------------------------------------------------


class _Block:
    def __init__(self, **attrs: Any) -> None:
        self.__dict__.update(attrs)


class _FakeMessages:
    def __init__(self, response: Any) -> None:
        self._response = response
        self.request: dict[str, Any] | None = None

    async def create(self, **kwargs: Any) -> Any:
        self.request = kwargs
        return self._response


class _FakeSDKClient:
    def __init__(self, response: Any) -> None:
        self.messages = _FakeMessages(response)


def _response(payload: dict[str, Any]) -> Any:
    return _Block(
        stop_reason="tool_use",
        content=[_Block(type="tool_use", name=TOOL["name"], input=payload)],
        usage=_Block(input_tokens=128, output_tokens=64),
    )


@pytest.fixture()
def fake_sdk(monkeypatch: pytest.MonkeyPatch):
    """Stand in for the vendor SDK so the live adapter runs offline."""

    def install(response: Any) -> _FakeSDKClient:
        client = _FakeSDKClient(response)
        module = types.ModuleType(PROVIDER_SDK_MODULE)
        module.AsyncAnthropic = lambda **_: client  # type: ignore[attr-defined]
        monkeypatch.setitem(sys.modules, PROVIDER_SDK_MODULE, module)
        return client

    return install
