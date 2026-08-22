"""The provider seam: any tool-calling backend, no vendor above the adapter.

Two things are worth pinning down here. First, that the planner really is
provider-agnostic — a backend written in this file, with no SDK behind it,
drives it end to end. Second, that the live adapter still translates the
contract correctly, since it is the one code path no other test reaches: a
fake SDK stands in for the vendor client so the translation is exercised
without a network call or a key.
"""

from __future__ import annotations

import sys
import types
from typing import Any

import pytest

from app.config import MOCK_MODEL_ID, Settings
from app.llm import (
    COMMAND_SEQUENCE_TOOL,
    TOOL_NAME,
    MockLLMClient,
    PlannerError,
    RemediationPlanner,
    RemediationPromptContext,
    ToolCall,
    build_planner,
)
from app.models import EnrichedContextPayload

from .test_intercept import CRITICAL_PAYLOAD

#: The SDK module app/llm_cloud.py imports. It appears here for the same
#: reason it appears there and nowhere else: monkeypatching has to use the
#: real distribution name. Pointing the adapter at another provider means
#: changing it in both places and nothing else.
PROVIDER_SDK_MODULE = "anthropic"

pytestmark = pytest.mark.anyio


def _context() -> RemediationPromptContext:
    return RemediationPromptContext(
        payload=EnrichedContextPayload.model_validate(CRITICAL_PAYLOAD),
        tenant="northwind-aerospace",
        tier="ENTERPRISE",
    )


def _planner(provider: Any, *, model: str = "test-model") -> RemediationPlanner:
    return RemediationPlanner(
        provider,
        model=model,
        max_tokens=4096,
        effort="medium",
        timeout_seconds=5.0,
    )


# ---------------------------------------------------------------------------
# The seam itself
# ---------------------------------------------------------------------------


class RecordingProvider:
    """A whole LLM backend in twenty lines, which is the point of the protocol."""

    name = "recording"

    def __init__(self, tool_input: dict[str, Any]) -> None:
        self._tool_input = tool_input
        self.calls: list[dict[str, Any]] = []

    async def complete(self, **kwargs: Any) -> ToolCall:
        self.calls.append(kwargs)
        return ToolCall(
            tool_input=self._tool_input,
            model="recording-model-v1",
            input_tokens=11,
            output_tokens=7,
        )


async def _reference_plan() -> dict[str, Any]:
    """A schema-valid plan, borrowed from the offline provider."""
    context = _context()
    call = await MockLLMClient(simulated_latency_ms=0).complete(
        model="m",
        system=context.render_system(),
        user=context.render_user(),
        tool=COMMAND_SEQUENCE_TOOL,
        tool_name=TOOL_NAME,
        max_tokens=4096,
        effort="medium",
    )
    return call.tool_input


async def test_a_provider_with_no_sdk_behind_it_drives_the_planner() -> None:
    provider = RecordingProvider(await _reference_plan())

    result = await _planner(provider).plan(_context())

    assert result.plan.commands
    # The provider is authoritative on which model answered.
    assert result.model == "recording-model-v1"
    assert (result.usage.input_tokens, result.usage.output_tokens) == (11, 7)

    # The planner hands over a rendered prompt and a forced tool, nothing vendor
    # shaped: no message envelopes, no content blocks, no SDK request dict.
    (call,) = provider.calls
    assert set(call) == {
        "model",
        "system",
        "user",
        "tool",
        "tool_name",
        "max_tokens",
        "effort",
    }
    assert call["tool_name"] == TOOL_NAME
    assert call["model"] == "test-model"


async def test_a_malformed_plan_is_a_planner_error_whatever_the_provider() -> None:
    with pytest.raises(PlannerError, match="malformed command sequence"):
        await _planner(RecordingProvider({"confidence": 2.0})).plan(_context())


async def test_the_offline_provider_is_deterministic() -> None:
    first = await _reference_plan()
    second = await _reference_plan()
    assert first == second


def test_the_default_build_is_offline_and_names_no_vendor_model() -> None:
    settings = Settings()
    assert settings.llm_provider == "mock"
    assert settings.llm_model == MOCK_MODEL_ID
    assert not settings.require_live_llm()
    assert build_planner(settings).model == MOCK_MODEL_ID


def test_cloud_mode_refuses_to_inherit_the_offline_model_id() -> None:
    with pytest.raises(ValueError, match="OO_LLM_MODEL"):
        Settings(llm_provider="cloud")

    settings = Settings(llm_provider="cloud", llm_model="some-provider-model-v1")
    assert settings.require_live_llm()


# ---------------------------------------------------------------------------
# The one adapter that is allowed to know a vendor
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


def _sdk_response(plan: dict[str, Any], *, stop_reason: str = "tool_use") -> Any:
    return _Block(
        model="some-provider-model-v1",
        stop_reason=stop_reason,
        content=[
            _Block(type="text", text="Emitting the remediation command sequence."),
            _Block(type="tool_use", name=TOOL_NAME, input=plan),
        ],
        usage=_Block(input_tokens=1089, output_tokens=632),
    )


async def test_the_live_adapter_forces_the_tool_call_and_reports_real_usage(fake_sdk) -> None:
    from app.llm_cloud import CloudLLMClient

    plan = await _reference_plan()
    sdk = fake_sdk(_sdk_response(plan))

    result = await _planner(CloudLLMClient(api_key="test-key")).plan(_context())

    assert result.plan.commands
    assert result.model == "some-provider-model-v1"
    assert (result.usage.input_tokens, result.usage.output_tokens) == (1089, 632)

    request = sdk.messages.request
    assert request is not None
    assert request["tool_choice"] == {"type": "tool", "name": TOOL_NAME}
    assert request["tools"] == [COMMAND_SEQUENCE_TOOL]
    assert request["max_tokens"] == 4096
    # The byte-stable system prompt carries the cache breakpoint.
    assert request["system"][0]["cache_control"] == {"type": "ephemeral"}


async def test_the_live_adapter_surfaces_a_missing_tool_call(fake_sdk) -> None:
    from app.llm_cloud import CloudLLMClient

    fake_sdk(_Block(model="m", stop_reason="max_tokens", content=[_Block(type="text", text="…")]))

    with pytest.raises(PlannerError, match="without a emit_command_sequence tool call"):
        await _planner(CloudLLMClient(api_key="test-key")).plan(_context())


async def test_the_live_adapter_surfaces_a_refusal(fake_sdk) -> None:
    from app.llm_cloud import CloudLLMClient

    fake_sdk(_Block(model="m", stop_reason="refusal", content=[]))

    with pytest.raises(PlannerError, match="declined to produce a plan"):
        await _planner(CloudLLMClient(api_key="test-key")).plan(_context())
