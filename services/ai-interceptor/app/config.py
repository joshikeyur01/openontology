"""Runtime configuration for the commercial interceptor.

Every value is environment driven (prefix ``OO_``) so the same image runs
unchanged from a laptop to a regulated production estate.
"""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Any, Literal

from pydantic import Field, SecretStr, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

EffortLevel = Literal["low", "medium", "high", "xhigh", "max"]

#: Schemes redis-py accepts in a connection URL.
REDIS_URL_SCHEMES = ("redis://", "rediss://", "unix://")

#: Model id reported while the offline provider is planning. No vendor's model
#: id is shipped as a default: it would be wrong for every provider except the
#: one that owns it, and a wrong default is worse than an absent one. Running
#: against a real provider means naming a model that provider actually serves.
MOCK_MODEL_ID = "openontology-mock-planner"


class Settings(BaseSettings):
    """Immutable process configuration."""

    model_config = SettingsConfigDict(
        env_prefix="OO_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        frozen=True,
    )

    # --- service identity -------------------------------------------------
    service_name: str = "openontology-ai-interceptor"
    environment: str = "local"
    log_level: str = "INFO"
    docs_enabled: bool = True

    # --- commercial licensing --------------------------------------------
    license_header: str = "X-License-Key"
    license_registry_path: Path | None = None
    rate_limit_window_seconds: int = Field(default=60, ge=1, le=3600)

    # --- LLM orchestration ------------------------------------------------
    #: ``mock`` is the deterministic in-process planner: no network, no key, no
    #: SDK. ``cloud`` routes the same call through a real tool-calling provider
    #: via app/llm_cloud.py, which is the only module that knows which vendor
    #: that is.
    llm_provider: Literal["mock", "cloud"] = "mock"
    llm_model: str = MOCK_MODEL_ID
    llm_max_tokens: int = Field(default=4096, ge=256, le=64000)
    llm_effort: EffortLevel = "medium"
    llm_timeout_seconds: float = Field(default=30.0, gt=0)
    llm_simulated_latency_ms: int = Field(default=40, ge=0, le=5_000)
    llm_api_key: SecretStr | None = None

    # --- plan shaping -----------------------------------------------------
    max_commands: int = Field(default=6, ge=1, le=20)
    idempotency_cache_size: int = Field(default=512, ge=0, le=100_000)

    # --- shared state -----------------------------------------------------
    # Two pieces of this service's state are correctness state, not caches: the
    # licence quota and the (tenant, event_id) idempotency record. Held in
    # process memory they are correct for exactly one worker and silently wrong
    # for two — quotas multiply by replica count and duplicate mutations get
    # re-planned and re-billed. Pointing OO_REDIS_URL at the same Redis the
    # resolution engine uses moves both into shared state; leaving it unset
    # keeps the process-local implementations, which is fine for tests and a
    # single-worker laptop run and nothing else.
    redis_url: str | None = None
    #: Refuse to start without a reachable Redis. Set it wherever the process
    #: runs more than one worker, so a deployment cannot quietly regress to
    #: per-process quotas.
    require_shared_state: bool = False
    #: Namespaced away from the engine's twin:, twinindex:, twinalarm: and
    #: dedupe: keys so both services can share one Redis without collision.
    quota_key_prefix: str = "quota:"
    plan_key_prefix: str = "plan:"
    redis_op_timeout_seconds: float = Field(default=2.0, gt=0, le=30)
    redis_pool_size: int = Field(default=16, ge=1, le=512)
    #: Lifetime of a stored plan. Longer than any realistic Kafka redelivery
    #: gap, shorter than the mutations topic's 30 day retention: replaying the
    #: archive a week later is a new decision, not a duplicate.
    idempotency_ttl_seconds: int = Field(default=86_400, ge=60, le=604_800)
    #: Failure policy when Redis is unreachable. Metering is availability
    #: first (admit and under-bill), idempotency is safety first (refuse and
    #: let the caller retry). See app/redis_state.py and app/idempotency.py
    #: for the full argument.
    rate_limit_fail_open: bool = True
    idempotency_fail_open: bool = False

    @field_validator("redis_url")
    @classmethod
    def _normalise_redis_url(cls, value: str | None) -> str | None:
        if value is None:
            return None
        url = value.strip()
        if not url:
            return None
        if not url.startswith(REDIS_URL_SCHEMES):
            raise ValueError(f"redis_url must start with one of {list(REDIS_URL_SCHEMES)}")
        return url

    @field_validator("quota_key_prefix", "plan_key_prefix")
    @classmethod
    def _non_empty_prefix(cls, value: str) -> str:
        prefix = value.strip()
        if not prefix:
            raise ValueError("redis key prefixes must not be empty")
        return prefix

    @field_validator("llm_model")
    @classmethod
    def _non_empty_model(cls, value: str) -> str:
        model = value.strip()
        if not model:
            raise ValueError("llm_model must not be empty")
        return model

    @field_validator("log_level")
    @classmethod
    def _normalise_log_level(cls, value: str) -> str:
        level = value.strip().upper()
        allowed = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
        if level not in allowed:
            raise ValueError(f"log_level must be one of {sorted(allowed)}")
        return level

    def model_post_init(self, __context: Any) -> None:
        if self.require_shared_state and not self.redis_url:
            raise ValueError(
                "OO_REQUIRE_SHARED_STATE is set but OO_REDIS_URL is empty: the quota "
                "and idempotency records would be process-local, which is incorrect "
                "for more than one worker"
            )
        if self.quota_key_prefix == self.plan_key_prefix:
            raise ValueError("quota_key_prefix and plan_key_prefix must differ")
        if self.require_live_llm() and self.llm_model == MOCK_MODEL_ID:
            raise ValueError(
                "OO_LLM_PROVIDER=cloud requires OO_LLM_MODEL to name a model the "
                "configured provider serves; it is still the offline default "
                f"({MOCK_MODEL_ID!r})"
            )

    def require_live_llm(self) -> bool:
        """True when a live provider client must be constructed."""
        return self.llm_provider == "cloud"

    def shared_state_enabled(self) -> bool:
        """True when quota and idempotency state live in Redis."""
        return bool(self.redis_url)

    def state_backend(self) -> str:
        """Name of the active backend, surfaced on the health endpoints."""
        return "redis" if self.shared_state_enabled() else "memory"


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Process-wide settings singleton."""
    return Settings()
