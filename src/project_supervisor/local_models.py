"""Provider-neutral local LLM discovery and conservative control profiles.

The module does not contact model servers. It converts observed runtime/model facts
into bounded, explainable defaults that a later probe or operator may apply.
"""

from __future__ import annotations

import math
from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum
from typing import Any


class LocalModelRuntime(StrEnum):
    LM_STUDIO = "lmStudio"
    OLLAMA = "ollama"
    LLAMA_CPP = "llamaCpp"
    OPENAI_COMPATIBLE = "openAICompatible"


class LocalModelRole(StrEnum):
    GENERAL = "general"
    FAST_GENERAL = "fastGeneral"
    DEEP_REASONING = "deepReasoning"
    VISION_GROUNDING = "visionGrounding"
    LONG_CONTEXT = "longContext"
    TOOL_ROUTER = "toolRouter"


class MemoryPressure(StrEnum):
    LOW = "low"
    MODERATE = "moderate"
    HIGH = "high"
    CRITICAL = "critical"
    UNKNOWN = "unknown"


@dataclass(frozen=True, slots=True)
class LocalRuntimeCandidate:
    runtime: LocalModelRuntime
    endpoint: str
    evidence: str
    probe_required: bool = True

    def to_protocol(self) -> dict[str, object]:
        return {
            "runtime": self.runtime.value,
            "endpoint": self.endpoint,
            "evidence": self.evidence,
            "probeRequired": self.probe_required,
        }


@dataclass(frozen=True, slots=True)
class LocalModelObservation:
    runtime: LocalModelRuntime
    model_id: str
    context_window_tokens: int | None = None
    supports_vision: bool = False
    supports_tools: bool = False
    supports_reasoning: bool = False
    measured_tokens_per_second: float | None = None
    measured_ttft_seconds: float | None = None
    loaded_memory_bytes: int | None = None
    max_parallel: int | None = None

    def __post_init__(self) -> None:
        if not self.model_id.strip():
            raise ValueError("model_id must not be empty")
        for name in ("context_window_tokens", "loaded_memory_bytes", "max_parallel"):
            value = getattr(self, name)
            if value is not None and value <= 0:
                raise ValueError(f"{name} must be positive when known")
        for name in ("measured_tokens_per_second", "measured_ttft_seconds"):
            value = getattr(self, name)
            if value is not None and (value < 0 or not math.isfinite(value)):
                raise ValueError(f"{name} must be a finite non-negative value")


@dataclass(frozen=True, slots=True)
class LocalModelControlProfile:
    runtime: LocalModelRuntime
    model_id: str
    context_tokens: int
    parallelism: int
    memory_pressure: MemoryPressure
    gpu_offload_policy: str
    reasoning_mode: str
    roles: tuple[LocalModelRole, ...]
    capabilities: frozenset[str]
    health_checks: tuple[str, ...]
    explanation: tuple[str, ...]

    def to_protocol(self) -> dict[str, object]:
        return {
            "schemaVersion": 1,
            "runtime": self.runtime.value,
            "modelID": self.model_id,
            "contextTokens": self.context_tokens,
            "parallelism": self.parallelism,
            "memoryPressure": self.memory_pressure.value,
            "gpuOffloadPolicy": self.gpu_offload_policy,
            "reasoningMode": self.reasoning_mode,
            "roles": [role.value for role in self.roles],
            "capabilities": sorted(self.capabilities),
            "healthChecks": list(self.health_checks),
            "explanation": list(self.explanation),
            "adaptiveMutation": False,
        }


def _mapping(value: object) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def detect_runtime_candidates(profile: Mapping[str, Any]) -> tuple[LocalRuntimeCandidate, ...]:
    """Map read-only bootstrap discovery facts to loopback probe candidates."""

    resources = _mapping(profile.get("resources"))
    servers = _mapping(resources.get("modelServers"))
    applications = _mapping(resources.get("localInferenceApplications"))
    result: list[LocalRuntimeCandidate] = []

    if servers.get("lms") or servers.get("lmstudio") or applications.get("lmStudio"):
        result.append(
            LocalRuntimeCandidate(
                LocalModelRuntime.LM_STUDIO,
                "http://127.0.0.1:1234/v1",
                "LM Studio application or CLI discovered",
            )
        )
    if servers.get("ollama"):
        result.append(
            LocalRuntimeCandidate(
                LocalModelRuntime.OLLAMA,
                "http://127.0.0.1:11434",
                "Ollama CLI discovered",
            )
        )
    if servers.get("llama-server"):
        result.append(
            LocalRuntimeCandidate(
                LocalModelRuntime.LLAMA_CPP,
                "http://127.0.0.1:8080/v1",
                "llama-server executable discovered",
            )
        )
    return tuple(result)


def _memory_pressure(
    *,
    loaded_memory_bytes: int | None,
    host_vram_bytes: int | None,
    host_ram_bytes: int | None,
) -> MemoryPressure:
    capacity = host_vram_bytes or host_ram_bytes
    if loaded_memory_bytes is None or capacity is None or capacity <= 0:
        return MemoryPressure.UNKNOWN
    ratio = loaded_memory_bytes / capacity
    if ratio >= 0.95:
        return MemoryPressure.CRITICAL
    if ratio >= 0.85:
        return MemoryPressure.HIGH
    if ratio >= 0.65:
        return MemoryPressure.MODERATE
    return MemoryPressure.LOW


def adapt_local_model(
    observation: LocalModelObservation,
    *,
    host_ram_bytes: int | None = None,
    host_vram_bytes: int | None = None,
    requested_context_tokens: int | None = None,
) -> LocalModelControlProfile:
    """Return conservative defaults from verified observations, never hidden tuning."""

    if requested_context_tokens is not None and requested_context_tokens <= 0:
        raise ValueError("requested_context_tokens must be positive")
    if host_ram_bytes is not None and host_ram_bytes <= 0:
        raise ValueError("host_ram_bytes must be positive")
    if host_vram_bytes is not None and host_vram_bytes <= 0:
        raise ValueError("host_vram_bytes must be positive")

    advertised_context = observation.context_window_tokens or 8_192
    desired_context = requested_context_tokens or min(8_192, advertised_context)
    context_tokens = min(desired_context, advertised_context)
    pressure = _memory_pressure(
        loaded_memory_bytes=observation.loaded_memory_bytes,
        host_vram_bytes=host_vram_bytes,
        host_ram_bytes=host_ram_bytes,
    )
    explanation: list[str] = []

    pressure_caps = {
        MemoryPressure.CRITICAL: 4_096,
        MemoryPressure.HIGH: 8_192,
        MemoryPressure.MODERATE: 16_384,
    }
    cap = pressure_caps.get(pressure)
    if cap is not None and context_tokens > cap:
        context_tokens = cap
        explanation.append(
            f"context capped at {cap} tokens because observed memory pressure is {pressure.value}"
        )
    if desired_context > advertised_context:
        explanation.append("requested context was capped by the model-advertised context window")

    max_parallel = observation.max_parallel or 1
    parallelism = 1
    speed = observation.measured_tokens_per_second
    if pressure in {MemoryPressure.LOW, MemoryPressure.UNKNOWN} and speed is not None:
        if speed >= 45 and max_parallel >= 4:
            parallelism = 4
        elif speed >= 20 and max_parallel >= 2:
            parallelism = 2
    if observation.supports_vision:
        parallelism = min(parallelism, 2)
    parallelism = min(parallelism, max_parallel)
    if parallelism == 1:
        explanation.append("parallelism remains one until capacity and throughput support more")
    else:
        explanation.append(f"parallelism increased to {parallelism} from measured throughput")

    if host_vram_bytes is None:
        gpu_policy = "unknown"
    elif pressure in {MemoryPressure.HIGH, MemoryPressure.CRITICAL}:
        gpu_policy = "conservative"
    else:
        gpu_policy = "preferFullOffload"

    roles: set[LocalModelRole] = {LocalModelRole.GENERAL}
    capabilities = {"chat", "local.inference"}
    if speed is not None and speed >= 20:
        roles.add(LocalModelRole.FAST_GENERAL)
        capabilities.add("latency.fast")
    if observation.supports_reasoning:
        roles.add(LocalModelRole.DEEP_REASONING)
        capabilities.add("reasoning")
    if observation.supports_vision:
        roles.add(LocalModelRole.VISION_GROUNDING)
        capabilities.update({"vision", "vision.grounding"})
    if advertised_context >= 32_768:
        roles.add(LocalModelRole.LONG_CONTEXT)
        capabilities.add("longContext")
    if observation.supports_tools:
        roles.add(LocalModelRole.TOOL_ROUTER)
        capabilities.add("toolCalling")

    reasoning_mode = "onDemand" if observation.supports_reasoning else "unsupported"
    health_checks = ["models.list", "chat.smoke", "latency.sample"]
    if observation.supports_tools:
        health_checks.append("tool-call.smoke")
    if observation.supports_vision:
        health_checks.append("vision.smoke")

    return LocalModelControlProfile(
        runtime=observation.runtime,
        model_id=observation.model_id,
        context_tokens=context_tokens,
        parallelism=parallelism,
        memory_pressure=pressure,
        gpu_offload_policy=gpu_policy,
        reasoning_mode=reasoning_mode,
        roles=tuple(sorted(roles, key=lambda role: role.value)),
        capabilities=frozenset(capabilities),
        health_checks=tuple(health_checks),
        explanation=tuple(explanation),
    )
