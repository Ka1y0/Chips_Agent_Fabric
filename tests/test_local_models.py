from __future__ import annotations

import pytest

from project_supervisor.local_models import (
    LocalModelObservation,
    LocalModelRole,
    LocalModelRuntime,
    MemoryPressure,
    adapt_local_model,
    detect_runtime_candidates,
)

GIB = 1024**3


def test_discovery_maps_installed_runtimes_to_loopback_probe_candidates() -> None:
    candidates = detect_runtime_candidates(
        {
            "resources": {
                "modelServers": {
                    "lms": "/bin/lms",
                    "lmstudio": None,
                    "ollama": "/bin/ollama",
                    "llama-server": None,
                },
                "localInferenceApplications": {"lmStudio": "/Applications/LM Studio.app"},
            }
        }
    )

    assert [item.runtime for item in candidates] == [
        LocalModelRuntime.LM_STUDIO,
        LocalModelRuntime.OLLAMA,
    ]
    assert all(item.endpoint.startswith("http://127.0.0.1:") for item in candidates)
    assert all(item.probe_required for item in candidates)


def test_high_memory_pressure_keeps_parallelism_one_and_caps_context() -> None:
    profile = adapt_local_model(
        LocalModelObservation(
            LocalModelRuntime.LM_STUDIO,
            "local-27b",
            context_window_tokens=32_768,
            supports_reasoning=True,
            supports_tools=True,
            measured_tokens_per_second=33.0,
            loaded_memory_bytes=15 * GIB,
            max_parallel=4,
        ),
        host_vram_bytes=16 * GIB,
        requested_context_tokens=32_768,
    )

    assert profile.memory_pressure is MemoryPressure.HIGH
    assert profile.context_tokens == 8_192
    assert profile.parallelism == 1
    assert profile.reasoning_mode == "onDemand"
    assert LocalModelRole.DEEP_REASONING in profile.roles
    assert "toolCalling" in profile.capabilities


def test_fast_low_pressure_model_can_gain_bounded_parallelism_and_vision_role() -> None:
    profile = adapt_local_model(
        LocalModelObservation(
            LocalModelRuntime.LLAMA_CPP,
            "vision-fast",
            context_window_tokens=16_384,
            supports_vision=True,
            measured_tokens_per_second=52.0,
            loaded_memory_bytes=6 * GIB,
            max_parallel=4,
        ),
        host_vram_bytes=16 * GIB,
    )

    assert profile.memory_pressure is MemoryPressure.LOW
    assert profile.parallelism == 2
    assert LocalModelRole.FAST_GENERAL in profile.roles
    assert LocalModelRole.VISION_GROUNDING in profile.roles
    assert {"vision", "vision.grounding", "latency.fast"} <= profile.capabilities


def test_unknown_capacity_never_invents_parallel_headroom() -> None:
    profile = adapt_local_model(
        LocalModelObservation(
            LocalModelRuntime.OLLAMA,
            "unknown-capacity",
            measured_tokens_per_second=100.0,
            max_parallel=None,
        )
    )

    assert profile.memory_pressure is MemoryPressure.UNKNOWN
    assert profile.parallelism == 1


def test_invalid_observations_fail_closed() -> None:
    with pytest.raises(ValueError, match="context_window_tokens"):
        LocalModelObservation(LocalModelRuntime.LM_STUDIO, "model", context_window_tokens=0)
