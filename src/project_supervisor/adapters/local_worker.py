from __future__ import annotations

import asyncio
import hashlib
import ipaddress
import json
import math
import re
import time
from collections.abc import AsyncIterator, Mapping
from contextlib import asynccontextmanager
from dataclasses import replace
from typing import Any
from urllib.parse import quote, urlparse

import httpx

from project_supervisor.domain import RunState

from .base import (
    EventSink,
    UnsafeWorkerRequest,
    Usage,
    WorkerAdapter,
    WorkerEvent,
    WorkerJobCapabilities,
    WorkerJobHandle,
    WorkerJobIdempotencyConflict,
    WorkerJobLaunchDisposition,
    WorkerJobLaunchObservation,
    WorkerJobLaunchRejected,
    WorkerJobLaunchState,
    WorkerJobObservation,
    WorkerJobOutcomeUncertain,
    WorkerJobState,
    WorkerProtocolError,
    WorkerRequest,
    WorkerResult,
    event_time,
    publish_event,
    request_requires_code_write,
)
from .native import redact

_ALLOWED_ROLES = {
    "FAST_ROUTER",
    "GENERAL_REASONING",
    "RAG",
    "UNCENSORED_REVIEWER",
}

_IDEMPOTENCY_KEY = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,199}$")
_DRIVER_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")
_PROFILE_FINGERPRINT = re.compile(r"^sha256:[0-9a-f]{64}$")
_MAX_HTTP_RESPONSE_BYTES = 2 * 1024 * 1024


def _is_loopback(hostname: str) -> bool:
    if hostname.strip().lower() == "localhost":
        return True
    try:
        return ipaddress.ip_address(hostname).is_loopback
    except ValueError:
        return False


class LocalWorkerAdapter(WorkerAdapter):
    """Negotiated v1/v2 HTTP boundary for the loopback/private Local Worker daemon.

    This adapter never talks directly to LM Studio and has an immutable
    read-only/non-code policy. V2 is enabled only when a durable authority advertises the complete
    launch-registry contract; otherwise the adapter remains truthfully v1. Redirects are disabled
    so a worker cannot turn the adapter into an arbitrary URL proxy.
    """

    ADAPTER_TYPE = "local-worker-http-v1"
    ADAPTER_TYPE_V2 = "local-worker-http-v2"
    JOB_CAPABILITIES = WorkerJobCapabilities(
        supports_reconcile=True,
        supports_resume=True,
        supports_cancel=True,
        # Protocol v1 does not document a provider-side idempotency key.
        supports_provider_idempotency=False,
        supports_stream_reconnect=False,
        # Terminal job representations are retrieved with a repeatable GET.
        supports_repeatable_collect=True,
    )
    JOB_CAPABILITIES_V2 = WorkerJobCapabilities(
        supports_reconcile=True,
        supports_resume=True,
        supports_cancel=True,
        supports_provider_idempotency=True,
        supports_stream_reconnect=False,
        supports_repeatable_collect=True,
        supports_idempotent_launch_lookup=True,
        supports_durable_launch_registry=True,
    )

    def __init__(
        self,
        base_url: str,
        *,
        token: str | None = None,
        driver_id: str | None = None,
        poll_interval_seconds: float = 0.25,
        response_timeout_seconds: float = 10.0,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        parsed = urlparse(base_url)
        if parsed.scheme not in {"http", "https"} or not parsed.hostname:
            raise ValueError("Local Worker base_url must be an absolute HTTP(S) URL")
        if parsed.username or parsed.password or parsed.query or parsed.fragment:
            raise ValueError("Local Worker base_url must not contain credentials or query data")
        if token is not None and not token.strip():
            raise ValueError("Local Worker bearer token must not be empty")
        if driver_id is not None and not _DRIVER_ID.fullmatch(driver_id):
            raise ValueError("Local Worker driver_id must be 1-128 safe ASCII characters")
        if not _is_loopback(parsed.hostname):
            if parsed.scheme != "https":
                raise ValueError("non-loopback Local Worker access requires HTTPS")
            if token is None:
                raise ValueError("non-loopback Local Worker access requires a bearer token")
        self.base_url = base_url.rstrip("/")
        self.token = token
        self._requested_driver_id = driver_id
        if poll_interval_seconds <= 0:
            raise ValueError("Local Worker poll interval must be positive")
        if (
            not isinstance(response_timeout_seconds, (int, float))
            or isinstance(response_timeout_seconds, bool)
            or not math.isfinite(response_timeout_seconds)
            or response_timeout_seconds <= 0
        ):
            raise ValueError("Local Worker response timeout must be a positive finite number")
        self.poll_interval_seconds = poll_interval_seconds
        self.response_timeout_seconds = float(response_timeout_seconds)
        self._injected_client = client
        self._v1_adapter_instance_id = (
            "sha256:" + hashlib.sha256(self.base_url.encode("utf-8")).hexdigest()
        )
        self._adapter_instance_id = self._v1_adapter_instance_id
        self._protocol_version = 1
        self._authority_id: str | None = None
        self._registry_id: str | None = None
        self._node_id: str | None = None
        self._runtime_instance_id: str | None = None
        self._driver_profile: dict[str, Any] | None = None
        self._job_capabilities = self.JOB_CAPABILITIES
        self._contract_observed = False
        self._pending_contract_error: Exception | None = None
        self._contract_lock = asyncio.Lock()
        self._job_handles: dict[str, WorkerJobHandle] = {}
        self._cancelled_runs: set[str] = set()
        self._lock = asyncio.Lock()

    @property
    def adapter_type(self) -> str:
        return self.ADAPTER_TYPE_V2 if self._protocol_version == 2 else self.ADAPTER_TYPE

    @property
    def adapter_instance_id(self) -> str:
        """Stable endpoint identity without disclosing the private endpoint itself."""

        return self._adapter_instance_id

    @property
    def job_capabilities(self) -> WorkerJobCapabilities:
        return self._job_capabilities

    @property
    def protocol_version(self) -> int:
        return self._protocol_version

    @property
    def node_id(self) -> str | None:
        return self._node_id

    @property
    def runtime_instance_id(self) -> str | None:
        return self._runtime_instance_id

    @property
    def driver_id(self) -> str | None:
        if self._driver_profile is not None:
            return str(self._driver_profile["driver_id"])
        return self._requested_driver_id

    @property
    def driver_profile(self) -> Mapping[str, Any] | None:
        return dict(self._driver_profile) if self._driver_profile is not None else None

    async def negotiate_job_contract(self) -> WorkerJobCapabilities:
        """Observe the daemon contract before Supervisor freezes a launch intent.

        V1 remains the conservative default until an authenticated health response proves that
        the same durable authority implements the complete V2 registry contract.
        """

        self._pending_contract_error = None
        async with self._client() as client:
            try:
                capabilities = await self._negotiate_job_contract(client)
            except Exception as error:
                self._reset_v1_contract()
                self._pending_contract_error = error
                raise
        self._contract_observed = True
        return capabilities

    def _reset_v1_contract(self) -> None:
        self._protocol_version = 1
        self._authority_id = None
        self._registry_id = None
        self._node_id = None
        self._runtime_instance_id = None
        self._driver_profile = None
        self._job_capabilities = self.JOB_CAPABILITIES
        self._adapter_instance_id = self._v1_adapter_instance_id
        self._contract_observed = False

    @property
    def _headers(self) -> dict[str, str]:
        headers = {"accept": "application/json", "content-type": "application/json"}
        if self.token:
            headers["authorization"] = f"Bearer {self.token}"
        return headers

    @asynccontextmanager
    async def _client(self) -> AsyncIterator[httpx.AsyncClient]:
        if self._injected_client is not None:
            yield self._injected_client
            return
        client = httpx.AsyncClient(
            base_url=self.base_url,
            headers=self._headers,
            follow_redirects=False,
            trust_env=False,
            timeout=httpx.Timeout(10.0),
        )
        try:
            yield client
        finally:
            await client.aclose()

    async def _request(
        self,
        client: httpx.AsyncClient,
        method: str,
        path: str,
        *,
        json_body: Mapping[str, Any] | None = None,
    ) -> httpx.Response:
        """Read one decoded HTTP response under byte and whole-response deadlines."""

        request = client.build_request(method, path, json=json_body)
        response: httpx.Response | None = None
        try:
            async with asyncio.timeout(self.response_timeout_seconds):
                response = await client.send(request, stream=True)
                content_length = response.headers.get("content-length")
                if content_length is not None:
                    try:
                        declared_length = int(content_length)
                    except ValueError:
                        declared_length = -1
                    if declared_length > _MAX_HTTP_RESPONSE_BYTES:
                        raise WorkerProtocolError(
                            "Local Worker HTTP response exceeded the capture limit"
                        )

                content = bytearray()
                async for chunk in response.aiter_bytes(chunk_size=64 * 1024):
                    if len(content) + len(chunk) > _MAX_HTTP_RESPONSE_BYTES:
                        raise WorkerProtocolError(
                            "Local Worker HTTP response exceeded the capture limit"
                        )
                    content.extend(chunk)
                return httpx.Response(
                    response.status_code,
                    headers=response.headers,
                    content=bytes(content),
                    request=request,
                    extensions=dict(response.extensions),
                )
        except TimeoutError as error:
            raise httpx.ReadTimeout(
                "Local Worker response exceeded its overall deadline",
                request=request,
            ) from error
        finally:
            if response is not None:
                await response.aclose()

    async def _negotiate_job_contract(
        self,
        client: httpx.AsyncClient,
    ) -> WorkerJobCapabilities:
        async with self._contract_lock:
            health = await self._request(client, "GET", "/v1/health")
            try:
                health.raise_for_status()
            except httpx.HTTPStatusError as exc:
                # No launch request has been sent, so this is a provable pre-launch rejection.
                raise WorkerJobLaunchRejected(exc.response.status_code) from exc
            envelope = self._json_object(health, "health")
            data = self._unwrap_data(envelope)
            versions = self._protocol_versions(envelope, data)
            capabilities = data.get("capabilities", envelope.get("capabilities"))
            authority_id = self._string(data, "authority_id", "authorityId") or self._string(
                envelope, "authority_id", "authorityId"
            )
            registry_id = self._string(data, "registry_id", "registryId") or self._string(
                envelope, "registry_id", "registryId"
            )
            node_id = self._string(data, "node_id", "nodeId", "nodeID") or self._string(
                envelope, "node_id", "nodeId", "nodeID"
            )
            runtime_instance_id = self._string(
                data, "runtime_instance_id", "runtimeInstanceId", "runtimeInstanceID"
            ) or self._string(
                envelope, "runtime_instance_id", "runtimeInstanceId", "runtimeInstanceID"
            )
            supports_v2 = (
                2 in versions
                and self._capability(capabilities, "supports_reconcile")
                and self._capability(capabilities, "supports_resume")
                and self._capability(capabilities, "supports_repeatable_collect")
                and self._capability(capabilities, "supports_provider_idempotency")
                and self._capability(capabilities, "supports_idempotent_launch_lookup")
                and self._capability(capabilities, "supports_durable_launch_registry")
            )
            if supports_v2:
                if authority_id is None or not 1 <= len(authority_id) <= 200:
                    raise WorkerProtocolError(
                        "Local Worker v2 did not advertise a bounded durable authority identity"
                    )
                if registry_id is None or not 1 <= len(registry_id) <= 200:
                    raise WorkerProtocolError(
                        "Local Worker v2 did not advertise a bounded durable registry identity"
                    )
                if node_id is not None and not 1 <= len(node_id) <= 200:
                    raise WorkerProtocolError("Local Worker v2 node identity is not bounded")
                if runtime_instance_id is not None and not 1 <= len(runtime_instance_id) <= 200:
                    raise WorkerProtocolError(
                        "Local Worker v2 runtime instance identity is not bounded"
                    )
                driver_profile = self._select_driver_profile(envelope, data, capabilities)
                if driver_profile is not None and (node_id is None or runtime_instance_id is None):
                    raise WorkerProtocolError(
                        "registered Local Worker drivers require node and runtime identities"
                    )
                supports_cancel = self._capability(capabilities, "supports_cancel")
                if driver_profile is not None:
                    supports_cancel = supports_cancel and bool(driver_profile["supports_cancel"])
                self._protocol_version = 2
                self._authority_id = authority_id
                self._registry_id = registry_id
                self._node_id = node_id
                self._runtime_instance_id = runtime_instance_id
                self._driver_profile = driver_profile
                self._job_capabilities = WorkerJobCapabilities(
                    supports_reconcile=True,
                    supports_resume=self._capability(capabilities, "supports_resume"),
                    supports_cancel=supports_cancel,
                    supports_provider_idempotency=True,
                    supports_stream_reconnect=self._capability(
                        capabilities, "supports_stream_reconnect"
                    ),
                    supports_repeatable_collect=True,
                    supports_idempotent_launch_lookup=True,
                    supports_durable_launch_registry=True,
                )
                identity_parts = [self.base_url, authority_id, registry_id]
                if driver_profile is not None:
                    identity_parts.extend(
                        [
                            str(node_id),
                            str(driver_profile["driver_id"]),
                            str(driver_profile["driver_type"]),
                            str(driver_profile["profile_revision"]),
                            str(driver_profile["profile_fingerprint"]),
                        ]
                    )
                identity_material = "\0".join(identity_parts).encode()
                self._adapter_instance_id = (
                    "sha256:" + hashlib.sha256(identity_material).hexdigest()
                )
                return self._job_capabilities
            if self._requested_driver_id is not None:
                raise WorkerProtocolError(
                    "configured Local Worker driver requires the complete protocol-v2 "
                    "registered-driver contract"
                )
            if 1 not in versions:
                raise WorkerProtocolError("Local Worker does not advertise protocol v1 or v2")
            self._reset_v1_contract()
            return self.JOB_CAPABILITIES

    @staticmethod
    def _protocol_versions(envelope: Mapping[str, Any], data: Mapping[str, Any]) -> set[int]:
        values: list[Any] = []
        for mapping in (envelope, data):
            advertised = mapping.get("protocol_versions") or mapping.get("protocolVersions")
            if isinstance(advertised, (list, tuple, set)):
                values.extend(advertised)
            scalar = mapping.get("protocol") or mapping.get("protocol_version")
            if scalar is None:
                scalar = mapping.get("protocolVersion")
            if scalar is not None:
                values.append(scalar)
        versions: set[int] = set()
        for value in values:
            try:
                versions.add(int(str(value).split(".", maxsplit=1)[0]))
            except (TypeError, ValueError):
                continue
        return versions

    @staticmethod
    def _capability(value: Any, snake_name: str) -> bool:
        camel_name = "".join(
            part if index == 0 else part.capitalize()
            for index, part in enumerate(snake_name.split("_"))
        )
        if isinstance(value, Mapping):
            return value.get(snake_name) is True or value.get(camel_name) is True
        if isinstance(value, (list, tuple, set)):
            return snake_name in value or camel_name in value
        return False

    def _select_driver_profile(
        self,
        envelope: Mapping[str, Any],
        data: Mapping[str, Any],
        capabilities: Any,
    ) -> dict[str, Any] | None:
        supports_profiles = self._capability(capabilities, "supports_server_driver_profiles")
        if not supports_profiles:
            if self._requested_driver_id is not None:
                raise WorkerProtocolError(
                    "Local Worker does not advertise registered server-side drivers"
                )
            return None
        raw_profiles = data.get("drivers", envelope.get("drivers"))
        if not isinstance(raw_profiles, list):
            raise WorkerProtocolError("Local Worker driver catalog is malformed")
        default_driver_id = self._string(
            data, "default_driver_id", "defaultDriverId"
        ) or self._string(envelope, "default_driver_id", "defaultDriverId")
        selected_id = self._requested_driver_id or default_driver_id
        if selected_id is None or not _DRIVER_ID.fullmatch(selected_id):
            raise WorkerProtocolError("Local Worker did not advertise a valid default driver")
        profiles: dict[str, dict[str, Any]] = {}
        for raw in raw_profiles:
            if not isinstance(raw, Mapping):
                raise WorkerProtocolError("Local Worker driver catalog entry is malformed")
            driver_id = self._string(raw, "driver_id", "driverId", "driverID")
            driver_type = self._string(raw, "driver_type", "driverType")
            fingerprint = self._string(raw, "profile_fingerprint", "profileFingerprint")
            revision = raw.get("profile_revision", raw.get("profileRevision"))
            available = raw.get("available")
            supports_execution = raw.get("supports_execution", raw.get("supportsExecution"))
            supports_cancel = raw.get("supports_cancel", raw.get("supportsCancel"))
            if (
                driver_id is None
                or not _DRIVER_ID.fullmatch(driver_id)
                or driver_type is None
                or not 1 <= len(driver_type) <= 64
                or fingerprint is None
                or not _PROFILE_FINGERPRINT.fullmatch(fingerprint)
                or not isinstance(revision, int)
                or isinstance(revision, bool)
                or revision < 1
                or not isinstance(available, bool)
                or not isinstance(supports_execution, bool)
                or not isinstance(supports_cancel, bool)
            ):
                raise WorkerProtocolError("Local Worker driver catalog entry is malformed")
            if driver_id in profiles:
                raise WorkerProtocolError("Local Worker driver catalog contains duplicate ids")
            profiles[driver_id] = {
                "driver_id": driver_id,
                "driver_type": driver_type,
                "profile_revision": revision,
                "profile_fingerprint": fingerprint,
                "available": available,
                "supports_execution": supports_execution,
                "supports_cancel": supports_cancel,
            }
        selected = profiles.get(selected_id)
        if selected is None:
            raise WorkerProtocolError("configured Local Worker driver is not registered")
        if not selected["available"] or not selected["supports_execution"]:
            raise WorkerProtocolError("configured Local Worker driver is unavailable")
        return selected

    async def execute(
        self,
        request: WorkerRequest,
        *,
        event_sink: EventSink | None = None,
    ) -> WorkerResult:
        self._validate_read_only(request)  # Refusal 1: public execution boundary.
        self._build_job_payload(request)  # Refusal 2: protocol serialization boundary.
        started_at = event_time()
        events: list[WorkerEvent] = []

        async def capture(event: WorkerEvent) -> None:
            events.append(event)
            await publish_event(event_sink, event)

        handle: WorkerJobHandle | None = None
        try:
            async with self._client() as client:
                handle = await self._start_job(
                    request,
                    idempotency_key=request.run_id,
                    event_sink=capture,
                    client=client,
                )
                result = await self._resume_job(
                    request,
                    handle,
                    event_sink=capture,
                    client=client,
                )
            return replace(result, started_at=started_at, events=tuple(events))
        except WorkerJobLaunchRejected as exc:
            state = self._status_run_state(exc.status_code)
            error = str(redact(str(exc)))
            kind = "workerAuthRequired" if state is RunState.AUTH_REQUIRED else "workerError"
            await self._emit(
                request.run_id,
                kind,
                {"error": error, "status": exc.status_code},
                capture,
            )
        except httpx.HTTPStatusError as exc:
            state = self._status_run_state(exc.response.status_code)
            error = str(redact(str(exc)))
            kind = "workerAuthRequired" if state is RunState.AUTH_REQUIRED else "workerError"
            await self._emit(
                request.run_id,
                kind,
                {"error": error, "status": exc.response.status_code},
                capture,
            )
        except (httpx.HTTPError, WorkerProtocolError) as exc:
            state = RunState.FAILED
            error = str(redact(str(exc)))
            await self._emit(request.run_id, "workerError", {"error": error}, capture)
        finally:
            await self._forget_job(request.run_id, handle)

        ended_at = event_time()
        await self._emit(
            request.run_id,
            "remoteJobExited",
            {"jobId": handle.provider_job_id if handle else None, "state": state.value},
            capture,
        )
        return WorkerResult(
            run_id=request.run_id,
            state=state,
            pid=None,
            exit_code=None,
            started_at=started_at,
            ended_at=ended_at,
            stdout="",
            stderr="",
            final_text="",
            events=tuple(events),
            session_id=handle.provider_session_id if handle else None,
            error=error,
        )

    async def start_job(
        self,
        request: WorkerRequest,
        *,
        idempotency_key: str,
        event_sink: EventSink | None = None,
    ) -> WorkerJobHandle:
        """Create or recover the one canonical job for this execution identity.

        Protocol v2 looks up the durable key before crossing the launch boundary and only sends a
        launch when the healthy registry authoritatively returns ``NOT_SEEN``. Protocol v1 retains
        its historical non-idempotent request shape and advertises that limitation truthfully.
        """

        async with self._client() as client:
            return await self._start_job(
                request,
                idempotency_key=idempotency_key,
                event_sink=event_sink,
                client=client,
            )

    async def _start_job(
        self,
        request: WorkerRequest,
        *,
        idempotency_key: str,
        event_sink: EventSink | None,
        client: httpx.AsyncClient,
    ) -> WorkerJobHandle:
        self._validate_read_only(request)
        payload = self._build_job_payload(request)
        if not idempotency_key.strip():
            raise ValueError("Worker job idempotency_key must not be empty")
        created_at = event_time()
        if self._pending_contract_error is not None:
            raise self._pending_contract_error
        if not self._contract_observed:
            try:
                await self._negotiate_job_contract(client)
            except Exception:
                self._reset_v1_contract()
                raise
            self._contract_observed = True
        await self._emit(
            request.run_id,
            "workerConnected",
            {
                "protocol": self.protocol_version,
                "driverID": self.driver_id,
                "nodeID": self.node_id,
            },
            event_sink,
        )
        if self.protocol_version == 2:
            self._validate_v2_idempotency_key(idempotency_key)
            handle = await self._start_job_v2(
                request,
                payload=self._build_v2_job_payload(request),
                idempotency_key=idempotency_key,
                client=client,
            )
            async with self._lock:
                self._job_handles[request.run_id] = handle
            await self._emit(
                request.run_id,
                "remoteJobStarted",
                {"jobId": handle.provider_job_id, "protocol": 2},
                event_sink,
            )
            return handle

        response = await self._request(client, "POST", "/v1/jobs", json_body=payload)
        # Once POST has crossed the transport boundary, HTTP status alone is not proof that the
        # daemon created no job. Keep the raw exception ambiguous unless a future protocol adds
        # an explicit, documented rejection receipt.
        response.raise_for_status()
        created = self._unwrap_data(self._json_object(response, "job creation"))
        raw_job_id = created.get("id") or created.get("jobId") or created.get("job_id")
        if not isinstance(raw_job_id, str) or not raw_job_id.strip():
            raise WorkerProtocolError("Local Worker did not return a job id")
        handle = WorkerJobHandle(
            run_id=request.run_id,
            adapter_type=self.adapter_type,
            adapter_instance_id=self.adapter_instance_id,
            provider_job_id=raw_job_id,
            provider_session_id=raw_job_id,
            created_at=created_at,
        )
        async with self._lock:
            self._job_handles[request.run_id] = handle
        await self._emit(
            request.run_id,
            "remoteJobStarted",
            {"jobId": handle.provider_job_id},
            event_sink,
        )
        return handle

    async def lookup_launch(
        self,
        request: WorkerRequest,
        *,
        idempotency_key: str,
    ) -> WorkerJobLaunchObservation:
        """Query a protocol-v2 launch registry without creating work."""

        self._validate_read_only(request)
        self._build_job_payload(request)
        self._validate_v2_idempotency_key(idempotency_key)
        async with self._client() as client:
            try:
                await self._negotiate_job_contract(client)
            except (httpx.HTTPError, WorkerProtocolError, WorkerJobLaunchRejected) as error:
                return self._unknown_launch_observation(
                    f"Local Worker launch registry negotiation failed: {type(error).__name__}"
                )
            if self.protocol_version != 2:
                return self._unknown_launch_observation(
                    "Local Worker protocol v1 has no durable launch lookup"
                )
            return await self._lookup_launch_v2(
                client,
                request,
                idempotency_key=idempotency_key,
            )

    @classmethod
    def request_digest(cls, request: WorkerRequest, *, driver_id: str | None = None) -> str:
        """Return the protocol-v2 digest of immutable, credential-free execution input."""

        job = cls._build_job_payload(request)
        if driver_id is not None:
            if not _DRIVER_ID.fullmatch(driver_id):
                raise ValueError("Local Worker driver_id must be 1-128 safe ASCII characters")
            job = {"driver_id": driver_id, **job}
        document = {
            "protocol_version": 2,
            "execution": {"run_id": request.run_id, "task_id": request.task_id},
            "job": job,
        }
        canonical = json.dumps(
            document,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
        return "sha256:" + hashlib.sha256(canonical).hexdigest()

    async def _start_job_v2(
        self,
        request: WorkerRequest,
        *,
        payload: Mapping[str, Any],
        idempotency_key: str,
        client: httpx.AsyncClient,
    ) -> WorkerJobHandle:
        authority_id, registry_id, adapter_instance_id = self._v2_contract_identity()
        digest = self.request_digest(request, driver_id=self.driver_id)
        observation = await self._lookup_launch_v2(
            client,
            request,
            idempotency_key=idempotency_key,
            expected_digest=digest,
            expected_authority_id=authority_id,
            expected_registry_id=registry_id,
            expected_adapter_instance_id=adapter_instance_id,
        )
        existing = self._handle_launch_observation(observation)
        if existing is not None:
            return existing
        if not self._launch_is_safe_to_post(observation):
            raise WorkerJobOutcomeUncertain(
                WorkerJobState.UNKNOWN,
                observation.detail or "Local Worker v2 launch lookup was inconclusive",
            )

        body = {
            "protocol_version": 2,
            "authority_id": authority_id,
            "registry_id": registry_id,
            "idempotency_key": idempotency_key,
            "request_digest": digest,
            "execution": {"run_id": request.run_id, "task_id": request.task_id},
            "job": dict(payload),
        }
        # One bounded replay is permitted only after the durable registry again proves NOT_SEEN.
        for attempt in range(2):
            try:
                response = await self._request(client, "POST", "/v2/launches", json_body=body)
                observation = self._v2_launch_observation(
                    response,
                    request,
                    idempotency_key=idempotency_key,
                    expected_digest=digest,
                    expected_authority_id=authority_id,
                    expected_registry_id=registry_id,
                    expected_adapter_instance_id=adapter_instance_id,
                )
            except WorkerJobIdempotencyConflict:
                raise
            except (httpx.HTTPError, WorkerProtocolError):
                observation = await self._lookup_launch_v2(
                    client,
                    request,
                    idempotency_key=idempotency_key,
                    expected_digest=digest,
                    expected_authority_id=authority_id,
                    expected_registry_id=registry_id,
                    expected_adapter_instance_id=adapter_instance_id,
                )
            existing = self._handle_launch_observation(observation)
            if existing is not None:
                return existing
            if self._launch_is_safe_to_post(observation) and attempt == 0:
                continue
            raise WorkerJobOutcomeUncertain(
                WorkerJobState.UNKNOWN,
                observation.detail or "Local Worker v2 launch outcome is not safe to replay",
            )
        raise AssertionError("bounded Local Worker v2 launch loop exhausted")  # pragma: no cover

    async def _lookup_launch_v2(
        self,
        client: httpx.AsyncClient,
        request: WorkerRequest,
        *,
        idempotency_key: str,
        expected_digest: str | None = None,
        expected_authority_id: str | None = None,
        expected_registry_id: str | None = None,
        expected_adapter_instance_id: str | None = None,
    ) -> WorkerJobLaunchObservation:
        expected_digest = expected_digest or self.request_digest(request, driver_id=self.driver_id)
        if (
            expected_authority_id is None
            or expected_registry_id is None
            or expected_adapter_instance_id is None
        ):
            (
                expected_authority_id,
                expected_registry_id,
                expected_adapter_instance_id,
            ) = self._v2_contract_identity()
        path = f"/v2/launches/{quote(idempotency_key, safe='')}"
        if self.driver_id is not None:
            path += f"?driver_id={quote(self.driver_id, safe='')}"
        try:
            response = await self._request(client, "GET", path)
            return self._v2_launch_observation(
                response,
                request,
                idempotency_key=idempotency_key,
                expected_digest=expected_digest,
                expected_authority_id=expected_authority_id,
                expected_registry_id=expected_registry_id,
                expected_adapter_instance_id=expected_adapter_instance_id,
                allow_not_seen=True,
            )
        except WorkerJobIdempotencyConflict:
            raise
        except (httpx.HTTPError, WorkerProtocolError) as error:
            return self._unknown_launch_observation(
                f"Local Worker launch lookup failed: {type(error).__name__}"
            )

    def _v2_launch_observation(
        self,
        response: httpx.Response,
        request: WorkerRequest,
        *,
        idempotency_key: str,
        expected_digest: str,
        expected_authority_id: str,
        expected_registry_id: str,
        expected_adapter_instance_id: str,
        allow_not_seen: bool = False,
    ) -> WorkerJobLaunchObservation:
        envelope = self._json_object(response, "v2 launch")
        data = self._unwrap_data(envelope)
        error_data = envelope.get("error")
        if not isinstance(error_data, Mapping):
            error_data = {}
        code = self._string(data, "code", "error_code", "errorCode") or self._string(
            envelope, "code", "error_code", "errorCode"
        )
        code = code or self._string(error_data, "code", "error_code", "errorCode")
        if response.status_code == 409 and code == "IDEMPOTENCY_CONFLICT":
            raise WorkerJobIdempotencyConflict(
                "Local Worker launch key was reused with a different request digest"
            )

        raw_protocol = (
            data.get("protocol_version")
            or data.get("protocolVersion")
            or envelope.get("protocol_version")
            or envelope.get("protocolVersion")
        )
        if raw_protocol not in {2, "2", "2.0"}:
            raise WorkerProtocolError("Local Worker launch receipt is not protocol v2")
        receipt_authority_id = self._string(data, "authority_id", "authorityId") or self._string(
            envelope, "authority_id", "authorityId"
        )
        receipt_registry_id = self._string(data, "registry_id", "registryId") or self._string(
            envelope, "registry_id", "registryId"
        )
        if (
            receipt_authority_id != expected_authority_id
            or receipt_registry_id != expected_registry_id
        ):
            raise WorkerProtocolError(
                "Local Worker launch receipt authority does not match the negotiated registry"
            )
        driver_metadata = self._validate_v2_driver_receipt(data)
        echoed_key = self._string(data, "idempotency_key", "idempotencyKey")
        if echoed_key != idempotency_key:
            raise WorkerProtocolError("Local Worker launch receipt did not echo the requested key")

        state = self._launch_state(data)
        disposition = self._launch_disposition(data)
        if state is WorkerJobLaunchState.UNKNOWN or disposition is None:
            if response.status_code >= 400:
                response.raise_for_status()
            raise WorkerProtocolError("Local Worker returned an unknown v2 launch state")
        if state is WorkerJobLaunchState.NOT_SEEN:
            if not allow_not_seen or response.status_code != 404:
                raise WorkerProtocolError("NOT_SEEN is valid only for authoritative launch lookup")
            if disposition is not WorkerJobLaunchDisposition.DEFINITELY_NOT_LAUNCHED:
                raise WorkerProtocolError("Local Worker NOT_SEEN receipt is not authoritative")
        elif response.status_code >= 400 and state is not WorkerJobLaunchState.REJECTED_PRE_LAUNCH:
            response.raise_for_status()

        receipt_digest = self._string(data, "request_digest", "requestDigest")
        if state is not WorkerJobLaunchState.NOT_SEEN:
            if receipt_digest is None:
                raise WorkerProtocolError("Local Worker launch receipt omitted request digest")
            if receipt_digest != expected_digest:
                raise WorkerJobIdempotencyConflict(
                    "Local Worker launch receipt belongs to a different request digest"
                )
        launch_record_id = self._string(
            data, "launch_record_id", "launchRecordId", "launchRecordID"
        )
        job_id = self._string(data, "job_id", "jobId", "id")
        receipt_id = self._string(data, "receipt_id", "receiptId", "receiptID")
        if state is WorkerJobLaunchState.REJECTED_PRE_LAUNCH:
            if data.get("accepted") is not False or not receipt_id:
                raise WorkerProtocolError(
                    "Local Worker pre-launch rejection omitted its authoritative receipt"
                )
            if disposition is not WorkerJobLaunchDisposition.DEFINITELY_NOT_LAUNCHED:
                raise WorkerProtocolError(
                    "Local Worker rejection does not prove no job was launched"
                )
        elif state is not WorkerJobLaunchState.NOT_SEEN:
            if data.get("accepted") is not True:
                raise WorkerProtocolError("Local Worker accepted launch receipt is malformed")
            if not launch_record_id or not job_id or not receipt_id:
                raise WorkerProtocolError("Local Worker accepted launch omitted durable identities")

        handle = None
        if job_id:
            handle = WorkerJobHandle(
                run_id=request.run_id,
                adapter_type=self.ADAPTER_TYPE_V2,
                adapter_instance_id=expected_adapter_instance_id,
                provider_job_id=job_id,
                provider_session_id=job_id,
                created_at=event_time(),
                schema_version=2,
                metadata={
                    "protocolVersion": 2,
                    "launchRecordID": launch_record_id,
                    "receiptID": receipt_id,
                    "requestDigest": receipt_digest,
                    "replayed": data.get("replayed") is True,
                    **driver_metadata,
                },
            )
        reason_code = self._string(data, "reason_code", "reasonCode", "code") or code
        detail = self._string(
            data,
            "reason",
            "detail",
            "message",
            "error",
            "launch_error",
            "launchError",
        ) or self._string(error_data, "message", "detail", "error")
        return WorkerJobLaunchObservation(
            state=state,
            disposition=disposition,
            observed_at=event_time(),
            handle=handle,
            receipt_id=receipt_id,
            request_digest=receipt_digest,
            detail=str(redact(detail)) if detail else None,
            metadata={
                "statusCode": response.status_code,
                "reasonCode": redact(reason_code),
                "launchRecordID": launch_record_id,
                **driver_metadata,
            },
        )

    def _validate_v2_driver_receipt(self, data: Mapping[str, Any]) -> dict[str, Any]:
        expected = self._driver_profile
        if expected is None:
            return {}
        driver_id = self._string(data, "driver_id", "driverId", "driverID")
        driver_type = self._string(data, "driver_type", "driverType")
        revision = data.get("driver_profile_revision", data.get("driverProfileRevision"))
        fingerprint = self._string(data, "driver_profile_fingerprint", "driverProfileFingerprint")
        runtime_instance_id = self._string(
            data,
            "launch_runtime_instance_id",
            "launchRuntimeInstanceId",
            "launchRuntimeInstanceID",
        )
        if (
            driver_id != expected["driver_id"]
            or driver_type != expected["driver_type"]
            or revision != expected["profile_revision"]
            or fingerprint != expected["profile_fingerprint"]
        ):
            raise WorkerProtocolError(
                "Local Worker launch receipt driver does not match the negotiated profile"
            )
        if runtime_instance_id is not None and not 1 <= len(runtime_instance_id) <= 200:
            raise WorkerProtocolError("Local Worker launch runtime identity is not bounded")
        return {
            "nodeID": self._node_id,
            "driverID": driver_id,
            "driverType": driver_type,
            "driverProfileRevision": revision,
            "driverProfileFingerprint": fingerprint,
            "launchRuntimeInstanceID": runtime_instance_id,
        }

    @staticmethod
    def _launch_state(data: Mapping[str, Any]) -> WorkerJobLaunchState:
        raw = str(data.get("launch_state") or data.get("launchState") or data.get("state") or "")
        normalized = raw.strip().upper().replace("-", "_")
        return {
            "NOT_SEEN": WorkerJobLaunchState.NOT_SEEN,
            "REJECTED_PRE_LAUNCH": WorkerJobLaunchState.REJECTED_PRE_LAUNCH,
            "RESERVED": WorkerJobLaunchState.RESERVED,
            "LAUNCHING": WorkerJobLaunchState.LAUNCHING,
            "RUNNING": WorkerJobLaunchState.RUNNING,
            "COMPLETED": WorkerJobLaunchState.COMPLETED,
            "SUCCEEDED": WorkerJobLaunchState.COMPLETED,
            "FAILED": WorkerJobLaunchState.FAILED,
            "CANCELLED": WorkerJobLaunchState.CANCELLED,
            "CANCELED": WorkerJobLaunchState.CANCELLED,
            "UNKNOWN": WorkerJobLaunchState.UNKNOWN,
        }.get(normalized, WorkerJobLaunchState.UNKNOWN)

    @staticmethod
    def _launch_disposition(
        data: Mapping[str, Any],
    ) -> WorkerJobLaunchDisposition | None:
        raw = str(data.get("disposition") or data.get("launch_disposition") or "")
        normalized = raw.strip().replace("-", "_").replace(" ", "_").lower()
        return {
            "definitely_not_launched": WorkerJobLaunchDisposition.DEFINITELY_NOT_LAUNCHED,
            "definitelynotlaunched": WorkerJobLaunchDisposition.DEFINITELY_NOT_LAUNCHED,
            "definitely_launched": WorkerJobLaunchDisposition.DEFINITELY_LAUNCHED,
            "definitelylaunched": WorkerJobLaunchDisposition.DEFINITELY_LAUNCHED,
            "launch_in_progress": WorkerJobLaunchDisposition.LAUNCH_IN_PROGRESS,
            "launchinprogress": WorkerJobLaunchDisposition.LAUNCH_IN_PROGRESS,
            "launch_outcome_unknown": WorkerJobLaunchDisposition.LAUNCH_OUTCOME_UNKNOWN,
            "launchoutcomeunknown": WorkerJobLaunchDisposition.LAUNCH_OUTCOME_UNKNOWN,
        }.get(normalized)

    @staticmethod
    def _unknown_launch_observation(detail: str) -> WorkerJobLaunchObservation:
        return WorkerJobLaunchObservation(
            state=WorkerJobLaunchState.UNKNOWN,
            disposition=WorkerJobLaunchDisposition.LAUNCH_OUTCOME_UNKNOWN,
            observed_at=event_time(),
            detail=detail,
        )

    @staticmethod
    def _validate_v2_idempotency_key(idempotency_key: str) -> None:
        if not _IDEMPOTENCY_KEY.fullmatch(idempotency_key):
            raise ValueError("Local Worker v2 idempotency key must be 1-200 safe ASCII characters")

    def _v2_contract_identity(self) -> tuple[str, str, str]:
        """Capture one immutable authority snapshot for a complete v2 launch operation."""

        if self._protocol_version != 2 or self._authority_id is None or self._registry_id is None:
            raise WorkerProtocolError("Local Worker v2 authority identity is unavailable")
        return self._authority_id, self._registry_id, self._adapter_instance_id

    @staticmethod
    def _handle_launch_observation(
        observation: WorkerJobLaunchObservation,
    ) -> WorkerJobHandle | None:
        if observation.state is WorkerJobLaunchState.REJECTED_PRE_LAUNCH:
            status_code = observation.metadata.get("statusCode", 422)
            reason_code = observation.metadata.get("reasonCode")
            raise WorkerJobLaunchRejected(
                int(status_code),
                observation.detail,
                receipt_id=observation.receipt_id,
                reason_code=str(reason_code) if reason_code else None,
                request_digest=observation.request_digest,
            )
        if observation.handle is None:
            return None
        if observation.disposition is WorkerJobLaunchDisposition.DEFINITELY_LAUNCHED:
            return observation.handle
        if (
            observation.state is WorkerJobLaunchState.LAUNCHING
            and observation.disposition is WorkerJobLaunchDisposition.LAUNCH_IN_PROGRESS
            and observation.receipt_id
        ):
            return observation.handle
        return None

    @staticmethod
    def _launch_is_safe_to_post(observation: WorkerJobLaunchObservation) -> bool:
        return observation.state is WorkerJobLaunchState.NOT_SEEN or (
            observation.state is WorkerJobLaunchState.RESERVED
            and observation.disposition is WorkerJobLaunchDisposition.DEFINITELY_NOT_LAUNCHED
            and bool(observation.receipt_id)
        )

    async def reconcile_job(self, handle: WorkerJobHandle) -> WorkerJobObservation:
        if handle.adapter_type == self.ADAPTER_TYPE_V2:
            try:
                await self.negotiate_job_contract()
            except (httpx.HTTPError, WorkerProtocolError, WorkerJobLaunchRejected) as error:
                return WorkerJobObservation(
                    WorkerJobState.PROVIDER_UNREACHABLE,
                    event_time(),
                    detail=f"Local Worker v2 contract unavailable: {type(error).__name__}",
                )
        mismatch = self._handle_mismatch(handle)
        if mismatch is not None:
            return WorkerJobObservation(WorkerJobState.UNKNOWN, event_time(), detail=mismatch)
        try:
            async with self._client() as client:
                data = await self._job_status(client, handle)
        except httpx.HTTPStatusError as exc:
            status = exc.response.status_code
            if status == 404:
                state = WorkerJobState.PROVIDER_NOT_FOUND
                detail = "Local Worker authoritatively reported that the job was not found"
            elif 500 <= status <= 599:
                state = WorkerJobState.PROVIDER_UNREACHABLE
                detail = f"Local Worker returned HTTP {status} during reconciliation"
            else:
                state = WorkerJobState.UNKNOWN
                detail = f"Local Worker reconciliation returned HTTP {status}"
            return WorkerJobObservation(state, event_time(), detail=detail)
        except httpx.HTTPError as exc:
            return WorkerJobObservation(
                WorkerJobState.PROVIDER_UNREACHABLE,
                event_time(),
                detail=f"Local Worker reconciliation transport failed: {type(exc).__name__}",
            )
        except WorkerProtocolError as exc:
            return WorkerJobObservation(
                WorkerJobState.UNKNOWN,
                event_time(),
                detail=str(redact(str(exc))),
            )
        state, raw_state = self._job_state(data)
        return WorkerJobObservation(
            state,
            event_time(),
            metadata={"providerState": redact(raw_state)},
        )

    async def resume_job(
        self,
        request: WorkerRequest,
        handle: WorkerJobHandle,
        *,
        event_sink: EventSink | None = None,
    ) -> WorkerResult:
        if handle.adapter_type == self.ADAPTER_TYPE_V2:
            await self.negotiate_job_contract()
        async with self._client() as client:
            return await self._resume_job(
                request,
                handle,
                event_sink=event_sink,
                client=client,
            )

    async def _resume_job(
        self,
        request: WorkerRequest,
        handle: WorkerJobHandle,
        *,
        event_sink: EventSink | None,
        client: httpx.AsyncClient,
    ) -> WorkerResult:
        self._require_handle(request, handle)
        events: list[WorkerEvent] = []

        async def emit(kind: str, data: Mapping[str, Any] | None = None) -> None:
            event = await self._emit(request.run_id, kind, data, event_sink)
            events.append(event)

        async with self._lock:
            self._job_handles[request.run_id] = handle
        elapsed = max(0.0, (event_time() - handle.created_at).total_seconds())
        deadline = time.monotonic() + max(0.0, request.timeout_seconds - elapsed)
        try:
            while True:
                # Protocol V2 cancellation acknowledgement means only that the durable authority
                # accepted the request.  Completion may have won the race, so V2 must continue
                # polling the authoritative terminal state instead of synthesizing CANCELLED.
                if (
                    handle.adapter_type != self.ADAPTER_TYPE_V2
                    and request.run_id in self._cancelled_runs
                ):
                    await emit(
                        "remoteJobExited",
                        {"jobId": handle.provider_job_id, "state": RunState.CANCELLED.value},
                    )
                    return self._job_result(
                        request,
                        handle,
                        RunState.CANCELLED,
                        {},
                        events,
                        error="local worker job was cancelled",
                    )
                if time.monotonic() >= deadline:
                    error = f"local worker exceeded {request.timeout_seconds:g} second timeout"
                    await emit("remoteJobTimedOut", {"jobId": handle.provider_job_id})
                    try:
                        cancellation = await self._request(
                            client, "POST", self._cancel_path(handle)
                        )
                    except httpx.HTTPError as exc:
                        raise WorkerJobOutcomeUncertain(
                            WorkerJobState.PROVIDER_UNREACHABLE,
                            f"{error}; remote cancellation could not be confirmed",
                        ) from exc
                    if cancellation.status_code == 404:
                        await emit(
                            "remoteJobExited",
                            {"jobId": handle.provider_job_id, "state": RunState.TIMED_OUT.value},
                        )
                        return self._job_result(
                            request,
                            handle,
                            RunState.TIMED_OUT,
                            {},
                            events,
                            error=error,
                        )
                    if cancellation.status_code >= 400:
                        raise WorkerJobOutcomeUncertain(
                            WorkerJobState.UNKNOWN,
                            f"{error}; cancellation returned HTTP {cancellation.status_code}",
                        )
                    try:
                        data = await self._job_status(client, handle)
                    except httpx.HTTPStatusError as exc:
                        if exc.response.status_code == 404:
                            await emit(
                                "remoteJobExited",
                                {
                                    "jobId": handle.provider_job_id,
                                    "state": RunState.TIMED_OUT.value,
                                },
                            )
                            return self._job_result(
                                request,
                                handle,
                                RunState.TIMED_OUT,
                                {},
                                events,
                                error=error,
                            )
                        observed = (
                            WorkerJobState.PROVIDER_UNREACHABLE
                            if 500 <= exc.response.status_code <= 599
                            else WorkerJobState.UNKNOWN
                        )
                        raise WorkerJobOutcomeUncertain(
                            observed,
                            f"{error}; cancellation state returned HTTP {exc.response.status_code}",
                        ) from exc
                    except httpx.HTTPError as exc:
                        raise WorkerJobOutcomeUncertain(
                            WorkerJobState.PROVIDER_UNREACHABLE,
                            f"{error}; cancellation state could not be queried",
                        ) from exc
                    except WorkerProtocolError as exc:
                        raise WorkerJobOutcomeUncertain(
                            WorkerJobState.UNKNOWN,
                            f"{error}; cancellation state was invalid",
                        ) from exc
                    job_state, raw_state = self._job_state(data)
                    if job_state is WorkerJobState.KNOWN_RUNNING:
                        raise WorkerJobOutcomeUncertain(
                            WorkerJobState.KNOWN_RUNNING,
                            f"{error}; cancellation is not terminal ({raw_state or 'running'})",
                        )
                    if job_state is WorkerJobState.UNKNOWN:
                        raise WorkerJobOutcomeUncertain(
                            WorkerJobState.UNKNOWN,
                            f"{error}; cancellation state is unknown ({raw_state or 'empty'})",
                        )
                    run_state, terminal_error = self._terminal_run_state(job_state, data)
                    await emit(
                        "remoteJobExited",
                        {"jobId": handle.provider_job_id, "state": run_state.value},
                    )
                    return self._job_result(
                        request,
                        handle,
                        run_state,
                        data,
                        events,
                        error=terminal_error,
                    )

                data = await self._job_status(client, handle)
                job_state, raw_state = self._job_state(data)
                await emit(
                    "remoteJobStatus",
                    {"jobId": handle.provider_job_id, "state": raw_state},
                )
                if job_state is WorkerJobState.KNOWN_RUNNING:
                    await asyncio.sleep(self.poll_interval_seconds)
                    continue
                run_state, error = self._terminal_run_state(job_state, data)
                await emit(
                    "remoteJobExited",
                    {"jobId": handle.provider_job_id, "state": run_state.value},
                )
                return self._job_result(
                    request,
                    handle,
                    run_state,
                    data,
                    events,
                    error=error,
                )
        finally:
            await self._forget_job(request.run_id, handle)

    async def collect_job(
        self,
        request: WorkerRequest,
        handle: WorkerJobHandle,
        *,
        event_sink: EventSink | None = None,
    ) -> WorkerResult:
        if handle.adapter_type == self.ADAPTER_TYPE_V2:
            await self.negotiate_job_contract()
        self._require_handle(request, handle)
        events: list[WorkerEvent] = []

        async def emit(kind: str, data: Mapping[str, Any] | None = None) -> None:
            event = await self._emit(request.run_id, kind, data, event_sink)
            events.append(event)

        async with self._client() as client:
            data = await self._job_status(client, handle)
        job_state, raw_state = self._job_state(data)
        await emit(
            "remoteJobStatus",
            {"jobId": handle.provider_job_id, "state": raw_state},
        )
        run_state, error = self._terminal_run_state(job_state, data)
        await emit(
            "remoteJobExited",
            {"jobId": handle.provider_job_id, "state": run_state.value},
        )
        await self._forget_job(request.run_id, handle)
        return self._job_result(request, handle, run_state, data, events, error=error)

    async def cancel(self, run_id: str) -> bool:
        async with self._lock:
            handle = self._job_handles.get(run_id)
            if handle is None:
                return False
        return await self.cancel_job(handle)

    async def cancel_job(self, handle: WorkerJobHandle) -> bool:
        expected_v2_identity: tuple[str, str, str] | None = None
        if handle.adapter_type == self.ADAPTER_TYPE_V2:
            await self.negotiate_job_contract()
            expected_v2_identity = self._v2_contract_identity()
        mismatch = self._handle_mismatch(handle)
        if mismatch is not None:
            raise WorkerProtocolError(mismatch)
        async with self._client() as client:
            response = await self._request(client, "POST", self._cancel_path(handle))
        if response.status_code >= 400:
            return False
        envelope = self._json_object(response, "job cancellation")
        data = self._unwrap_data(envelope)
        if expected_v2_identity is not None:
            authority_id, registry_id, _adapter_instance_id = expected_v2_identity
            raw_protocol = (
                data.get("protocol_version")
                or data.get("protocolVersion")
                or envelope.get("protocol_version")
                or envelope.get("protocolVersion")
            )
            if raw_protocol not in {2, "2", "2.0"}:
                raise WorkerProtocolError("Local Worker cancellation receipt is not protocol v2")
            receipt_authority_id = self._string(
                data, "authority_id", "authorityId"
            ) or self._string(envelope, "authority_id", "authorityId")
            receipt_registry_id = self._string(data, "registry_id", "registryId") or self._string(
                envelope, "registry_id", "registryId"
            )
            if receipt_authority_id != authority_id or receipt_registry_id != registry_id:
                raise WorkerProtocolError(
                    "Local Worker cancellation receipt authority does not match "
                    "the negotiated registry"
                )
            self._validate_v2_driver_receipt(data)
            accepted_value = data.get("cancel_accepted", data.get("cancelAccepted"))
            if not isinstance(accepted_value, bool):
                raise WorkerProtocolError("Local Worker v2 cancellation receipt is malformed")
            accepted = accepted_value
        else:
            accepted_value = data.get("accepted")
            if accepted_value is None:
                # Historical Protocol V1 daemons used HTTP 2xx as their only acknowledgement.
                accepted = True
            elif isinstance(accepted_value, bool):
                accepted = accepted_value
            else:
                raise WorkerProtocolError("Local Worker v1 cancellation receipt is malformed")
        if accepted and handle.adapter_type != self.ADAPTER_TYPE_V2:
            async with self._lock:
                self._cancelled_runs.add(handle.run_id)
        return accepted

    async def _job_status(
        self,
        client: httpx.AsyncClient,
        handle: WorkerJobHandle,
    ) -> Mapping[str, Any]:
        version = "v2" if handle.adapter_type == self.ADAPTER_TYPE_V2 else "v1"
        response = await self._request(
            client,
            "GET",
            f"/{version}/jobs/{quote(handle.provider_job_id, safe='')}",
        )
        response.raise_for_status()
        envelope = self._json_object(response, "job status")
        data = self._unwrap_data(envelope)
        if version == "v2" and self._driver_profile is not None:
            authority_id, registry_id, _instance_id = self._v2_contract_identity()
            receipt_authority_id = self._string(
                data, "authority_id", "authorityId"
            ) or self._string(envelope, "authority_id", "authorityId")
            receipt_registry_id = self._string(data, "registry_id", "registryId") or self._string(
                envelope, "registry_id", "registryId"
            )
            if receipt_authority_id != authority_id or receipt_registry_id != registry_id:
                raise WorkerProtocolError(
                    "Local Worker job status authority does not match the negotiated registry"
                )
            self._validate_v2_driver_receipt(data)
        return data

    def _cancel_path(self, handle: WorkerJobHandle) -> str:
        version = "v2" if handle.adapter_type == self.ADAPTER_TYPE_V2 else "v1"
        job_id = quote(handle.provider_job_id, safe="")
        return f"/{version}/jobs/{job_id}/cancel"

    @staticmethod
    def _job_state(data: Mapping[str, Any]) -> tuple[WorkerJobState, str]:
        raw_state = str(data.get("state") or data.get("status") or "")
        normalized = raw_state.strip().lower().replace("-", "_")
        if normalized in {
            "accepted",
            "queued",
            "pending",
            "reserved",
            "starting",
            "launching",
            "running",
            "in_progress",
            "processing",
            "cancelling",
            "canceling",
        }:
            return WorkerJobState.KNOWN_RUNNING, raw_state
        if normalized in {"completed", "succeeded", "success"}:
            return WorkerJobState.KNOWN_COMPLETED, raw_state
        if normalized in {"failed", "error"}:
            return WorkerJobState.KNOWN_FAILED, raw_state
        if normalized in {"cancelled", "canceled"}:
            return WorkerJobState.KNOWN_CANCELLED, raw_state
        return WorkerJobState.UNKNOWN, raw_state

    @classmethod
    def _terminal_run_state(
        cls,
        state: WorkerJobState,
        data: Mapping[str, Any],
    ) -> tuple[RunState, str | None]:
        if state is WorkerJobState.KNOWN_COMPLETED:
            return RunState.COMPLETED, None
        if state is WorkerJobState.KNOWN_FAILED:
            error = cls._safe_error(data) or "local worker job failed"
            provider_code = cls._string(data, "error")
            classified = {
                "PROVIDER_AUTH_REQUIRED": RunState.AUTH_REQUIRED,
                "PROVIDER_RATE_LIMITED": RunState.RATE_LIMITED,
                "PROVIDER_TIMEOUT": RunState.TIMED_OUT,
            }.get(provider_code, RunState.FAILED)
            return classified, error
        if state is WorkerJobState.KNOWN_CANCELLED:
            return RunState.CANCELLED, "local worker job was cancelled"
        raise WorkerProtocolError(f"Local Worker job is not collectable from {state.value}")

    @classmethod
    def _job_result(
        cls,
        request: WorkerRequest,
        handle: WorkerJobHandle,
        state: RunState,
        data: Mapping[str, Any],
        events: list[WorkerEvent],
        *,
        error: str | None,
    ) -> WorkerResult:
        final_text = cls._final_text(data)
        return WorkerResult(
            run_id=request.run_id,
            state=state,
            pid=None,
            exit_code=0 if state is RunState.COMPLETED else None,
            started_at=handle.created_at,
            ended_at=event_time(),
            stdout=final_text,
            stderr="",
            final_text=final_text,
            events=tuple(events),
            session_id=handle.provider_session_id or handle.provider_job_id,
            model=cls._model(data),
            usage=cls._usage(data),
            error=error,
        )

    def _handle_mismatch(self, handle: WorkerJobHandle) -> str | None:
        if handle.adapter_type == self.ADAPTER_TYPE:
            expected_instance = self._v1_adapter_instance_id
        elif handle.adapter_type == self.ADAPTER_TYPE_V2 and self.protocol_version == 2:
            expected_instance = self.adapter_instance_id
        else:
            return "Worker job handle adapter type does not match Local Worker"
        if handle.adapter_instance_id != expected_instance:
            return "Worker job handle adapter instance does not match Local Worker"
        return None

    def _require_handle(self, request: WorkerRequest, handle: WorkerJobHandle) -> None:
        mismatch = self._handle_mismatch(handle)
        if mismatch is not None:
            raise WorkerProtocolError(mismatch)
        if request.run_id != handle.run_id:
            raise WorkerProtocolError("Worker job handle belongs to a different run")

    async def _forget_job(
        self,
        run_id: str,
        handle: WorkerJobHandle | None,
    ) -> None:
        async with self._lock:
            current = self._job_handles.get(run_id)
            if handle is None or current == handle:
                self._job_handles.pop(run_id, None)
            self._cancelled_runs.discard(run_id)

    @staticmethod
    async def _emit(
        run_id: str,
        kind: str,
        data: Mapping[str, Any] | None,
        event_sink: EventSink | None,
    ) -> WorkerEvent:
        event = WorkerEvent(run_id, kind, event_time(), redact(data or {}))
        await publish_event(event_sink, event)
        return event

    @staticmethod
    def _status_run_state(status_code: int) -> RunState:
        """Map a Worker HTTP rejection onto the supervisor's run vocabulary.

        Authentication failures are not transient: the runtime marks the worker
        offline instead of retrying against an endpoint that will keep refusing.
        """

        if status_code in {401, 403}:
            return RunState.AUTH_REQUIRED
        if status_code == 429:
            return RunState.RATE_LIMITED
        return RunState.FAILED

    @staticmethod
    def _validate_read_only(request: WorkerRequest) -> None:
        if request_requires_code_write(request):
            raise UnsafeWorkerRequest("Local AI is not authorized for code-write tasks")

    @classmethod
    def _build_job_payload(cls, request: WorkerRequest) -> dict[str, Any]:
        cls._validate_read_only(request)
        if request.model is not None:
            raise UnsafeWorkerRequest("Local model selection must remain server-side")
        metadata = dict(request.metadata)
        role = metadata.get("worker_role")
        if role is None:
            labels = {str(value) for value in metadata.get("labels", [])}
            role = "FAST_ROUTER" if "fastRouting" in labels else "GENERAL_REASONING"
        if role not in _ALLOWED_ROLES:
            raise UnsafeWorkerRequest("Local Worker role is not authorized")

        schema = metadata.get("response_schema")
        if schema is not None and not isinstance(schema, Mapping):
            raise WorkerProtocolError("Local Worker response_schema must be an object")
        payload: dict[str, Any] = {
            "job_type": "inference.structured" if schema is not None else "inference.chat",
            "role": role,
            "prompt": request.prompt,
        }
        if schema is not None:
            payload["schema"] = redact(dict(schema))
        return payload

    def _build_v2_job_payload(self, request: WorkerRequest) -> dict[str, Any]:
        """Bind semantic work to one operator-selected server-side driver.

        No executable, argv, environment, cwd, URL, or shell field is accepted from the Task.
        The daemon resolves ``driver_id`` against its immutable local catalog.
        """

        payload = self._build_job_payload(request)
        if self.driver_id is not None:
            return {"driver_id": self.driver_id, **payload}
        return payload

    @staticmethod
    def _json_object(response: httpx.Response, context: str) -> Mapping[str, Any]:
        content_length = response.headers.get("content-length")
        if content_length is not None:
            try:
                declared_length = int(content_length)
            except ValueError:
                declared_length = -1
            if declared_length > _MAX_HTTP_RESPONSE_BYTES:
                raise WorkerProtocolError(
                    f"Local Worker {context} response exceeded the capture limit"
                )
        if len(response.content) > _MAX_HTTP_RESPONSE_BYTES:
            raise WorkerProtocolError(f"Local Worker {context} response exceeded the capture limit")
        try:
            value = response.json()
        except (ValueError, json.JSONDecodeError) as exc:
            raise WorkerProtocolError(f"Local Worker {context} response is not JSON") from exc
        if not isinstance(value, Mapping):
            raise WorkerProtocolError(f"Local Worker {context} response must be an object")
        return value

    @staticmethod
    def _unwrap_data(mapping: Mapping[str, Any]) -> Mapping[str, Any]:
        data = mapping.get("data")
        return data if isinstance(data, Mapping) else mapping

    @staticmethod
    def _string(mapping: Mapping[str, Any], *keys: str) -> str | None:
        for key in keys:
            value = mapping.get(key)
            if isinstance(value, str):
                return value
        return None

    @classmethod
    def _final_text(cls, data: Mapping[str, Any]) -> str:
        result = data.get("result")
        if isinstance(result, Mapping):
            structured = result.get("json")
            if isinstance(structured, (Mapping, list)):
                return json.dumps(structured, separators=(",", ":"), sort_keys=True)
            text = cls._string(result, "content", "text", "output", "response")
            if text is not None:
                return text
            content = result.get("content")
            if isinstance(content, (Mapping, list)):
                return json.dumps(content, separators=(",", ":"), sort_keys=True)
        return cls._string(data, "result", "text", "output", "response") or ""

    @classmethod
    def _model(cls, data: Mapping[str, Any]) -> str | None:
        result = data.get("result")
        if isinstance(result, Mapping):
            model = cls._string(result, "model", "modelId", "model_id")
            if model:
                return model
        return cls._string(data, "selected_model", "model", "modelId", "model_id")

    @classmethod
    def _safe_error(cls, data: Mapping[str, Any]) -> str | None:
        value = cls._string(data, "error", "message", "detail")
        return str(redact(value)) if value else None

    @staticmethod
    def _usage(data: Mapping[str, Any]) -> Usage:
        raw = data.get("usage") or data.get("metrics")
        if not isinstance(raw, Mapping):
            return Usage()

        def integer(*keys: str) -> int | None:
            return next(
                (
                    value
                    for key in keys
                    if isinstance((value := raw.get(key)), int) and not isinstance(value, bool)
                ),
                None,
            )

        cost = raw.get("costUsd") or raw.get("cost_usd")
        return Usage(
            input_tokens=integer("inputTokens", "input_tokens", "prompt_tokens"),
            output_tokens=integer("outputTokens", "output_tokens", "completion_tokens"),
            total_tokens=integer("totalTokens", "total_tokens"),
            cost_usd=float(cost) if isinstance(cost, (int, float)) else None,
            raw=redact(dict(raw)),
        )
