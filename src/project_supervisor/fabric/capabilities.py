from __future__ import annotations

import hashlib
import json
import math
import re
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import InitVar, dataclass, field
from datetime import datetime
from enum import StrEnum
from types import MappingProxyType
from typing import Any

_CAPABILITY_ID = re.compile(r"^[a-z][a-z0-9]*(?:[._-][a-z0-9]+)*$")
_SAFE_NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,199}$")
_VERSION = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:/-]{0,127}$")
_SHA256 = re.compile(r"^sha256:[0-9a-f]{64}$")
WORKER_MANIFEST_SCHEMA_VERSION = "worker-capability-manifest/v1"

ParameterValue = str | int | float | bool


class CapabilityError(ValueError):
    """A capability definition, claim, or manifest is invalid."""


class UnknownCapabilityError(CapabilityError):
    """A manifest referenced a capability absent from its declared catalog."""


class CapabilityParameterError(CapabilityError):
    """A capability claim violated its definition's parameter contract."""


class CostMode(StrEnum):
    SUBSCRIPTION = "subscription"
    LOCAL_FREE = "localFree"
    PAID = "paid"
    METERED = "metered"
    UNKNOWN = "unknown"


class SubscriptionState(StrEnum):
    AVAILABLE = "available"
    EXHAUSTED = "exhausted"
    UNKNOWN = "unknown"


class ObservationFreshness(StrEnum):
    FRESH = "fresh"
    STALE = "stale"
    UNKNOWN = "unknown"


class WorkerLocality(StrEnum):
    LOCAL = "local"
    REMOTE = "remote"
    UNKNOWN = "unknown"


class WorkerPrivacy(StrEnum):
    PUBLIC = "public"
    INTERNAL = "internal"
    SENSITIVE = "sensitive"
    RESTRICTED = "restricted"
    UNKNOWN = "unknown"


class WorkerHealth(StrEnum):
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    OFFLINE = "offline"
    UNHEALTHY = "offline"
    UNKNOWN = "unknown"


class QuotaAvailability(StrEnum):
    AVAILABLE = "available"
    SCARCE = "scarce"
    WARNING = "scarce"
    EXHAUSTED = "exhausted"
    UNKNOWN = "unknown"


def _require_safe_name(value: str, label: str) -> None:
    if not _SAFE_NAME.fullmatch(value):
        raise CapabilityError(f"{label} must be a bounded safe semantic identifier")


def _canonical_json(value: Mapping[str, Any]) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


@dataclass(frozen=True, slots=True)
class CapabilityParameterConstraint:
    """A small deterministic schema for one manifest capability parameter."""

    value_type: str
    required: bool = False
    allowed_values: tuple[ParameterValue, ...] = ()
    minimum: float | None = None
    maximum: float | None = None
    min_length: int | None = None
    max_length: int | None = None
    pattern: str | None = None

    def __post_init__(self) -> None:
        if self.value_type not in {"string", "integer", "number", "boolean"}:
            raise CapabilityError(
                "parameter value_type must be string, integer, number, or boolean"
            )
        if self.minimum is not None and not math.isfinite(self.minimum):
            raise CapabilityError("parameter minimum must be finite")
        if self.maximum is not None and not math.isfinite(self.maximum):
            raise CapabilityError("parameter maximum must be finite")
        if self.minimum is not None and self.maximum is not None and self.minimum > self.maximum:
            raise CapabilityError("parameter minimum cannot exceed maximum")
        if self.min_length is not None and self.min_length < 0:
            raise CapabilityError("parameter min_length cannot be negative")
        if self.max_length is not None and self.max_length < 0:
            raise CapabilityError("parameter max_length cannot be negative")
        if (
            self.min_length is not None
            and self.max_length is not None
            and self.min_length > self.max_length
        ):
            raise CapabilityError("parameter min_length cannot exceed max_length")
        if self.pattern is not None:
            if self.value_type != "string" or len(self.pattern) > 256:
                raise CapabilityError("parameter pattern requires a bounded string constraint")
            try:
                re.compile(self.pattern)
            except re.error as error:
                raise CapabilityError("parameter pattern is invalid") from error
        for value in self.allowed_values:
            self._validate(value, check_allowed=False)

    def validate(self, value: ParameterValue, *, name: str) -> None:
        try:
            self._validate(value, check_allowed=True)
        except CapabilityParameterError as error:
            raise CapabilityParameterError(
                f"invalid capability parameter {name}: {error}"
            ) from error

    def _validate(self, value: ParameterValue, *, check_allowed: bool) -> None:
        type_matches = {
            "string": isinstance(value, str),
            "integer": isinstance(value, int) and not isinstance(value, bool),
            "number": isinstance(value, (int, float)) and not isinstance(value, bool),
            "boolean": isinstance(value, bool),
        }[self.value_type]
        if not type_matches:
            raise CapabilityParameterError(f"must be {self.value_type}")
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            if not math.isfinite(float(value)):
                raise CapabilityParameterError("must be finite")
            if self.minimum is not None and value < self.minimum:
                raise CapabilityParameterError(f"must be at least {self.minimum}")
            if self.maximum is not None and value > self.maximum:
                raise CapabilityParameterError(f"must be at most {self.maximum}")
        if isinstance(value, str):
            if self.min_length is not None and len(value) < self.min_length:
                raise CapabilityParameterError(
                    f"must contain at least {self.min_length} characters"
                )
            if self.max_length is not None and len(value) > self.max_length:
                raise CapabilityParameterError(f"must contain at most {self.max_length} characters")
            if self.pattern is not None and re.fullmatch(self.pattern, value) is None:
                raise CapabilityParameterError("does not match the required pattern")
        if check_allowed and self.allowed_values and value not in self.allowed_values:
            raise CapabilityParameterError("is not an allowed value")

    def to_protocol(self) -> dict[str, Any]:
        return {
            "type": self.value_type,
            "required": self.required,
            "allowedValues": list(self.allowed_values),
            "minimum": self.minimum,
            "maximum": self.maximum,
            "minLength": self.min_length,
            "maxLength": self.max_length,
            "pattern": self.pattern,
        }


@dataclass(frozen=True, slots=True)
class CapabilityClaim:
    name: str
    parameters: Mapping[str, ParameterValue] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not isinstance(self.name, str) or not self.name.strip() or len(self.name) > 200:
            raise CapabilityError("capability claim name must be bounded and non-empty")
        values = dict(self.parameters)
        for key, value in values.items():
            if not _SAFE_NAME.fullmatch(key):
                raise CapabilityParameterError(
                    "capability parameter names must be bounded safe identifiers"
                )
            if not isinstance(value, (str, int, float, bool)):
                raise CapabilityParameterError("capability parameter values must be JSON scalars")
        object.__setattr__(self, "parameters", MappingProxyType(dict(sorted(values.items()))))

    def to_protocol(self) -> dict[str, Any]:
        return {"name": self.name, "parameters": dict(self.parameters)}


@dataclass(frozen=True, slots=True)
class CapabilityDefinition:
    """One extensible capability name; names are data, never a closed provider enum."""

    name: str
    aliases: frozenset[str] = frozenset()
    parameters: Mapping[str, CapabilityParameterConstraint] = field(default_factory=dict)
    parameter_aliases: Mapping[str, str] = field(default_factory=dict)
    description: str = ""

    def __post_init__(self) -> None:
        if not _CAPABILITY_ID.fullmatch(self.name):
            raise CapabilityError(
                "canonical capability names must be lowercase semantic identifiers"
            )
        if len(self.description) > 1000:
            raise CapabilityError("capability description is too long")
        aliases = frozenset(self.aliases)
        if self.name in aliases:
            raise CapabilityError("canonical capability name cannot also be an alias")
        for alias in aliases:
            if not isinstance(alias, str) or not alias or len(alias) > 200 or not alias.isascii():
                raise CapabilityError("capability aliases must be bounded non-empty ASCII strings")
        constraints = dict(self.parameters)
        for key, value in constraints.items():
            if not _CAPABILITY_ID.fullmatch(key):
                raise CapabilityError("capability parameter names must be semantic identifiers")
            if not isinstance(value, CapabilityParameterConstraint):
                raise CapabilityError("capability parameters require typed constraints")
        parameter_aliases = dict(self.parameter_aliases)
        for alias, canonical in parameter_aliases.items():
            if (
                not isinstance(alias, str)
                or not alias
                or len(alias) > 200
                or not alias.isascii()
                or canonical not in constraints
                or alias in constraints
            ):
                raise CapabilityError("capability parameter alias is invalid")
        if len(parameter_aliases) != len(set(parameter_aliases)):
            raise CapabilityError("capability parameter aliases must be unique")
        object.__setattr__(self, "aliases", aliases)
        object.__setattr__(self, "parameters", MappingProxyType(dict(sorted(constraints.items()))))
        object.__setattr__(
            self,
            "parameter_aliases",
            MappingProxyType(dict(sorted(parameter_aliases.items()))),
        )

    def canonicalize(self, claim: CapabilityClaim) -> CapabilityClaim:
        supplied: dict[str, ParameterValue] = {}
        for raw_name, value in claim.parameters.items():
            name = self.parameter_aliases.get(raw_name, raw_name)
            if name in supplied and supplied[name] != value:
                raise CapabilityParameterError(
                    f"capability {self.name} repeats parameter {name} with conflicting values"
                )
            supplied[name] = value
        unknown = set(supplied) - set(self.parameters)
        if unknown:
            raise CapabilityParameterError(
                f"capability {self.name} has unknown parameters: {','.join(sorted(unknown))}"
            )
        missing = {
            name for name, constraint in self.parameters.items() if constraint.required
        } - set(supplied)
        if missing:
            raise CapabilityParameterError(
                f"capability {self.name} is missing parameters: {','.join(sorted(missing))}"
            )
        for name, value in supplied.items():
            self.parameters[name].validate(value, name=name)
        return CapabilityClaim(self.name, supplied)

    def satisfies(self, available: CapabilityClaim, required: CapabilityClaim) -> bool:
        """Return whether concrete Worker limits satisfy the requested minimum contract."""

        worker_claim = self.canonicalize(available)
        task_claim = self.canonicalize(required)
        for name, requested in task_claim.parameters.items():
            provided = worker_claim.parameters.get(name)
            if provided is None:
                return False
            if (
                isinstance(requested, (int, float))
                and not isinstance(requested, bool)
                and isinstance(provided, (int, float))
                and not isinstance(provided, bool)
            ):
                if provided < requested:
                    return False
            elif provided != requested:
                return False
        return True

    def to_protocol(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "aliases": sorted(self.aliases),
            "parameters": {
                name: constraint.to_protocol() for name, constraint in self.parameters.items()
            },
            "parameterAliases": dict(self.parameter_aliases),
            "description": self.description,
        }


class CapabilityCatalog:
    """Immutable, versioned capability vocabulary with explicit alias resolution."""

    def __init__(self, version: str, definitions: Iterable[CapabilityDefinition]) -> None:
        if not _VERSION.fullmatch(version):
            raise CapabilityError("capability catalog version must be a bounded semantic version")
        by_name: dict[str, CapabilityDefinition] = {}
        aliases: dict[str, str] = {}
        for definition in definitions:
            if definition.name in by_name or definition.name in aliases:
                raise CapabilityError(f"duplicate capability name: {definition.name}")
            by_name[definition.name] = definition
            for alias in definition.aliases:
                if alias in by_name or alias in aliases:
                    raise CapabilityError(f"duplicate capability alias: {alias}")
                aliases[alias] = definition.name
        if not by_name:
            raise CapabilityError("capability catalog must not be empty")
        self.version = version
        self._definitions = MappingProxyType(dict(sorted(by_name.items())))
        self._aliases = MappingProxyType(dict(sorted(aliases.items())))

    @property
    def definitions(self) -> tuple[CapabilityDefinition, ...]:
        return tuple(self._definitions.values())

    def resolve(self, name: str) -> CapabilityDefinition:
        canonical = self._aliases.get(name, name)
        definition = self._definitions.get(canonical)
        if definition is None:
            raise UnknownCapabilityError(f"unknown capability in catalog {self.version}: {name}")
        return definition

    def canonicalize_claim(self, claim: str | CapabilityClaim) -> CapabilityClaim:
        value = CapabilityClaim(claim) if isinstance(claim, str) else claim
        return self.resolve(value.name).canonicalize(value)

    def canonicalize_claims(
        self, claims: Iterable[str | CapabilityClaim]
    ) -> tuple[CapabilityClaim, ...]:
        result: dict[str, CapabilityClaim] = {}
        for raw in claims:
            claim = self.canonicalize_claim(raw)
            previous = result.get(claim.name)
            if previous is not None and previous != claim:
                raise CapabilityError(f"conflicting claims for capability {claim.name}")
            result[claim.name] = claim
        return tuple(result[name] for name in sorted(result))

    def canonicalize_names(self, names: Iterable[str]) -> frozenset[str]:
        return frozenset(self.canonicalize_claim(name).name for name in names)

    def extend(
        self, *, version: str, definitions: Iterable[CapabilityDefinition]
    ) -> CapabilityCatalog:
        return CapabilityCatalog(version, (*self.definitions, *tuple(definitions)))

    def to_protocol(self) -> dict[str, Any]:
        return {
            "version": self.version,
            "capabilities": [definition.to_protocol() for definition in self.definitions],
        }


INITIAL_CAPABILITY_CATALOG = CapabilityCatalog(
    "fabric-capabilities/v1",
    (
        CapabilityDefinition("analysis"),
        CapabilityDefinition("architecture"),
        CapabilityDefinition("architecture-review"),
        CapabilityDefinition(
            "control-browser", aliases=frozenset({"CONTROL_BROWSER", "controlBrowser"})
        ),
        CapabilityDefinition("control-gui", aliases=frozenset({"CONTROL_GUI", "controlGUI"})),
        CapabilityDefinition("creative"),
        CapabilityDefinition(
            "edit-code",
            aliases=frozenset({"EDIT_CODE", "code", "coding", "editCode"}),
        ),
        CapabilityDefinition("execute"),
        CapabilityDefinition("fast-routing", aliases=frozenset({"fastRouting"})),
        CapabilityDefinition("generate-3d", aliases=frozenset({"GENERATE_3D", "generate3D"})),
        CapabilityDefinition(
            "generate-image", aliases=frozenset({"GENERATE_IMAGE", "generateImage"})
        ),
        CapabilityDefinition(
            "generate-video", aliases=frozenset({"GENERATE_VIDEO", "generateVideo"})
        ),
        CapabilityDefinition("high-uncertainty", aliases=frozenset({"highUncertainty"})),
        CapabilityDefinition("local-classification", aliases=frozenset({"localClassification"})),
        CapabilityDefinition("local-inference", aliases=frozenset({"localInference"})),
        CapabilityDefinition(
            "long-context",
            aliases=frozenset({"longContext"}),
            parameters={
                "context-tokens": CapabilityParameterConstraint(
                    "integer", minimum=1, maximum=100_000_000
                )
            },
        ),
        CapabilityDefinition("native-workers", aliases=frozenset({"nativeWorkers"})),
        CapabilityDefinition("privacy-sensitive", aliases=frozenset({"privacySensitive"})),
        CapabilityDefinition("rag"),
        CapabilityDefinition("read-image", aliases=frozenset({"READ_IMAGE", "readImage"})),
        CapabilityDefinition("read-text", aliases=frozenset({"READ_TEXT", "readText"})),
        CapabilityDefinition(
            "read-only-analysis",
            aliases=frozenset({"readOnlyAnalysis", "read_only_analysis"}),
        ),
        CapabilityDefinition(
            "read-video",
            aliases=frozenset({"READ_VIDEO", "readVideo", "read_video"}),
            parameters={
                "local-only": CapabilityParameterConstraint("boolean"),
                "max-duration-seconds": CapabilityParameterConstraint(
                    "number", minimum=0, maximum=604_800
                ),
            },
            parameter_aliases={
                "localOnly": "local-only",
                "local_only": "local-only",
                "maxDuration": "max-duration-seconds",
                "maxDurationSeconds": "max-duration-seconds",
                "max_duration": "max-duration-seconds",
                "max_duration_seconds": "max-duration-seconds",
            },
        ),
        CapabilityDefinition(
            "read-youtube",
            aliases=frozenset({"READ_YOUTUBE", "readYouTube"}),
        ),
        CapabilityDefinition("reasoning"),
        CapabilityDefinition("research"),
        CapabilityDefinition(
            "review-code",
            aliases=frozenset({"REVIEW_CODE", "review", "reviewCode"}),
        ),
        CapabilityDefinition("run-tests", aliases=frozenset({"RUN_TESTS", "runTests"})),
        CapabilityDefinition("search-x", aliases=frozenset({"SEARCH_X", "searchX"})),
        CapabilityDefinition("slow"),
        CapabilityDefinition(
            "structured-output",
            parameters={
                "schema-version": CapabilityParameterConstraint(
                    "string",
                    required=True,
                    min_length=1,
                    max_length=128,
                    pattern=r"[A-Za-z0-9][A-Za-z0-9._:/-]*",
                )
            },
        ),
        CapabilityDefinition("use-local-gpu", aliases=frozenset({"USE_LOCAL_GPU", "useLocalGPU"})),
        CapabilityDefinition(
            "use-local-model", aliases=frozenset({"USE_LOCAL_MODEL", "useLocalModel"})
        ),
        CapabilityDefinition("verify-result", aliases=frozenset({"VERIFY_RESULT", "verifyResult"})),
        CapabilityDefinition("web-search", aliases=frozenset({"WEB_SEARCH", "webSearch"})),
    ),
)


@dataclass(frozen=True, slots=True)
class WorkerManifest:
    """Immutable static Worker contract; dynamic health and load are deliberately separate."""

    worker_id: str
    node_id: str
    provider_id: str
    adapter_kind: str
    capabilities: Sequence[str | CapabilityClaim]
    worker_classes: frozenset[str] = frozenset()
    models: tuple[str, ...] = ()
    manifest_revision: int = 1
    schema_version: str = WORKER_MANIFEST_SCHEMA_VERSION
    catalog_version: str = INITIAL_CAPABILITY_CATALOG.version
    locality: WorkerLocality = WorkerLocality.UNKNOWN
    privacy: WorkerPrivacy = WorkerPrivacy.UNKNOWN
    cost_mode: CostMode = CostMode.UNKNOWN
    incremental_cost_usd: float | None = None
    max_concurrency: int = 1
    catalog: InitVar[CapabilityCatalog | None] = None
    digest: str = field(init=False)

    def __post_init__(self, catalog: CapabilityCatalog | None) -> None:
        for label, value in (
            ("worker_id", self.worker_id),
            ("node_id", self.node_id),
            ("provider_id", self.provider_id),
            ("adapter_kind", self.adapter_kind),
        ):
            _require_safe_name(value, label)
        if self.schema_version != WORKER_MANIFEST_SCHEMA_VERSION:
            raise CapabilityError("worker manifest schema_version is unsupported")
        if self.manifest_revision < 1:
            raise CapabilityError("worker manifest revision must be positive")
        if self.max_concurrency < 1:
            raise CapabilityError("worker max_concurrency must be positive")
        if self.incremental_cost_usd is not None and (
            not math.isfinite(self.incremental_cost_usd) or self.incremental_cost_usd < 0
        ):
            raise CapabilityError("worker incremental cost must be finite and non-negative")
        selected_catalog = catalog or INITIAL_CAPABILITY_CATALOG
        if selected_catalog.version != self.catalog_version:
            raise CapabilityError("worker manifest catalog version does not match its validator")
        canonical_claims = selected_catalog.canonicalize_claims(self.capabilities)
        for worker_class in self.worker_classes:
            _require_safe_name(worker_class, "worker class")
        models = tuple(dict.fromkeys(self.models))
        if len(models) != len(self.models):
            raise CapabilityError("worker manifest models must be unique")
        for model in models:
            _require_safe_name(model, "model")
        object.__setattr__(self, "capabilities", canonical_claims)
        object.__setattr__(self, "models", models)
        digest = "sha256:" + hashlib.sha256(_canonical_json(self.canonical_data())).hexdigest()
        object.__setattr__(self, "digest", digest)

    @property
    def capability_names(self) -> frozenset[str]:
        return frozenset(claim.name for claim in self.capabilities)

    def canonical_data(self) -> dict[str, Any]:
        return {
            "adapterKind": self.adapter_kind,
            "capabilities": [claim.to_protocol() for claim in self.capabilities],
            "catalogVersion": self.catalog_version,
            "costMode": self.cost_mode.value,
            "incrementalCostUSD": self.incremental_cost_usd,
            "locality": self.locality.value,
            "manifestRevision": self.manifest_revision,
            "maxConcurrency": self.max_concurrency,
            "models": list(self.models),
            "nodeID": self.node_id,
            "privacy": self.privacy.value,
            "providerID": self.provider_id,
            "schemaVersion": self.schema_version,
            "workerID": self.worker_id,
            "workerClasses": sorted(self.worker_classes),
        }

    def to_protocol(self) -> dict[str, Any]:
        return {**self.canonical_data(), "digest": self.digest}


@dataclass(frozen=True, slots=True)
class WorkerDynamicState:
    """Time-varying observations that must never alter a manifest's canonical digest."""

    worker_id: str
    health: WorkerHealth = WorkerHealth.UNKNOWN
    health_freshness: ObservationFreshness = ObservationFreshness.UNKNOWN
    quota: QuotaAvailability = QuotaAvailability.UNKNOWN
    quota_freshness: ObservationFreshness = ObservationFreshness.UNKNOWN
    subscription_state: SubscriptionState = SubscriptionState.UNKNOWN
    load: float | None = None
    running_tasks: int = 0
    observed_at: datetime | None = None

    def __post_init__(self) -> None:
        _require_safe_name(self.worker_id, "worker_id")
        if self.load is not None and (not math.isfinite(self.load) or not 0 <= self.load <= 1):
            raise CapabilityError("worker load must be between zero and one when known")
        if self.running_tasks < 0:
            raise CapabilityError("worker running_tasks cannot be negative")
        if self.observed_at is not None and self.observed_at.tzinfo is None:
            raise CapabilityError("worker dynamic observation time must be timezone-aware")
        if self.observed_at is None and (
            self.health is not WorkerHealth.UNKNOWN
            or self.health_freshness is not ObservationFreshness.UNKNOWN
            or self.quota is not QuotaAvailability.UNKNOWN
            or self.quota_freshness is not ObservationFreshness.UNKNOWN
            or self.subscription_state is not SubscriptionState.UNKNOWN
            or self.load is not None
        ):
            raise CapabilityError("known dynamic Worker observations require observed_at")

    def to_protocol(self) -> dict[str, Any]:
        return {
            "workerID": self.worker_id,
            "health": self.health.value,
            "healthFreshness": self.health_freshness.value,
            "quota": self.quota.value,
            "quotaFreshness": self.quota_freshness.value,
            "subscriptionState": self.subscription_state.value,
            "load": self.load,
            "runningTasks": self.running_tasks,
            "observedAt": (
                self.observed_at.isoformat().replace("+00:00", "Z")
                if self.observed_at is not None
                else None
            ),
        }


def validate_manifest_digest(value: str) -> str:
    if not _SHA256.fullmatch(value):
        raise CapabilityError("worker manifest digest must be a sha256 value")
    return value
