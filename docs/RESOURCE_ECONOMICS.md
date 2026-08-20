# Resource economics visibility FOUNDATION

Status: **V0.3 FOUNDATION** built on the accepted V0.2/V0.2.1 invocation telemetry and resource
ledger. Cyber Office consumes these values; it does not calculate or persist canonical usage.

`GET /v1/resources/economics` exposes four normalized foundations:

- `codexOffloadRatio`: offloaded classified calls divided by Codex plus offloaded classified calls.
  Calls with unknown executor identity are excluded from the denominator and reported in existing
  telemetry coverage. Provenance is `LOCALLY_MEASURED`.
- `qualityAdjustedOffload`: the sum of observed quality scores for offloaded calls divided by the
  sum for scored classified calls. It is `INFERRED`; unscored/unknown calls never receive fabricated
quality.
- `tokensPerSuccessfulGoal`: total observed input plus output tokens divided by Goals durably
  terminated with `SUCCESS`.
- `tokensPerAcceptedTask`: total observed input plus output tokens divided by Tasks in canonical
  `succeeded` state. In the autonomous driver this is the durable accepted Task outcome; it remains
  distinct from Goal success.

Cache counters are not added to input plus output totals because providers may report them as a
subset of input tokens. Each per-outcome token rate is known only when every eligible Goal/Task has
at least one invocation and every included invocation reports both token dimensions. Otherwise the
whole rate is `UNKNOWN`/`notReported`; observed partial sums are never divided into a misleading
rate. Coverage counts disclose complete and unknown invocations, Codex/offloaded/unknown executor
classification, scored/unscored calls, and accepted outcomes.

The endpoint supports optional `projectID` and `goalID` scopes and rejects mismatched scope pairs.
Known derived token rates have `INFERRED` provenance. Missing model/provider subscription quota,
remaining balance, reset time, cost, or token counts remain unavailable; this endpoint does not call
an AI model or provider status service.

Machine contract: `schemas/resource-economics-v1.schema.json`.
