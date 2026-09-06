# Executable foundations toward Personal Intelligence Fabric

Status: implemented local HTTP probe and optional lossless wire codec, with focused tests.
This is not zero-touch deployment, automatic tuning, real GUI control, an elastic cluster,
or verified semantic compression between live LLMs. The existing release gates still apply.

## Local model probe: executable, opt-in

After installing this branch, `chips-model-probe` uses the existing HTTPX dependency.
No separate model SDK is required. The selected model server must already be running.

```sh
# Dry-run: validates the endpoint and prints JSON without opening a socket.
chips-model-probe --runtime lmStudio --endpoint http://127.0.0.1:1234/v1

# Explicitly permit a model catalog request, but no inference or model load request.
chips-model-probe --runtime lmStudio --endpoint http://127.0.0.1:1234/v1 --allow-network

# Replace EXACT_MODEL_ID with an ID returned by the catalog. This may load the model.
chips-model-probe --runtime lmStudio --endpoint http://127.0.0.1:1234/v1 \
  --model EXACT_MODEL_ID --allow-network --allow-inference

# Ollama uses its native API rather than guessing OpenAI-compatible fields.
chips-model-probe --runtime ollama --endpoint http://127.0.0.1:11434 --allow-network
```

`llamaCpp` and `openAICompatible` use the same explicit `/v1/models` and
`/v1/chat/completions` routes as the LM Studio compatible path. There is no port scan,
provider login, credential lookup, downloaded-model installation, service start, or auto-enrollment.
Protected endpoints return `authRequired`. Do not put authentication in endpoint URLs.

Inference requires both flags and an exact catalog model ID. It sends only the fixed synthetic
prompt `Reply with exactly the word OK.`, with a small requested output limit and no tools.
An exact terminal reply and matching model ID are required for `inferenceVerified`.
Model aliases, reasoning-heavy outputs, or a different response may fail this narrow smoke test;
such a failure does not prove the model lacks general reasoning or inference capability.
Server output limits are requests, not a guarantee about hidden reasoning or provider-side compute.

The JSON report separates catalog visibility, verified text response, timing, provider-reported
usage, and expiry. The existing local-model profiler is reused, with unknown context, memory,
tool/vision/reasoning support and parallel capacity retained. Non-streaming response time is not
TTFT. Provider-reported decode throughput is not treated as a measured routing benchmark.
A loopback endpoint can proxy remote inference, so execution locality remains `unknown`.
The resulting profile does not authorize or register a Worker.

The live client pins `localhost` to `127.0.0.1`, supports explicit IPv6 loopback, ignores inherited
proxy configuration, refuses redirects/compressed responses, and bounds JSON bytes, model count,
and total operation time. TLS verification remains enabled; a localhost-only certificate may not
match the pinned IP. Do not disable verification to work around that mismatch.
Cancellation closes this client's connection; it does not certify that the server stopped work.
No raw model output is retained. Reports contain local model IDs and endpoints, so review them
before sharing. The command writes only stdout and never applies runtime settings.

Reference contracts: [LM Studio compatible API](https://lmstudio.ai/docs/developer/openai-compat),
[Ollama model catalog](https://docs.ollama.com/api/tags), and
[Ollama chat](https://docs.ollama.com/api/chat).

## Bridge: real wire encoding, unchanged authority boundary

`ZlibJSONCodec` implements an optional `zlib-json/1.0` wire format. Register it explicitly:

```python
from project_supervisor.bridge import BridgeConfig, BridgeFoundation
from project_supervisor.bridge.zlib_codec import ZLIB_JSON_CODEC, ZlibJSONCodec

bridge = BridgeFoundation(
    BridgeConfig(enabled=True),
    codecs={ZLIB_JSON_CODEC: ZlibJSONCodec()},
)
```

Both peers must advertise the codec. Existing default behavior stays disabled/identity-only.
The wire frame is `FZ1\0` plus `Z` for zlib or `J` for raw canonical JSON. An incompressible
body uses raw mode; framing overhead is still subject to the envelope limit. Payload limits are
not bypassed by compression. Decoding enforces a maximum output size before allocation can grow
without bound, rejects truncation/concatenated streams, then uses the existing schema and full
SHA-256 envelope validation. Hashes detect corruption, but do not authenticate a sender.

This reduces transfer bytes on suitable content, not model tokens. A receiver process reconstructs
ordinary structured context before giving it to a model. There is no claim that an LLM understands
compressed bytes or that the existing semantic-mode selector implements a learned codec.
The existing Foundation round-trip and forbidden-authority fallback remain in charge; no new
network listener, Worker dispatcher, permission grant or canonical-state writer is introduced.

## Five-goal implementation sequence

| Goal | This slice | Next concrete acceptance gate |
|---|---|---|
| Low-friction installation | One packaged probe command and explicit first-run instructions | Signed/reviewed installer with resumable operations, state preservation and rollback on fresh hosts |
| Real OS/browser Computer Use | No new GUI backend | One allowlisted browser workflow through semantic IDs, resource leases, cancellation and post-action verification |
| Automatic local LLM adaptation | Catalog and synthetic inference probes feed timestamped existing profiles | Persist benchmark observations; validate proposed settings, explicitly apply one bounded change, then verify or roll back |
| Durable large-scale clusters | Existing planning and spawn governance unchanged | Persist one expanded DAG through the canonical task store, resume after a killed host, prove no duplicate dispatch under bounded load |
| High-density Bridge communication | Negotiable bounded lossless codec and cross-process round-trip tests | Two real authorized Workers exchange a task-scoped artifact; compare exactness, bytes, latency and token use against unchanged structured fallback |

The next integration should bind probe reports to the existing observation registry and task
permission model, not treat a CLI success as a capability grant or add a second canonical database.
Do not extend to automatic settings changes until cancellation, consent and rollback are demonstrated.

## Validation and known blocker

`tests/test_local_probe.py` tests mock providers and a real loopback HTTP socket.
`tests/test_bridge_zlib.py` tests framing, bounded decompression, the existing Bridge integration,
and a separate receiver process. No live provider account, real LLM weights, or user machine is used.
The independent executable-foundations CI job validates these tests, package builds and a clean
wheel-installed dry-run command even when an unrelated full-suite gate stalls. It does not replace
or excuse the full suite or any existing acceptance job.

Issue #2 remains open for autonomous-host lease turnover and full-suite stalls. A successful focused
slice is not release readiness. `main`, the package version and database migrations are unchanged.
