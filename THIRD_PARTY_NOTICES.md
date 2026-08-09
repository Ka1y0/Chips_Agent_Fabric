# Third-party notices

CHIPS Agent Fabric does not vendor third-party source code. Python dependencies
are installed from their upstream distributions and remain under their own
licenses.

The direct runtime and development dependencies declared in `pyproject.toml`
were reviewed from the installed distribution metadata for the V0.1 gate:

| Dependency | Observed license identifier |
|---|---|
| FastAPI | MIT |
| HTTPX | BSD-3-Clause |
| Pydantic | MIT |
| Uvicorn | BSD-3-Clause |
| Hatchling | MIT |
| jsonschema | MIT |
| pytest | MIT |
| pytest-asyncio | Apache-2.0 |
| Ruff | MIT |

This inventory is evidence from the verified development environment, not a
substitute for checking the dependency set embedded in each future release.
Release automation must regenerate and review the dependency inventory whenever
dependencies change.

Project_Bridge was inspected as a separate local research prototype. It has no
license file, so no Project_Bridge code is copied or distributed here. The
Fabric documents only an independently designed interoperability boundary based
on observable protocol behavior.
