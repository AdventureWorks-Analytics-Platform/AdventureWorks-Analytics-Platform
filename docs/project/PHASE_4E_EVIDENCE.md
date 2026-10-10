# Phase 4E Evidence

Date: 2026-09-07

This document records the executable evidence for the implemented Phase 4E orchestration and delivery boundary. Commands were run from the repository root with `.venv`.

## CLI Help

Command:

```powershell
python -m src.app.cli --help
```

Verified options:

- `--mode {full,incremental}`
- `--stage {full,bronze,silver,gold}`
- `--log-level {DEBUG,INFO,WARNING,ERROR,CRITICAL}`
- `--recovery-snapshot`
- `--source-snapshot-id`
- `--report`
- `--report-format {json,markdown}`

## Unit Selection

Command:

```powershell
python -m pytest -m "not integration" -q
```

Result:

```text
211 passed, 33 deselected
```

The 33 deselected tests are explicitly marked `integration` and require
PostgreSQL and/or SQL Server.

## Focused Delivery Tests

Command:

```powershell
python -m pytest tests/test_pipeline_cli.py tests/test_pipeline_runner.py -q
```

Result:

```text
25 passed
```

Coverage includes CLI option validation, status-to-exit-code mapping, JSON/Markdown rendering, report metadata, invalid recovery input, and report-delivery failure.

## Current Split Integration Evidence

PostgreSQL-only command:

```powershell
python -m pytest -m "postgres_integration and not sqlserver_integration" -q
```

Result:

```text
29 passed, 215 deselected
```

SQL Server selection was verified to contain four tests. Execution requires a
configured AdventureWorks SQL Server source; it was not run as part of this
local PostgreSQL validation.

## Static Checks

```text
Black on changed CLI/report files: PASS
compileall: PASS
git diff --check: PASS
```

The earlier non-blocking quality baseline has been remediated. Current quality
commands all pass and the CI quality job is now blocking:

```text
Black --check: PASS (63 files)
Flake8: PASS
MyPy: PASS (56 source files)
Unit tests: 211 passed, 33 deselected
```

Structured logging uses the shared JSON event contract in
`src/shared/observability/structured_logging.py`. Pipeline, Bronze, Silver,
Gold, PostgreSQL, and SQL Server boundaries emit correlated lifecycle,
table, batch, retry, and adapter events. Focused tests verify event fields,
pipeline correlation, and rejection of unapproved payload fields.

## Integration and CI Policy

Integration coverage is split by dependency. The verified PostgreSQL lane uses an
ephemeral PostgreSQL 15 database initialized from the warehouse schema script
and runs `postgres_integration` tests independently of SQL Server. The
SQL Server lane is enabled only when `SQL_SERVER_HOST` is configured and runs
`sqlserver_integration` tests using the configured AdventureWorks source.

See [INTEGRATION_TESTING.md](INTEGRATION_TESTING.md) for local setup and CI
configuration. SQL Server coverage remains conditional on an external source
database; its absence does not block the independently reproducible
PostgreSQL lane.

## Result Contract Closure

Gold publication now exposes `current_pointer` through `GoldRunResult` and the runner/report result. Generic runner failures expose the documented `error_message` field while retaining `error` as a compatibility alias. Top-level aggregate row counts are also preserved alongside the nested `counts` mapping.

Focused contract validation:

```text
python -m pytest tests/test_pipeline_runner.py tests/test_pipeline_cli.py tests/test_gold_publish.py -q
25 passed
```

## Recovery References

- CLI and stage prerequisites: [README.md](../../README.md)
- Operational recovery: [PHASE_4E_RUNBOOK.md](PHASE_4E_RUNBOOK.md)
- Implementation checklist and evidence log: [phase4e_Orchestration&Delivery_execution.md](../ToDoCheckList/Phase_4_Review&Enhance_Code/phase4e_Orchestration&Delivery_execution.md)
- CI workflow: [ci.yml](../../.github/workflows/ci.yml)
