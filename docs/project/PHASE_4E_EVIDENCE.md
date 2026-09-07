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
182 passed, 22 deselected
```

The 22 deselected tests are explicitly marked `integration` and require PostgreSQL and/or SQL Server.

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

## Full Regression

Command:

```powershell
python -m pytest -q
```

Result:

```text
204 passed
```

## Static Checks

```text
Black on changed CLI/report files: PASS
compileall: PASS
git diff --check: PASS
```

The broader repository Black and Flake8 baseline contains pre-existing formatting and lint debt. CI runs those checks and retains their logs as artifacts, but the quality lane is currently informational/non-blocking until that baseline is reduced.

Current quality command results:

```text
Black: fails; 37 files would be reformatted
Flake8: fails on existing E501 and related baseline issues
MyPy: fails with 102 existing/type-stub errors across 22 files
```

The checks are present and executed by CI; these failures are recorded blockers for making the quality lane required.

## Integration and CI Policy

The CI integration lane provisions PostgreSQL, checks PostgreSQL and SQL Server prerequisites, runs marked integration tests only when prerequisites are available, and uploads prerequisite/test logs. Missing external services produce blocked/non-success evidence and are not reported as application success.

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
