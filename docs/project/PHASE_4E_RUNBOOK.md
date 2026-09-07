# Phase 4E Runbook

## Run

Start PostgreSQL and configure SQL Server/PostgreSQL settings, then run:

```text
python -m src.app.cli --mode full --stage full --report logs/pipeline.json
python -m src.app.cli --mode full --stage full --log-level INFO --report logs/pipeline.json
```

Use `--report-format markdown` for an operator-readable report. Reports are written by the CLI after the runner returns; stage code does not write files.

## Stage prerequisites

- `full` runs health, bootstrap, Bronze, Silver, the Silver gate, and Gold.
- `bronze` runs only Bronze and produces a snapshot for later recovery.
- `silver` requires `--recovery-snapshot` with an explicit valid Bronze snapshot.
- `gold` requires `--source-snapshot-id`; it never discovers the latest Silver snapshot.
- `--mode` accepts `full` and `incremental`.
- `--log-level` accepts `DEBUG`, `INFO`, `WARNING`, `ERROR`, and `CRITICAL`.
- `--report-format` accepts `json` and `markdown`; JSON reports include `report_schema_version`.

Exit codes are `0` for `SUCCESS` and policy-approved `SUCCESS_WITH_REJECTIONS`, `2` for `PARTIAL_SUCCESS`, and `1` for `FAILED` or report-delivery failure.

## Failure handling

Connectivity failure stops before bootstrap. Bootstrap creates schemas and metadata idempotently, then checks required metadata and schema version before data mutation. A failed readiness check stops all data stages. A failed stage leaves the previously published current pointer unchanged; inspect the report's `failed_stage` and rerun after correcting the dependency.

## Staging cleanup and preservation

Do not delete the currently published Bronze, Silver, or Gold pointer when recovering a failed run. First inspect the report and durable audit/checkpoint records for the `run_id` and snapshot. Remove only abandoned run-specific staging objects after reconciliation confirms that no retry or unknown commit remains. Use the existing staging cleanup/reconciliation services or their operational scripts; do not issue broad schema-level `DROP`, `TRUNCATE`, or `DELETE` commands.

For Gold failures, preserve the previous `gold_current_pointer` version. For Silver failures, preserve the previous `silver_current_pointer` version. Repair the dependency, clean only the failed run's staging state, and rerun with the same explicit recovery or source snapshot input.

## Report lookup

JSON and Markdown reports are written to the path supplied with `--report`. The JSON report is the machine-readable source and includes `run_id`, `status`, `failed_stage`, stage results, counts, report schema version, and report paths. Use the `run_id` with the Bronze audit/reconciliation records when investigating a failed stage. A report write failure returns a non-zero exit code and must be treated as a delivery failure even when data stages completed.

Run unit tests with:

```text
python -m pytest -m "not integration" -q
```

Database-backed tests are marked `integration` and require configured external services.

CI separates unit and integration work. The unit lane runs `-m "not integration"` without external databases. The integration lane provisions PostgreSQL, checks PostgreSQL and SQL Server availability, and records unavailable prerequisites as a blocked/non-success result. Black, Flake8, and MyPy outputs are retained as CI artifacts; the current repository has pre-existing quality debt, so that quality lane is informational until the baseline is reduced.