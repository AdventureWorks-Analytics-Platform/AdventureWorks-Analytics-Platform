# Phase 4E - Orchestration and Delivery Execution Plan

## 1. Purpose

This document translates the approved Phase 4 orchestration and delivery baseline into an implementation scope, impact analysis, task breakdown, and verifiable acceptance criteria.

Phase 4E covers:

1. Implement a health/readiness gate.
2. Replace the bootstrap placeholder with an idempotent bootstrap and schema readiness check.
3. Create `PipelineRunner` and a documented CLI.
4. Add process exit codes and machine-readable summary reports.
5. Mark integration tests and establish CI checks.
6. Update README, runbook, checklist, and operational evidence.

Phase 4E coordinates the completed or approved Bronze, Silver, and Gold boundaries. It does not duplicate their transformation, retry, staging, or validation mechanics.

### 1.1 Approved decisions

The following decisions are the implementation contract for Phase 4E. Where earlier sections differ, these decisions take precedence.

| Topic | Approved decision |
|---|---|
| Canonical orchestration | Extend the existing `PipelineRunner` and keep one production orchestration path. `PipelineRunner` coordinates stages; it does not reimplement Bronze, Silver, or Gold mechanics. |
| Gold ownership | `GoldJob` owns Gold build, integrity validation, constraints, KPI validation, version publication, and current-pointer update. The runner invokes the job and evaluates its final result; it does not duplicate Gold validation. |
| Full-run partial policy | In a full pipeline, any required Bronze target failure blocks Silver and Gold. `PARTIAL_SUCCESS` is not downstream-eligible. `SUCCESS_WITH_REJECTIONS` is allowed only when rejection policy permits it. |
| Status and exit code | `SUCCESS` and policy-approved `SUCCESS_WITH_REJECTIONS` return exit code `0`. `PARTIAL_SUCCESS` and `FAILED` return non-zero for a full pipeline. |
| Readiness ordering | Before bootstrap, validate Settings and dependency connectivity only. After bootstrap, validate schemas, required tables, metadata, and compatible versions before data-stage mutation. |
| Stage selection | `full` runs health -> bootstrap -> Bronze -> Silver -> Silver gate -> Gold. Standalone Silver requires an explicit valid Bronze snapshot/recovery input. Standalone Gold requires an explicit valid Silver `source_snapshot_id`; it must not auto-discover the latest snapshot. Unsupported combinations fail before mutation. |
| Snapshot propagation | The runner passes `source_snapshot_id` and upstream stage identity into Gold without loss. Gold results retain `pipeline_snapshot_id`, `source_snapshot_id`, `gold_run_id`, `gold_load_id`, `gold_version`, current-pointer state, KPI state, and publication state. |
| Gold failure | Gold failure returns pipeline `FAILED` with `failed_stage="gold"`; Bronze and Silver are not rolled back, and the previous valid Gold version remains active. |
| Application compatibility | Preserve the dictionary-returning application behavior through `run_application() -> dict`; provide a CLI-facing `main() -> int` and use `SystemExit` at the process boundary. `App.run()` maps the runner's terminal status to a coarse `ok`/`degraded` field: only `SUCCESS` and `SUCCESS_WITH_REJECTIONS` map to `ok` (decision 2026-09-09; `PARTIAL_SUCCESS` previously mapped to `ok`, which contradicted the CLI's non-zero exit-code policy for `PARTIAL_SUCCESS`). |
| Report delivery | JSON and Markdown are renderings of the same structured result. Report generation failure is a non-success delivery outcome and must not be silently treated as pipeline success. |
| Integration prerequisites | Local unit selection may skip unavailable external services with an explicit reason. Production DoD remains blocked/non-success until required integration evidence exists or the blocker is recorded. |

### 1.2 Stage and exit-code policy

The full pipeline distinguishes terminal statuses as follows:

| Status | Meaning | Full-pipeline downstream eligibility | Exit code |
|---|---|---:|---:|
| `SUCCESS` | All required stages pass without approved rejection | Yes | `0` |
| `SUCCESS_WITH_REJECTIONS` | Required stages pass and rejected rows remain within policy | Yes | `0` |
| `PARTIAL_SUCCESS` | A stage or domain did not fully complete, even if policy records partial context | No | Non-zero |
| `FAILED` | A required gate, stage, validation, publication, or delivery step failed | No | Non-zero |

`PARTIAL_SUCCESS` may be represented for diagnostics or an explicitly supported recovery operation, but it cannot satisfy the full-pipeline success policy.

### 1.3 Readiness and stage-selection rules

Readiness is split into two gates:

```text
Settings validation
  -> pre-bootstrap connectivity check
      -> idempotent bootstrap
          -> post-bootstrap schema/table/metadata/version readiness
              -> data stages
```

The runner must reject these selections before mutation:

```text
full   -> health -> bootstrap -> Bronze -> Silver -> Silver gate -> Gold
bronze -> health -> bootstrap -> Bronze
silver -> explicit valid Bronze snapshot/recovery input required
gold   -> explicit valid Silver source_snapshot_id required
```

Standalone Silver and Gold runs must not infer a missing upstream snapshot from the latest available database state.

## 2. Dependencies and target workflow

Phase 4E depends on:

- Phase 4A settings, shared result/status contracts, and domain ownership.
- Phase 4B Bronze jobs, audit, quarantine, staging, publish, retry, and reconciliation.
- Phase 4C Silver job, validation gate, snapshot/load contract, and published-table metadata.
- Phase 4D Gold job, pre-publish integrity/KPI validation, constraints, versioned publication, and current pointer.

Target execution order:

```text
load and validate Settings
    -> health/readiness gate
        -> idempotent bootstrap and schema version check
            -> Bronze domain jobs
                -> Silver transformation
                    -> Silver snapshot gate
                        -> GoldJob
                            -> Gold build, integrity/KPI validation, and current-pointer publication
                                    -> summary report and exit code
```

A failed configuration, connectivity/readiness check, bootstrap, required Bronze domain, Silver transformation/gate, GoldJob, or report-delivery step must produce a non-success result and prevent unsafe downstream execution.

## 3. Baseline and current implementation

### 3.1 Confirmed implementation surface

> **Runtime reconciliation (2026-09-07):** Phase 4E orchestration, bootstrap, CLI/report delivery, integration marker policy, and CI workflow are implemented and locally verified. Remaining production evidence is limited to external-service CI/integration availability and reducing the pre-existing quality baseline debt.

| Area | Current location | Current behavior | Phase 4E concern |
|---|---|---|---|
| Application entrypoint | `main.py`, `src/app/cli.py` | `run_application()` preserves the dictionary API; CLI `main()` returns an exit code and the process boundary uses `SystemExit` | CLI/report behavior must remain aligned with the documented status matrix |
| Application orchestration | `src/app/app.py`, `src/app/pipeline_runner.py` | Runs Settings/health -> idempotent bootstrap/readiness -> Bronze/Silver -> Gold; `App` maps internal success statuses to external `ok` | Full lifecycle and stage orchestration exist; broader CLI, exit-code, report, and CI contracts remain |
| Health service | `src/shared/services/connection_health_service.py` | Returns sanitized pre-bootstrap configuration/connectivity checks and `ok`/`degraded` aggregate status | Runner must enforce the configuration/connectivity gate before bootstrap |
| Bootstrap | `src/jobs/platform_bootstrap.py` | Creates platform metadata idempotently and returns post-bootstrap schema/metadata/version readiness checks | Required object inventory and compatible version must remain aligned with migrations |
| Silver | `src/features/Sales_Performance/jobs/sales_silver_job.py`, `PostgresSilverPublishService` | Injectable job writes a complete `silver_v<SNAPSHOT>` candidate and updates `silver.silver_current_pointer` after all six targets pass | Phase 4E must treat the Silver pointer/gate as an upstream contract, not reimplement Silver publication |
| Gold | `src/features/Sales_Performance/jobs/sales_gold_job.py`, PostgreSQL Gold adapters | Injectable Gold job owns fact batches, validation, constraints, KPI, candidate schema, audit, and atomic current pointer | Phase 4E must invoke/evaluate Gold result without duplicating Gold mechanics |
| Result contract | `src/shared/ingestion/ingestion_models.py` | Shared ingestion statuses and table result shape | Runner needs stage/pipeline result aggregation without losing table context |
| Test configuration | `pytest.ini`, `tests/` | Registers `integration`; live SQL Server/PostgreSQL modules are marked and unit selection uses `-m "not integration"` | New database tests must carry the marker |
| CI | `.github/workflows/ci.yml` | Unit, quality, and prerequisite-aware integration jobs retain logs/results as artifacts | Quality baseline remains non-blocking until existing debt is reduced; unavailable integration prerequisites are blocking evidence |
| Documentation | `README.md`, `docs/project/PHASE_4E_RUNBOOK.md`, `docs/project/PHASE_4E_EVIDENCE.md`, Phase 4 docs | One-command execution, recovery, exit codes, test selection, CI policy, and evidence are documented | Keep commands and evidence synchronized with runtime |

### 3.2 Falsifiable orchestration hypothesis

The primary orchestration defect is that `App.run()` ignores health status when deciding whether to execute bootstrap and Bronze. A fake service test that returns degraded health and records calls should show that bootstrap and all data jobs are not invoked. A second fake-stage test should show that a failed Silver validation prevents Gold invocation. If either test records downstream calls, the gate belongs in the runner boundary rather than in individual jobs.

**Current evidence:** `PipelineRunner` stops before bootstrap/data stages when Settings or health validation fails, stops before data stages when post-bootstrap readiness is not ready, stops before Gold when Bronze/Silver returns non-success, and propagates the pipeline snapshot plus Silver result into `SalesGoldJob`.

### 3.3 Compatibility surface

Unless a separate compatibility decision is recorded, preserve:

- `App(bootstrap_job=None, health_service=None, bronze_job=None, settings=None)` construction and `App.run()` availability.
- The existing application invocation remains available through a dictionary-returning application function; the process-facing `main() -> int` is the CLI boundary and exits through `SystemExit`.
- `PlatformBootstrapJob.run()` as a callable bootstrap surface, with its response evolved compatibly where practical.
- Existing Bronze/Silver/Gold job and script entrypoints through delegating adapters.
- Existing test commands and import paths during migration.

Compatibility wrappers must delegate to the new runner or service and must not contain a second orchestration implementation.

## 4. Target architecture and contracts

### 4.1 Component boundaries

| Component | Owns | Must not own |
|---|---|---|
| Settings boundary | Load `.env`, process overrides, typed validation, safe summary | Stage execution or connection retry |
| Health/readiness service | Pre-bootstrap dependency connectivity and post-bootstrap schema/table/metadata/version readiness report | Starting data jobs |
| `PlatformBootstrapJob` | Idempotent schemas, metadata tables, migrations, version check | Transforming or publishing data |
| `PipelineRunner` | Stage order, stage gates, failure policy, result aggregation, report generation, and Gold result propagation | Bronze/Silver/Gold transformation, validation, retry, staging, or publication mechanics |
| CLI adapter | Parse mode/stage/log/report options and map result to exit code | Reimplementing runner behavior |
| Stage jobs | Their own transformation/loading contract | Calling unrelated downstream stages |
| Summary reporter | JSON/Markdown serialization of run evidence | Hiding or rewriting failed status |
| Test/CI configuration | Unit/integration selection and quality commands | Making unavailable external services look successful |

### 4.2 Pipeline result contract

`PipelineRunner.run()` must return a machine-readable result with at least:

```text
run_id, pipeline_name, mode, requested_stages,
status, started_at, finished_at, duration_ms,
health, bootstrap, stages, failed_stage,
rows_read, rows_written, rows_rejected,
error_type, error_message, report_paths
```

The nested Gold result must preserve the Phase 4D contract without flattening away lineage:

```text
pipeline_snapshot_id, source_snapshot_id,
gold_run_id, gold_load_id, gold_version,
current_pointer, published, kpi_passed,
constraints_verified, previous_version
```

Each stage result must preserve its standard table/batch results and include:

```text
stage, status, started_at, finished_at,
duration_ms, counts, errors, publication_state
```

Terminal statuses are `SUCCESS`, `SUCCESS_WITH_REJECTIONS`, `PARTIAL_SUCCESS`, and `FAILED`. A pipeline is exit-code success only when every required stage satisfies its success policy. Report serialization must use the same structured result for JSON and Markdown and must not recalculate status independently.

### 4.3 Required stage policy

| Condition | Runner behavior | Terminal result |
|---|---|---|
| Settings validation fails | Stop before health check | `FAILED` |
| Health/readiness fails | Do not run bootstrap or data stages | `FAILED` |
| Bootstrap fails/version incompatible | Do not run data stages | `FAILED` |
| Bronze domain has allowed partial failure | Record all domain results; in a full run, any missing/failed required target blocks Silver and Gold | `PARTIAL_SUCCESS` or `FAILED` |
| Required Bronze failure | Stop before Silver | `FAILED` |
| Silver transformation fails | Do not run Silver validation or Gold | `FAILED` |
| Silver validation fails | Do not run Gold | `FAILED` |
| Gold build/validation/publish fails | Preserve prior Gold; no success exit | `FAILED` |
| All required stages pass with no rejection | Write reports and return exit code `0` | `SUCCESS` |
| Approved rejected rows within policy | Write reports and return exit code `0` | `SUCCESS_WITH_REJECTIONS` |
| Partial stage/domain completion in full run | Preserve diagnostic context but do not claim downstream success | `PARTIAL_SUCCESS`, non-zero |

## 5. Impact analysis

### 5.1 Code and runtime impact

| Impacted area | Expected change | Risk | Mitigation/evidence |
|---|---|---|---|
| `App` | Delegate to `PipelineRunner` while preserving constructor and `run()` | Existing callers may expect current result keys | Compatibility test for constructor, result shape, and injected fakes |
| Health gate | Treat Settings and dependency connectivity as a pre-bootstrap hard precondition, then run schema/version readiness after bootstrap | Partial processing may already occur if gate is late or readiness is checked at the wrong boundary | Tests assert zero bootstrap/data calls on connectivity failure and zero data calls on post-bootstrap readiness failure |
| Bootstrap | Replace hard-coded response with idempotent schema/metadata/version operations | Existing database may have partial or older objects | Migration/version tests, non-destructive rerun test, explicit incompatible-version failure |
| Stage wiring | Inject Bronze/Silver/Gold jobs and gate/report services | Hidden constructor defaults can make tests require live services or duplicate Gold validation | Dependency injection, fake-stage tests, and GoldJob ownership test |
| CLI | Add `full`, `incremental` where supported, stage selection, log level, report path, and help | Invalid options could silently select unsafe behavior | Parser tests and strict mode validation |
| Exit status | Map pipeline result to process code while preserving dictionary-returning application compatibility | Automation may treat degraded/partial result as success or break existing callers | `run_application() -> dict`, `main() -> int`, exit-code matrix, and subprocess smoke test |
| Reports | Generate JSON and Markdown from one structured result | Reports may omit failed stage, expose secrets, or fail after pipeline completion without affecting outcome | Fixture tests, schema assertions, redaction tests, and report-failure policy test |
| Pytest | Register `integration` marker and separate default commands | Local test run may hit unavailable services | Marker configuration and `pytest -m "not integration"` evidence |
| CI | Add unit, lint, format, type-check, and integration-aware jobs | CI environment may lack database prerequisites | Local unit selection may skip with explicit reason; Production DoD remains blocked/non-success without integration evidence |
| Documentation | Update README, runbook, checklist, and evidence | Docs can drift from CLI behavior | Command/help output and documented recovery steps reviewed together |

### 5.2 Operational impact

- Operators receive one documented command for a full run and explicit stage-selection commands for recovery.
- Settings/connectivity failures stop processing before bootstrap; post-bootstrap schema, metadata, and version readiness failures stop processing before Bronze, Silver, or Gold mutation.
- Every run has a stable `run_id`, stage status, timing, counts, failure context, report path, and exit code.
- A failed stage or report-delivery failure does not masquerade as a successful pipeline because a report was generated.
- Stage selection must respect dependencies. Running Silver alone requires an explicit valid Bronze snapshot/recovery input; running Gold alone requires an explicit valid Silver `source_snapshot_id` and must not auto-discover the latest snapshot.
- Reports contain safe metadata only; passwords, connection strings with credentials, and raw payloads are excluded.
- Integration failures caused by unavailable PostgreSQL/SQL Server are classified as environment prerequisites, not converted into passing evidence. Local unit runs may skip them with an explicit reason; the Production DoD remains blocked until evidence exists or the blocker is recorded.

### 5.3 Out of scope and decisions required

| Item | Treatment |
|---|---|
| New transformation logic | Out of scope; owned by Phase 4B-4D stage jobs |
| Scheduler deployment | Out of scope; CLI and exit code must be scheduler-ready |
| Cloud secrets manager | Out of scope; use centralized Settings contract and document secret injection boundary |
| Unsupported partial stage runs | Must fail clearly; do not infer missing upstream snapshots |
| Domain partial-success policy | In a full run, required Bronze target failure blocks Silver/Gold; `PARTIAL_SUCCESS` is diagnostic only and is never full-pipeline success |
| Migration rollback policy | Bootstrap must document compatible version checks and non-destructive recovery; destructive rollback is not implicit |

## 6. Task breakdown

### 6.1 Health/readiness gate

| ID | Task | Output | Acceptance criteria | Dependency | Status |
|---|---|---|---|---|---|
| 8.1.1 | Define readiness contract | Pre/post-bootstrap readiness result model | Result distinguishes configuration, connectivity, schema, table, metadata, and version failures by gate phase | W1-W3 | Done |
| 8.1.2 | Add safe health diagnostics | Sanitized dependency checks | Report includes dependency name/status/reason without credentials | 8.1.1 | Done |
| 8.1.3 | Add post-bootstrap readiness checks | Required object inventory | Missing schema/table/metadata/version is reported after bootstrap and before any data job starts | W4-W7 | Done |
| 8.1.4 | Enforce hard gates | Runner pre-bootstrap and post-bootstrap gates | Connectivity failure produces no bootstrap call; post-bootstrap readiness failure produces no data-stage call | 8.1.2/8.1.3 | Done |
| 8.1.5 | Test health gate | Fake health/readiness tests | SQL Server/PostgreSQL failure and missing object scenarios stop processing deterministically | 8.1.4 | Done |

### 6.2 Real bootstrap and schema versioning

| ID | Task | Output | Acceptance criteria | Dependency | Status |
|---|---|---|---|---|---|
| 8.2.1 | Inventory bootstrap objects | Schema/metadata inventory | Required Bronze/Silver/Gold/ingestion objects and versions are documented | W0-W7 | Done |
| 8.2.2 | Implement idempotent bootstrap | `PlatformBootstrapJob` implementation | Running bootstrap twice creates no destructive changes and returns the same compatible version | 8.2.1 | Done |
| 8.2.3 | Add schema version table/check | Version contract | Current version is recorded; incompatible or unsupported version fails clearly | 8.2.2 | Done |
| 8.2.4 | Separate DDL and data operations | Bootstrap migration boundary | Bootstrap does not drop published Bronze/Silver/Gold data | 8.2.2 | Done |
| 8.2.5 | Test bootstrap failure/retry | Unit and database-backed tests | Partial setup is reported; rerun repairs allowed missing objects without destroying valid data | 8.2.2/8.2.3 | Done |

### 6.3 Pipeline runner and stage orchestration

| ID | Task | Output | Acceptance criteria | Dependency | Status |
|---|---|---|---|---|---|
| 8.3.1 | Define stage registry/order | Runner contract | Full order is Settings -> connectivity -> bootstrap -> readiness -> Bronze -> Silver -> Silver gate -> GoldJob; Gold validation/publication remain inside GoldJob | 8.1/8.2 | Done |
| 8.3.2 | Create `PipelineRunner` | Injectable runner | Runner accepts settings, stage jobs, gates, bootstrap, health service, reporter, and clock dependencies without duplicating Gold validation | 8.3.1 | Done |
| 8.3.3 | Implement stage selection | Selection policy | `full`, `bronze`, `silver`, and `gold` validate explicit upstream snapshot/recovery prerequisites and fail before mutation when unsupported | 8.3.2 | Done |
| 8.3.4 | Implement failure policy | Stop/continue policy | Failed required stage blocks dependent stages while preserving prior result context | 8.3.2 | Done |
| 8.3.5 | Aggregate counts and statuses | Pipeline result | Stage and table counts/errors are retained without flattening away failed context | 8.3.4 | Done |
| 8.3.6 | Test stage sequencing | Fake-stage tests | Success order and every required failure gate are verified without a live database | 8.3.5 | Done |

### 6.4 CLI, exit codes, and summary reports

| ID | Task | Output | Acceptance criteria | Dependency | Status |
|---|---|---|---|---|---|
| 8.4.1 | Define CLI contract | `python -m ... --help` or documented command | Help lists mode, stage, log level, report format/path, and prerequisite behavior | 8.3 | Done |
| 8.4.2 | Validate CLI options | Strict parser | Invalid mode/stage/log/report options return non-zero without starting the runner | 8.4.1 | Done |
| 8.4.3 | Implement exit-code mapping | Exit-code function | `SUCCESS` and policy-approved `SUCCESS_WITH_REJECTIONS` return `0`; `PARTIAL_SUCCESS`, configuration, gate, stage, validation, publish, and delivery failures return non-zero | 8.3.4 | Done |
| 8.4.4 | Generate JSON summary | Versioned JSON schema | Summary contains run identity, stage statuses, Gold identity/version/pointer fields, counts, durations, failures, and report metadata | 8.3.5 | Done |
| 8.4.5 | Generate Markdown summary | Human-readable report | Report identifies failed stage, reason, counts, and publication state without secrets | 8.4.4 | Done |
| 8.4.6 | Test subprocess behavior | CLI smoke tests | Success/failure/report-failure fake runs produce expected report and exit code | 8.4.3-8.4.5 | Done |

### 6.5 Integration markers and CI

| ID | Task | Output | Acceptance criteria | Dependency | Status |
|---|---|---|---|---|---|
| 8.5.1 | Classify database tests | `@pytest.mark.integration` usage | Tests requiring PostgreSQL/SQL Server are marked; pure unit tests are not | W0-W7 | Done |
| 8.5.2 | Register marker/configuration | `pytest.ini` or project config | Unknown-marker warnings are absent and default unit command is documented | 8.5.1 | Done |
| 8.5.3 | Add unit test command | Test command/documentation | `python -m pytest -m "not integration" -q` runs without external services | 8.5.2 | Done |
| 8.5.4 | Define CI quality jobs | CI workflow | CI runs unit tests, lint, format, and type check on supported Python version | 8.5.3 | Done |
| 8.5.5 | Define integration job | Optional/service-backed CI job | Integration job provisions/checks prerequisites and reports unavailable services clearly; unavailable evidence is blocked/non-success for Production DoD | 8.5.4 | Done |
| 8.5.6 | Add CI artifact retention | Test/report artifacts | Failed CI preserves test output and pipeline summary for diagnosis without secrets | 8.5.4 | Done |

### 6.6 README, runbook, checklist, and evidence

| ID | Task | Output | Acceptance criteria | Dependency | Status |
|---|---|---|---|---|---|
| 8.6.1 | Document one-command execution | README/runbook command | Command, environment setup, modes, prerequisites, and expected exit codes are accurate | 8.4 | Done |
| 8.6.2 | Document recovery paths | Runbook | Health failure, bootstrap mismatch, failed staging, rerun, and report lookup steps are actionable | 8.3/8.4 | Done |
| 8.6.3 | Update master checklist | Phase 4 checklist | Phase 4E tasks and statuses link to implementation/evidence files | 8.4-8.6 | Done |
| 8.6.4 | Record command/help evidence | Evidence log | CLI help, unit test, CI, and representative failure/success outputs are recorded | 8.4/8.5 | Done |
| 8.6.5 | Review docs against runtime | Documentation review | No documented command or status differs from actual behavior | 8.6.1-8.6.4 | Done |

## 7. Verifiable acceptance criteria

### 7.1 Health and bootstrap

- [x] Settings are loaded and validated before health checks or stage execution.
- [x] Pre-bootstrap checks cover Settings and configured SQL Server/PostgreSQL connectivity.
- [x] Post-bootstrap readiness checks cover required schemas, tables, metadata, and compatible versions.
- [x] Connectivity failure prevents bootstrap and all downstream calls; post-bootstrap readiness failure prevents Bronze, Silver, and Gold calls.
- [x] Bootstrap creates/checks required schemas and metadata idempotently.
- [x] Bootstrap records and validates schema version; incompatible versions fail clearly and non-destructively.
- [x] Bootstrap rerun does not drop or replace published data.

### 7.2 Runner and pipeline gates

- [x] `PipelineRunner` executes the documented order: Settings, connectivity, bootstrap, post-bootstrap readiness, Bronze, Silver, Silver snapshot gate, and `GoldJob`.
- [x] Runner accepts injected stage dependencies and can be tested without a live database.
- [x] Stage selection validates prerequisites: Silver requires explicit Bronze snapshot/recovery input; Gold requires explicit valid Silver `source_snapshot_id`; unsupported combinations fail before mutation.
- [x] Silver transformation/validation failure prevents Gold execution.
- [x] Gold failure or KPI mismatch produces `FAILED` with `failed_stage="gold"`, preserves the last valid Gold publication, and does not roll back Bronze or Silver.
- [x] `GoldJob` owns the Gold workflow and delegates validation, KPI, constraint, and current-pointer mechanics to injected Gold services; the runner does not duplicate them.
- [x] Runner preserves Gold `pipeline_snapshot_id`, `source_snapshot_id`, `gold_run_id`, `gold_load_id`, `gold_version`, `current_pointer`, KPI, constraint, and publication fields.
- [x] Stage-level errors retain stage, table, dependency, and root-cause context.
- [x] Pipeline result aggregates statuses, counts, timings, publication state, and errors without exposing secrets.

### 7.3 CLI, exit code, and reports

- [x] CLI help documents mode, stage selection, log level, report output, prerequisites, and exit behavior.
- [x] Invalid CLI options fail before the runner starts and return non-zero.
- [x] Exit code `0` is returned for `SUCCESS` and policy-approved `SUCCESS_WITH_REJECTIONS` only.
- [x] `PARTIAL_SUCCESS` returns non-zero for a full pipeline and is not downstream-eligible.
- [x] Health, bootstrap, stage, validation, and publication failures return non-zero.
- [x] JSON summary is machine-readable and includes `run_id`, final status, stage results, Gold identity/version/pointer fields, counts, durations, failure context, and report path.
- [x] Markdown summary is readable and identifies failed stage, reason, counts, and publication state.
- [x] JSON and Markdown are rendered from the same structured result and contain no password, credential-bearing connection string, or full raw payload.
- [x] Report generation failure is a non-success delivery outcome and returns non-zero.

### 7.4 Test separation and CI

- [x] Database-dependent tests carry the `integration` marker.
- [x] `python -m pytest -m "not integration" -q` is deterministic without PostgreSQL/SQL Server.
- [x] CI runs unit tests, lint, format, and type-check commands agreed by the repository.
- [x] Integration CI explicitly provisions or checks PostgreSQL/SQL Server prerequisites.
- [x] Environment-unavailable integration failures are visible and not reported as application success; local unit selection may skip them only with an explicit prerequisite reason.
- [x] CI retains actionable test/report artifacts while excluding secrets.

### 7.5 Documentation and operations

- [x] README contains the supported one-command pipeline invocation and environment setup.
- [x] Runbook documents health failure, bootstrap/version mismatch, rerun, staging cleanup, rollback/preservation, and report lookup.
- [x] Phase 4 master checklist links Phase 4E implementation and evidence and does not mark unverified work as done.
- [x] Documentation matches actual CLI help, exit codes, report fields, and stage order.

## 8. Test matrix and commands

| Scenario | Test type | Expected result |
|---|---|---|
| Valid full run | Unit/fake stages | All stages execute in order, summary is written, exit code is `0` |
| Settings failure | Unit/CLI | No health or data job call; non-zero exit |
| Degraded health | Unit | Bootstrap and all downstream stages are not called |
| Connectivity failure before bootstrap | Unit | Bootstrap and all data stages are not called |
| Missing schema/table | Unit/integration | Readiness fails before mutation |
| Bootstrap first run | Integration | Required objects/version are created safely |
| Bootstrap rerun | Unit/integration | No destructive change; compatible success |
| Bootstrap incompatible version | Unit/integration | Clear failure; published data unchanged |
| Bronze failure | Unit | Silver and Gold are not called when Bronze is required |
| Silver transformation failure | Unit | Silver validation and Gold are not called |
| Silver validation failure | Unit | Gold is not called; pipeline is failed |
| Gold/KPI failure | Unit/integration | Non-zero result; previous Gold remains available |
| Full-run partial Bronze result | Unit | Silver/Gold are not called; result is `PARTIAL_SUCCESS` or `FAILED`, exit is non-zero |
| Standalone Silver without snapshot | Unit/CLI | Selection fails before mutation |
| Standalone Gold without explicit source snapshot | Unit/CLI | Selection fails before mutation |
| Invalid CLI mode/stage | CLI | Parser fails before runner execution |
| Successful CLI run | CLI/subprocess | JSON/Markdown report generated and exit code `0` |
| Failed CLI run | CLI/subprocess | Failure report generated and non-zero exit |
| Report generation failure | Unit/CLI | Delivery result is non-success and exit is non-zero |
| Unit test selection | CI/local | `pytest -m "not integration"` needs no external service |
| Integration prerequisite unavailable | CI/integration | Failure is classified and visible, never silently passed |
| Secret redaction | Unit | Reports/logs contain no credentials or raw payloads |

Suggested commands from repository root:

```powershell
python -m pytest -m "not integration" -q
python -m pytest tests/test_architecture_contract.py tests/test_phase4a_w0_contract.py -q
python -m pytest tests/test_pipeline_runner.py tests/test_pipeline_cli.py tests/test_bootstrap.py tests/test_health_gate.py -q
python -m pytest -m integration -q
python -m pytest -q
```

The focused command names are targets for the implementation. Update them in this document when the final module/test names are approved. Do not claim a command passed until its output is recorded in the evidence log.

## 9. Definition of Done for Phase 4E

Phase 4E is complete only when all applicable items below have implementation and evidence:

- [x] Settings/connectivity is a hard gate before bootstrap, and post-bootstrap schema/metadata/version readiness is a hard gate before data processing.
- [x] Bootstrap is real, idempotent, versioned, and non-destructive to published data.
- [x] `PipelineRunner` coordinates all required stages with explicit failure policy and does not duplicate GoldJob mechanics.
- [x] Silver snapshot validation is a pipeline gate; Gold validation/KPI/publication remains inside `GoldJob` and its result is a pipeline gate.
- [x] CLI supports documented modes/stage selection and rejects invalid options.
- [x] Exit code `0` is limited to `SUCCESS` and policy-approved `SUCCESS_WITH_REJECTIONS`; full-run `PARTIAL_SUCCESS` and `FAILED` are non-zero.
- [x] JSON and Markdown summary reports are generated from one structured result, preserve Gold identity/version/pointer fields, and are redacted; delivery failure is non-success.
- [x] Integration tests are marked and unit tests run without external services; unavailable integration prerequisites are explicit and block Production DoD.
- [x] CI runs the agreed unit, lint, format, type-check, and integration-aware checks. Quality checks execute and upload logs; the current baseline is informational/non-blocking because Black/Flake8/MyPy still report pre-existing debt.
- [x] README/runbook/checklist reflect actual commands, prerequisites, recovery, and evidence.

Production DoD must not be inferred from a fake-stage success test alone. Health gating, bootstrap idempotency/version checks, failure sequencing, exit codes, report redaction, integration classification, and documentation/runtime agreement all require evidence.

## 10. Risks and mitigations

| Risk | Impact | Mitigation |
|---|---|---|
| Health result is advisory only | Unsafe partial processing | Pre-bootstrap hard-gate test asserts zero bootstrap/data calls; post-bootstrap readiness test asserts zero data calls |
| Bootstrap reports success without preparing schema | Late runtime failures | Idempotent DDL/version integration checks |
| Runner duplicates stage logic | Divergent behavior from jobs | Keep mechanics in stage services and use dependency injection |
| Stage selection bypasses dependencies | Inconsistent Silver/Gold snapshot | Require explicit Bronze recovery input for Silver and explicit `source_snapshot_id` for Gold |
| Partial result maps to exit `0` | Scheduler records false success | `PARTIAL_SUCCESS` is non-zero and not downstream-eligible; verify with subprocess tests |
| Reports expose secrets | Credential leakage in artifacts | Safe serializers and redaction tests |
| Report generation fails after stage execution | Delivery evidence is missing but process reports success | Treat report failure as non-success and test the delivery boundary |
| Integration tests run by default | Local/CI instability | Register marker and document unit-first command |
| CI lacks database services | False confidence or opaque failures | Separate service-backed job with prerequisite diagnostics |
| Docs drift from runtime | Operators run unsafe commands | Compare README/runbook with CLI help in evidence review |

## 11. Evidence log

Update this table after each task. A task may be marked `Done` only after implementation, focused tests, and relevant regression/CI evidence are recorded.

| Date | Task | Files/symbols | Validation command | Result | Status |
|---|---|---|---|---|---|
| 2026-09-04 | Create Phase 4E orchestration and delivery scope | `docs/ToDoCheckList/Phase_4_Review&Enhance_Code/phase4_Orchestration&Delivery_execution.md` | Markdown review | Document created; implementation not started | Done |
| 2026-09-04 | Baseline current orchestration/delivery behavior | `main.py`, `src/app/app.py`, `src/jobs/platform_bootstrap.py`, `connection_health_service.py`, `pytest.ini`, `README.md` | Source/configuration inspection | Historical baseline: bootstrap was placeholder; delivery contract, integration marker, and CI were absent. Current runner now gates on health and invokes Bronze/Silver/Gold, but bootstrap/readiness and delivery gaps remain. | Superseded |
| 2026-09-07 | Reconcile Phase 4E with current Bronze-to-Gold runtime | `src/app/pipeline_runner.py`, `src/app/app.py`, `src/features/Sales_Performance/jobs/sales_silver_job.py`, `src/features/Sales_Performance/jobs/sales_gold_job.py`, PostgreSQL pointers | `python -m pytest tests/test_bronze_publish.py tests/test_silver_job.py tests/test_silver_pipeline_gate.py tests/test_postgres_gold_constraints_integration.py tests/test_postgres_gold_candidate_integration.py tests/test_postgres_gold_publish_integration.py tests/test_postgres_gold_reconciliation_integration.py tests/test_gold_job.py tests/test_gold_fact_staging.py tests/test_gold_validation.py tests/test_gold_kpi.py -q` | `61 passed`; runtime verified full Bronze -> Silver -> Gold with Silver pointer `9f3148f1-9313-4b87-ba99-8e60727b08d7`, Gold `v008`, `PUBLISHED`, validation/constraints/KPI pass, and zero active PostgreSQL sessions. | Done |
| 2026-09-07 | Identify remaining Phase 4E delivery gaps | `src/jobs/platform_bootstrap.py`, `main.py`, `pytest.ini`, repository root | Source/configuration inspection | Bootstrap remains hard-coded; no post-bootstrap readiness service, CLI parser, process exit-code mapping, JSON/Markdown reporter, integration marker, or CI workflow exists. | In progress |
| 2026-09-07 | Implement 6.1 health/readiness gate | `src/shared/services/connection_health_service.py`, `src/jobs/platform_bootstrap.py`, `src/app/pipeline_runner.py`, `tests/test_health_gate.py`, `tests/test_bootstrap.py` | `python -m pytest tests/test_pipeline_runner.py tests/test_bootstrap.py tests/test_health_gate.py -q`; `python -m pytest -q` | `10 passed` focused; `190 passed` full suite. Settings/connectivity failures stop before bootstrap; missing schema/metadata/version readiness stops data stages; diagnostics exclude exception details. | Done |
| 2026-09-07 | Implement 6.2 real bootstrap and schema versioning | `src/shared/ingestion/postgres_ingestion_schema.py`, `src/jobs/platform_bootstrap.py`, `tests/test_bootstrap.py` | `python -m pytest tests/test_bootstrap.py tests/test_health_gate.py tests/test_pipeline_runner.py -q`; `python -m pytest -q` | `12 passed` focused; full regression recorded after implementation. Bootstrap inventory/version contract is explicit; rerun is non-destructive; incompatible version and interrupted setup are reported clearly. | Done |
| 2026-09-07 | Implement 6.3 pipeline runner and stage orchestration | `src/app/pipeline_runner.py`, `tests/test_pipeline_runner.py` | `python -m pytest tests/test_pipeline_runner.py tests/test_bootstrap.py tests/test_health_gate.py -q`; `python -m pytest -q` | `15 passed` focused; `195 passed` full suite. Stage order, injected adapters, explicit selection, partial-failure blocking, Gold failure policy, stage context, and aggregate counts verified without live database. | Done |
| 2026-09-07 | Implement 6.4 CLI, exit codes, and summary reports | `src/app/cli.py`, `src/app/reporter.py`, `main.py`, `tests/test_pipeline_cli.py` | `python -m pytest tests/test_pipeline_cli.py tests/test_pipeline_runner.py -q`; `python -m pytest -q` | `16 passed` focused at implementation time; current full suite is `204 passed`. CLI options, strict parsing, four-status exit mapping, JSON/Markdown rendering, report metadata, report-delivery failure, and process boundary verified. | Done |
| 2026-09-07 | Implement 6.5 integration markers and CI | `pytest.ini`, marked integration tests, `.github/workflows/ci.yml` | `python -m pytest -m "not integration" -q`; `python -m pytest -m integration -q` | `182 passed, 22 deselected`; integration `22 passed`. CI runs unit tests, quality checks, prerequisite-aware integration tests, and uploads diagnostics/results as artifacts. | Done |
| 2026-09-07 | Close Gold result/report contract gaps | `src/shared/ingestion/postgres_gold_publish_service.py`, `src/features/Sales_Performance/jobs/sales_gold_job.py`, `src/app/pipeline_runner.py`, `tests/test_pipeline_runner.py` | `python -m pytest tests/test_pipeline_runner.py tests/test_pipeline_cli.py tests/test_gold_publish.py -q`; `python -m pytest -q` | `24 passed` focused; `204 passed` full suite. Gold `current_pointer` and lineage fields propagate to runner/report results; generic failures expose `error_message` with legacy `error` compatibility alias; top-level row counts are exposed. | Done |
| 2026-09-07 | Verify Phase 4E Definition of Done | Unit, focused, integration, full, quality and compile commands | Unit `182 passed, 22 deselected`; focused `25 passed`; integration `22 passed, 182 deselected`; full `204 passed`; compile/diff pass. Black/Flake8/MyPy execute but report existing quality debt and remain informational in CI. | Done |
| 2026-09-09 | Align `App.run()` compatibility status with CLI exit-code policy | `src/app/app.py`, `tests/test_app_bronze_to_silver.py` | `pytest tests/test_app_bronze_to_silver.py tests/test_pipeline_cli.py tests/test_pipeline_runner.py -q`; `pytest tests/ -q -m "not integration"` | Policy decided: `PARTIAL_SUCCESS` now maps to `degraded` (previously `ok`), matching the CLI's non-zero exit code for `PARTIAL_SUCCESS`; new focused test `test_app_partial_success_is_degraded_not_ok` added; 199 passed (198 + 1 new) | Done |

## 12. Related documents

- `docs/internal/phase4_review_enhance_spec.md`
- `docs/internal/phase4_enhancement_execution_plan_spec.md`
- `docs/ToDoCheckList/Phase_4_Review&Enhance_Code/phase4a_foundation_execution.md`
- `docs/ToDoCheckList/Phase_4_Review&Enhance_Code/phase4b_bronzelayer_execution.md`
- `docs/ToDoCheckList/Phase_4_Review&Enhance_Code/phase4c_silverlayer_execution.md`
- `docs/ToDoCheckList/Phase_4_Review&Enhance_Code/phase4d_goldlayer_execution.md`
- `src/app/app.py`
- `src/jobs/platform_bootstrap.py`
- `src/shared/services/connection_health_service.py`
- `main.py`
- `pytest.ini`
- `README.md`
