# Phase 4A - Foundation Execution Document

## 1. Purpose

This document converts workstreams W0-W3 from `PHASE_4_ENHANCEMENT_EXECUTION_PLAN_VI.md` into an implementation, testing, and review scope. Phase 4A is complete only when code, focused tests, regression tests, and corresponding evidence are available.

Scope:

1. W0 - Complete the baseline and confirm public API compatibility.
2. W1 - Implement centralized configuration with `pydantic-settings`.
3. W2 - Separate domain ownership and standardize `TableSpec`.
4. W3 - Create shared result, staging, audit, retry, and checkpoint models/services.
5. Run foundation unit tests independently of databases.

Out of scope for Phase 4A: full Bronze/Silver/Gold reliability implementation, end-to-end orchestration, and production CI.

## 2. Baseline and principles

### 2.1. Git baseline

| Item | Current value |
|---|---|
| Implementation branch | `Phase4_A_EnhanceFoundation` |
| Baseline commit | `fc699b3` |
| Main before Phase 4A | `7de3bf9` |
| Current runtime | `App.run()` performs health check, placeholder bootstrap, and Bronze execution |
| Test policy | Unit tests must not require external services; integration tests must be marked |

Update this baseline evidence when implementation starts if the branch or commit changes.

### 2.2. Mandatory principles

- Do not add new logic to compatibility wrappers.
- Connectors and business jobs must not read `os.getenv()` directly.
- Never log passwords, tokens, or full raw payloads.
- The shared layer owns reusable mechanics, not domain-specific business rules.
- Retry only transient errors and preserve logical identity.
- Validation failure must block downstream publication.
- Every task needs a focused test before its status can become `Done`.

## 3. Dependency and execution order

```text
W0 baseline/API
    -> W1 Settings
        -> W2 domain ownership/TableSpec
            -> W3 shared models/services
                -> foundation unit tests
```

Do not start W2 until the public API inventory is complete. Do not start W3 until the Settings contract and domain ownership are agreed.

## 4. W0 - Baseline and API compatibility

### Objective

Confirm current behavior, public entrypoints/imports, and constraints that must be protected before refactoring.

### Tasks

| ID | Work | Output | Acceptance criteria | Status |
|---|---|---|---|---|
| 0.1 | Capture branch, commit, and test baseline | Baseline report | Commands, results, and execution environment are recorded | Not started |
| 0.2 | Inventory entrypoints | List of `main.py`, `App`, CLI, and job entrypoints | Main callers and exit behavior are identified | Not started |
| 0.3 | Inventory public imports | Compatibility matrix | Existing imports are checked by tests or evidence | Not started |
| 0.4 | Inventory tables and domains | Ownership matrix | Every table has exactly one owner | Not started |
| 0.5 | Record current behavior | Regression notes | Refactoring is not based on unverified assumptions | Not started |

### Minimum compatibility matrix

| API/entrypoint | Current caller | Contract to preserve | Verification |
|---|---|---|---|
| `src.app.app.App` | `main.py`/tests | Constructor and `run()` | Import test and smoke test |
| Legacy Bronze job import | Existing tests/callers | Class name, constructor, and `run()` | Compatibility test |
| Public connector classes | App/services | Constructor and connection contract | Import/signature test |
| Silver/Gold callable surface | Pipeline/tests | Current signature during migration | Contract test |

### W0 execution results

| Area | Evidence |
|---|---|
| Git baseline | Branch `Phase4_A_EnhanceFoundation`, HEAD `fc699b3`, with two uncommitted Phase 4A documents at verification time |
| Test baseline | `31 passed, 3 failed`; all three failures were caused by PostgreSQL being unavailable at `localhost:5432` |
| Entrypoint | `main.py` calls `App().run()`; `App` coordinates health check, placeholder bootstrap, and Bronze |
| Public API | `App`, `PlatformBootstrapJob`, `ConnectionHealthService`, `SalesBronzeIngestionJob`, `SalesExtractor`, `BronzeLoader`, `BronzeValidator`, `SQLServerConnector`, `PostgreSQLConnector` |
| Compatibility test | `tests/test_phase4a_w0_contract.py`: 3 passed |
| Ownership gap | `SalesBronzeIngestionJob` currently loads five Sales tables plus `Production.Product` and `Person.Person`; separation belongs to W2 |

W0 is complete for baseline, inventory, and compatibility-surface confirmation. Target ownership is not marked complete; the current gap is a required input for W2.
### W0 Definition of Done

- [x] Baseline command and result are recorded.
- [x] Public imports and entrypoints have an identified owner.
- [x] Compatibility tests exist for APIs with real callers.
- [x] Domain/table inventory and ownership gap are recorded before W2 starts.

## 5. W1 - Centralized configuration

### Objective

Create one typed, validated, and injectable `Settings` object. Process environment variables must override `.env` values.

### Implementation scope

| ID | Implementation | Acceptance criteria | Test evidence | Status |
|---|---|---|---|---|
| 1.1 | Add `pydantic-settings` to `requirements.txt` | Dependency imports successfully | Dependency/import test | Not started |
| 1.2 | Create `src/core/settings.py` | Uses `BaseSettings`, `SettingsConfigDict`, `SecretStr`, and cached `get_settings()` | Settings construction test | Done |
| 1.3 | Define connection fields | SQL Server/PostgreSQL host, port, database, driver, and auth mode parse correctly | Field parsing test | Done |
| 1.4 | Define runtime fields | `batch_size > 0`, `bronze_rejected_threshold >= 0`, `silver_rejected_threshold >= 0`, retry attempts in `1..3`, and valid delays | Invalid value test | Done |
| 1.5 | Validate authentication | `windows` does not require credentials; `sql` requires username/password | Authentication matrix test | Done |
| 1.6 | Redact secrets | Password uses `SecretStr`; safe summary excludes secrets | Redaction test | Done |
| 1.7 | Inject Settings | App, connectors, services, and jobs use the same instance | Constructor/usage test | Done |
| 1.8 | Synchronize `.env.example` | Template matches schema and contains no real secrets | Template consistency test | Done |

### Settings contract

Minimum fields:

```text
environment: development | test | staging | production
debug: bool
log_level: str
sql_server_host: str
sql_server_port: int
sql_server_database: str
sql_server_driver: str
sql_server_auth_mode: windows | sql
sql_server_username: str | None
sql_server_password: SecretStr | None
postgres_host: str
postgres_port: int
postgres_database: str
postgres_username: str
postgres_password: SecretStr
batch_size: int > 0
bronze_rejected_threshold: int >= 0
silver_rejected_threshold: int >= 0
retry_max_attempts: int 1..3
retry_initial_delay_seconds: float > 0
retry_max_delay_seconds: float > 0
```

Rules:

- Process environment variables have precedence over `.env`.
- Missing or invalid configuration must fail before health checks.
- Credential defaults are allowed only in approved development/test environments.
- `bronze_rejected_threshold` is a global Bronze policy with default `0`; an explicit non-negative environment value may override it for an approved environment.
- `None` is not a valid configured Bronze threshold and must not mean unlimited rejected rows.
- The effective Bronze threshold must be included in safe runtime summaries, table results, audit records, and publication decisions.
- `retry_max_attempts` is the total number of attempts, including the initial attempt; default `3`, minimum `1`, maximum `3`.
- Values above `3` are invalid and must fail Settings validation before health checks.
- Safe summaries may contain host, port, database, auth mode, batch size, rejection thresholds, and retry limits only.

### W1 Definition of Done

- [x] One centralized Settings boundary exists.
- [x] Canonical connectors no longer read environment variables directly.
- [x] Authentication and type validation have unit tests.
- [x] Passwords do not appear in `repr`, logs, exception reports, or summaries.
- [x] App and dependencies receive Settings through injection.
- [x] Bronze rejection policy is centralized in Settings with default `0`, explicit override, and no implicit unlimited mode.
- [x] Retry policy is centralized with default `3`, hard maximum `3`, and explicit total-attempt semantics.

## 6. W2 - Domain ownership and TableSpec

### Objective

Separate business ownership from infrastructure mechanics and remove hard-coded table mappings from `run()` methods.

### Ownership contract

| Domain | Tables | Owner |
|---|---|---|
| Sales | `SalesOrderHeader`, `SalesOrderDetail`, `Customer`, `SalesTerritory`, `SalesPerson` | `SalesBronzeJob` |
| Production | `Product` | `ProductionBronzeJob` |
| Person | `Person` | `PersonBronzeJob` |

Actual schema and table names must be reconciled with the source inventory before implementation.

### Implementation scope

| ID | Implementation | Acceptance criteria | Test evidence | Status |
|---|---|---|---|---|
| 2.1 | Create immutable `TableSpec` | Includes source/target, primary key, required columns, ordering key, and incremental column | Spec validation test | Done |
| 2.2 | Create Sales specs/job | Sales job handles only the five Sales tables | Ownership test | Done |
| 2.3 | Create Production job | Product has an independent result | Domain result test | Done |
| 2.4 | Create Person job | Person has an independent result | Domain result test | Done |
| 2.5 | Separate shared mechanics | Jobs provide specs/policy; runner owns mechanics | Architecture/import test | Done |
| 2.6 | Preserve compatibility wrapper | Wrapper delegates without new business logic | Legacy regression test | Done |
| 2.7 | Confirm domain isolation | One domain failure does not remove another domain's result | Failure isolation test | Done |

### TableSpec contract

`TableSpec` should be an immutable dataclass or equivalent model with at least:

```text
source_schema
source_table
target_schema
target_table
primary_key
required_columns
ordering_key
incremental_column
```

Required validation:

- Identifiers are non-empty and validated before SQL use.
- The primary key belongs to, or has an explicit relationship with, required columns.
- The ordering key is stable for batch/incremental reads.
- `TableSpec` does not contain retry policy, staging state, or database connections.

### W2 Definition of Done

- [x] Product and Person are not in the canonical Sales job.
- [x] Table mappings are defined in specs rather than scattered through `run()`.
- [x] The shared runner contains no domain-specific business rules.
- [x] Compatibility wrappers only delegate.
- [x] Domain ownership and isolation have tests.

### W2 execution results

| Area | Evidence | Status |
|---|---|---|
| `TableSpec` | `src/shared/ingestion/ingestion_models.py`, immutable metadata with identifier validation and qualified names | Done |
| Sales ownership | `SalesBronzeJob` declares only five Sales specs | Done |
| Production ownership | `ProductionBronzeJob` declares `Production.Product` separately | Done |
| Person ownership | `PersonBronzeJob` declares `Person.Person` separately | Done |
| Shared mechanics | `DomainBronzeJob` provides shared extract/load/validate mechanics | Done |
| Compatibility | `SalesBronzeIngestionJob` delegates to `SalesBronzeJob` and preserves its signature | Done |
| Focused tests | W0/W2 ownership/spec and Bronze regression -> `11 passed` | Done |
| Full regression | `python -m pytest -q` -> `48 passed` | Done |

## 7. W3 - Shared foundation models and services

### Objective

Standardize contracts shared by Bronze, Silver, and Gold before implementing stage-specific reliability.

### Proposed package shape

```text
src/shared/ingestion/
├── ingestion_models.py
├── batch_ingestion_engine.py
├── retry_policy.py
├── checkpoint_manager.py
├── staging_manager.py
└── audit_service.py
```

Module names may follow the existing structure, but ownership of these contracts must remain clear.

### Result contract

Every stage/table/batch must expose at least:

```python
{
    "run_id": "...",
    "stage": "bronze",
    "source_table": "Sales.SalesOrderDetail",
    "target_table": "bronze.sales_order_detail",
    "status": "SUCCESS",
    "rows_read": 0,
    "rows_written": 0,
    "rows_rejected": 0,
    "attempt_count": 1,
    "started_at": "...",
    "finished_at": "...",
    "error_type": None,
    "error_message": None,
}
```

Minimum status vocabulary: `SUCCESS`, `SUCCESS_WITH_REJECTIONS`, `PARTIAL_SUCCESS`, `FAILED`, `RETRYING`, and `QUARANTINED`.

### Implementation scope

| ID | Implementation | Acceptance criteria | Test evidence | Status |
|---|---|---|---|---|
| 3.1 | Create status/result models | Bronze/Silver/Gold use the same vocabulary and fields | Model serialization test | Done |
| 3.2 | Create run/load/batch identity | Retries preserve logical IDs | Identity test | Done |
| 3.3 | Create audit model/service contract | Run/table/batch, counts, attempts, timestamps, and errors are represented | Audit contract test | Done |
| 3.4 | Create `StagingManager` contract | Identifiers are validated; published tables are untouched before publish | Staging safety test | Done |
| 3.5 | Create error classifier | Transient and deterministic errors are distinguished | Classifier matrix test | Done |
| 3.6 | Create retry policy/executor contract | Maximum three total attempts, backoff/jitter, injectable clock/sleeper | Retry behavior test | Done |
| 3.7 | Create checkpoint contract | Checkpoint advances only after successful commit | Checkpoint ordering test | Done |
| 3.8 | Create fake fixtures | Foundation tests run without a live database | Fixture smoke test | Done |

### Invariants to protect

- `run_id`, `load_id`, and `batch_id` are logical identities; `attempt_count` does not create a new identity.
- A checkpoint must never be written before the data commit.
- Only transient errors are retryable.
- Retry exhaustion returns `FAILED`, does not hide the error, and does not publish.
- Staging identifiers are validated instead of concatenating arbitrary input into SQL.
- Audit records preserve the final outcome and attempt history.
- Shared models do not know Sales, Production, or Person business rules.

### W3 Definition of Done

- [x] The result contract is used consistently across foundation components.
- [x] Retry, staging, audit, and checkpoint boundaries are explicit.
- [x] Identity, retry, and commit ordering have independent tests.
- [x] Error classification has transient/deterministic test coverage.
- [x] Foundation unit tests run without a live database.

### W3 execution results

| Area | Evidence | Status |
|---|---|---|
| Models/contracts | `ingestion_models.py`: status, execution identity, result, and audit records | Done |
| Retry | `retry_policy.py`: classifier, exponential backoff+jitter, max attempts, and injectable sleeper | Done |
| Staging | `staging_manager.py`: identifier validation, validation gate, publish preservation, and cleanup | Done |
| Checkpoint | `checkpoint_manager.py`: cannot advance before commit | Done |
| Audit | `audit_service.py`: in-memory run/table/batch history with no secrets | Done |
| Focused tests | W3 foundation tests -> `10 passed` | Done |
| Full regression | `python -m pytest -q` -> `58 passed` | Done |

## 8. Foundation unit tests

### Proposed test layout

```text
tests/
├── test_settings.py
├── test_domain_ownership.py
├── test_table_spec.py
├── test_ingestion_models.py
├── test_retry_policy.py
├── test_staging_manager.py
├── test_checkpoint_manager.py
└── test_audit_service.py
```

Files may be combined when consistent with the existing structure, but every contract must have an easy-to-locate test.

### Required scenarios

| Area | Scenarios |
|---|---|
| Settings | `.env`, environment override, invalid type, Windows/SQL auth, secret redaction |
| Compatibility | Legacy import, constructor/signature, wrapper delegation |
| Ownership | Correct table owner; Sales excludes Product/Person |
| TableSpec | Required fields, invalid identifiers, stable ordering key, immutability |
| Result | Status/count serialization and success/failure/error fields |
| Retry | Transient retry, deterministic no-retry, max attempts, injected backoff |
| Identity | Retry preserves run/load/batch IDs |
| Staging | Safe identifier, cleanup, publish guard |
| Checkpoint | No advance before commit; advance after commit |
| Audit | Attempt and final outcome records; no secrets |

### Commands and evidence

Run from the repository root:

```powershell
python -m pytest -m "not integration" -q
python -m pytest tests/test_settings.py tests/test_table_spec.py tests/test_ingestion_models.py -q
```

If a test file does not yet exist, create the test before changing task status. Evidence must record the command, pass/fail count, and the reason for any environment-blocked test in the PR, commit notes, or evidence document.

## 9. Transition gates

| Gate | Condition |
|---|---|
| G0 -> W1 | Baseline and compatibility inventory are complete |
| W1 -> W2 | Settings tests pass and the injection boundary is confirmed |
| W2 -> W3 | Ownership/TableSpec tests pass and wrapper regression passes |
| W3 -> Test completion | Model/service contracts pass and fake fixtures run without a database |
| Phase 4A complete | Focused tests and `pytest -m "not integration"` pass; evidence is updated |

## 10. Evidence log

Update this table after each completed task. Do not set a task to `Done` without recording the file/symbol, command, and verification result.

| Date | Task | Files/symbols | Validation command | Result | Status |
|---|---|---|---|---|---|
| 2026-09-04 | Create English execution document | `docs/ToDoCheckList/Phase_4_Review&Enhance_Code/phase4a_foundation_execution.md` | Markdown review | Created | Done |
| 2026-09-04 | Execute W0 baseline/API compatibility | `tests/test_phase4a_w0_contract.py`, `main.py`, `src/app/app.py`, Sales Bronze job | `python -m pytest tests/test_phase4a_w0_contract.py -q` | 3 passed; baseline suite 31 passed, 3 blocked by unavailable PostgreSQL | Done |
| 2026-09-04 | Execute W1 centralized configuration | `src/core/settings.py`, connectors, App, `.env.example`, `tests/test_settings.py` | `python -m pytest tests/test_settings.py -q` | 8 passed including Bronze threshold default, override, validation, and safe-summary evidence | Done |
| 2026-09-04 | Execute W2 domain ownership and TableSpec | Domain jobs, shared runner, `tests/test_phase4a_w2_domain_ownership.py` | `python -m pytest tests/test_phase4a_w2_domain_ownership.py -q` | 4 passed | Done |
| 2026-09-04 | Execute W3 foundation models/services | Shared ingestion models/services and W3 tests | `python -m pytest tests/test_ingestion_models.py tests/test_retry_policy.py tests/test_staging_manager.py tests/test_checkpoint_manager.py tests/test_audit_service.py -q` | 10 passed | Done |
| 2026-09-04 | Run Phase 4A foundation suite | W0-W3 foundation tests | `python -m pytest tests/test_settings.py tests/test_phase4a_w0_contract.py tests/test_phase4a_w2_domain_ownership.py tests/test_ingestion_models.py tests/test_retry_policy.py tests/test_staging_manager.py tests/test_checkpoint_manager.py tests/test_audit_service.py -q` | 24 passed | Done |
| 2026-09-04 | Run full regression suite | All repository tests | `python -m pytest -q` | 58 passed | Done |

## 11. Phase 4A Definition of Done

- [x] W0 baseline and public API compatibility have evidence.
- [x] W1 centralized `Settings` works and is injected.
- [x] W2 domain ownership and immutable `TableSpec` are tested.
- [x] W3 result/staging/audit/retry/checkpoint contracts are tested.
- [x] Retry does not create a new logical identity and checkpoints do not advance before commit.
- [x] Secrets do not appear in logs or evidence.
- [x] Foundation unit tests run independently of a database.
- [x] The appropriate regression suite passes.
- [x] The checklist and README reflect actual runtime behavior.

## 12. Related documents

## W1 Execution Evidence

- Standard environment: repository-local `.venv` using Python 3.11 64-bit at `A:\Workspace\DataEngineer\AdventureWorks Analytics Platform\.venv\Scripts\python.exe`. All project commands must use this interpreter; do not use the old shared `python\.venv` environment.
- Installation record: `python -m pip install -r requirements.txt` completed successfully in the standard environment.
- Environment check: runtime imports for `pandas`, `psycopg2`, `pyodbc`, `sqlalchemy`, and `pydantic_settings` are available.
- Dependency: added `pydantic-settings==2.4.0` to `requirements.txt` and installed it in the Pylance-selected venv.
- Implementation: added `src/core/settings.py` with typed settings, `.env` loading, environment override, auth/runtime validation, `SecretStr`, and `safe_summary()`.
- Injection: passed Settings through App, health service, Bronze job, extractor, loader, and database connectors.
- Focused validation: `tests/test_settings.py` and `tests/test_phase4a_w0_contract.py` -> `10 passed`; `tests/test_phase0_connectivity.py` -> `5 passed`.
- Connector injection is now validated in the standard environment.
- Full regression: `python -m pytest -q` -> `44 passed`.

- `docs/project/PHASE_4A_FOUNDATION_EXECUTION_VI.md`
- `docs/internal/PHASE_4_ENHANCEMENT_EXECUTION_PLAN_VI.md`
- `docs/internal/PHASE_4_REVIEW_ENHANCE_CODE_VI.md`
- `docs/project/WORKING_STANDARDS.md`
- `tests/test_architecture_contract.py`
- `pytest.ini`
