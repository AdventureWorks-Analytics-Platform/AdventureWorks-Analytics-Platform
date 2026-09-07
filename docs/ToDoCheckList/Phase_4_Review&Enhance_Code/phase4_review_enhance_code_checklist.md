# Phase 4 - Code Review and Enhancement Checklist

## Phase context

| Item | Value |
|---|---|
| Current branch | `Enhance_Project` |
| Current HEAD | `f6a681a` - Fix Phase 3 runtime and document current workflow |
| Branch alignment | `Enhance_Project`, `main`, and `origin/main` point to the same commit |
| Worktree status | Clean at review time |
| Current test baseline | Unit selection `182 passed, 22 deselected`; focused Phase 4E run `24 passed`; full regression `204 passed` |
| Test failure cause | Historical baseline failures were caused by PostgreSQL unavailable at `localhost:5432`; current runtime PostgreSQL and SQL Server evidence exists |
| Current architecture | `App -> PipelineRunner -> BronzeToSilverPipeline -> SalesGoldJob`; Silver uses `silver_v<SNAPSHOT>` plus `silver_current_pointer`; Gold uses versioned candidate schema plus current pointer |
| Phase objective | Make the Sales pipeline reliable, observable, testable, and runnable as one controlled workflow |
| Owner | AI / User |
| Review date | 2026-09-03 |

## Status legend

- `Not started`: Work has not begun.
- `In progress`: Work is actively being implemented.
- `Done`: Implementation and acceptance evidence are complete.
- `Blocked`: Cannot proceed because a dependency or environment is unavailable.
- `Needs clarification`: Scope or acceptance criteria require confirmation.

## Current code assessment

| Area | Current state | Impact current | Enhancement target |
|---|---|---|---|
| Application orchestration | `PipelineRunner` executes Settings/health, bootstrap/readiness, Bronze/Silver, Silver gate through Gold, and returns a structured dictionary; CLI provides exit/report delivery | Full runtime path and delivery boundary are implemented | Preserve contract while adding future scheduler integrations |
| Bootstrap | `PlatformBootstrapJob.run()` owns idempotent metadata bootstrap and schema-version readiness | Required inventory/version must remain aligned with migrations | Keep bootstrap non-destructive and version-compatible |
| Bronze ingestion | Class-based job loads seven source tables and validates row-count parity | Limited error isolation, retry, audit, and incremental-load protection | Per-table execution report, retry policy, audit records, and idempotency |
| Silver transformation | Injectable job writes six targets to a versioned candidate and atomically updates `silver_current_pointer` | Standalone stage selection/readiness contract is not yet implemented | Keep pointer/gate ownership in Silver and expose explicit runner inputs |
| Gold loading | Injectable `SalesGoldJob` builds fact batches, validates, constrains, audits, and atomically publishes a versioned candidate | Delivery/CLI boundary is not yet implemented | Keep Gold mechanics in the job; runner evaluates final result |
| Validation | Silver and Gold validations are separate scripts | Validation is not enforced as a pipeline gate | Validation stages that can stop publication and return evidence |
| Configuration | Connectors read environment variables independently | Configuration is duplicated and difficult to validate consistently | Central typed settings object and environment validation |
| Testing | Unit coverage exists; database tests run by default | Local test runs fail without external services and integration scope is unclear | Separate unit/integration markers, fixtures, and CI execution policy |
| Observability | Logging and result dictionaries exist but are limited | Difficult to diagnose table-level failures and run history | Structured logs, run ID, metrics, audit, and actionable errors |
| Documentation | Current workflow is documented, but Phase 4 work is not tracked | Enhancement work has no controlled acceptance checklist | This tracker plus implementation and validation evidence |

## Technical review findings for discussion

### Configuration and environment variables

| Finding | Current behavior | Risk | Proposed direction |
|---|---|---|---|
| `.env` loading is inconsistent | `.env.example` exists, but the main runtime and shared connectors do not centrally call `load_dotenv()`; only some scripts/tests do | Runtime behavior can differ between `main.py`, scripts, tests, and shell sessions | Load configuration once at application startup through a central typed settings object |
| Credentials have fallback defaults | PostgreSQL connector defaults to `postgres/postgres`; Docker Compose also provides local defaults | Missing configuration can be hidden, and unsafe defaults could reach non-development environments | Allow defaults only in development; require explicit credentials elsewhere; never log passwords |
| Configuration is duplicated | Connectors independently read environment variables | Host, port, database, authentication mode, batch size, and retry settings can diverge | Pass one validated settings object into connectors, services, and jobs |
| Startup validation is incomplete | Missing host, driver, or required credential values are not reported as one clear configuration error | Failures appear later as connection or query errors | Validate required settings before health checks and pipeline execution |

Recommended configuration flow:

```text
.env / process environment
  -> Settings loader
	  -> validation
		  -> App / PipelineRunner
			  -> connectors, jobs, and validators
```

#### Decided implementation: `pydantic-settings`

The project will use `pydantic-settings` as the single configuration mechanism for the Python application. Add the dependency to `requirements.txt`:

```text
pydantic-settings
```

Create `src/core/settings.py` with one cached settings object:

```python
from functools import lru_cache

from pydantic import Field, SecretStr, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
	model_config = SettingsConfigDict(
		env_file=".env",
		env_file_encoding="utf-8",
		case_sensitive=False,
		extra="ignore",
	)

	environment: str = "development"
	debug: bool = False
	log_level: str = "INFO"

	sql_server_host: str
	sql_server_port: int = 1433
	sql_server_database: str = "AdventureWorks2012"
	sql_server_driver: str = "ODBC Driver 17 for SQL Server"
	sql_server_auth_mode: str = "windows"
	sql_server_username: str | None = None
	sql_server_password: SecretStr | None = None

	postgres_host: str = "localhost"
	postgres_port: int = 5432
	postgres_database: str = "adventureworks_warehouse"
	postgres_username: str
	postgres_password: SecretStr

	batch_size: int = Field(default=10000, gt=0)
	retry_max_attempts: int = Field(default=3, ge=1, le=10)
	retry_initial_delay_seconds: float = Field(default=1.0, gt=0)
	retry_max_delay_seconds: float = Field(default=30.0, gt=0)

	@model_validator(mode="after")
	def validate_authentication(self):
		if self.sql_server_auth_mode == "sql":
			if not self.sql_server_username or not self.sql_server_password:
				raise ValueError(
					"SQL Server username and password are required "
					"when sql_server_auth_mode=sql"
				)

		if self.environment != "development":
			if self.postgres_password.get_secret_value() == "postgres":
				raise ValueError(
					"Default PostgreSQL password is not allowed outside development"
				)

		return self


@lru_cache
def get_settings() -> Settings:
	return Settings()
```

Configuration rules:

| Rule | Decision |
|---|---|
| Source priority | Process environment variables override `.env`; `.env` overrides only safe application defaults |
| `.env` loading | `SettingsConfigDict(env_file=".env")` loads the local file when `Settings` is created; no connector loads `.env` independently |
| Required values | `SQL_SERVER_HOST`, `POSTGRES_USERNAME`, and `POSTGRES_PASSWORD` are required unless an explicitly documented local-only policy says otherwise |
| SQL Server Windows Authentication | `SQL_SERVER_AUTH_MODE=windows`; username/password may be empty and are not used |
| SQL Server SQL Authentication | `SQL_SERVER_AUTH_MODE=sql`; username/password are mandatory |
| Secret handling | Password fields use `SecretStr`; passwords are never logged or included in reports |
| Default credentials | `postgres/postgres` is allowed only for local development; staging/production must provide explicit credentials |
| Settings lifetime | `get_settings()` is cached and the same `Settings` instance is injected into App, services, connectors, and jobs |
| Unknown variables | `extra="ignore"` prevents unrelated environment variables from breaking startup; required application values remain validated |

The application boundary should create settings before health checks:

```text
main.py
  -> settings = get_settings()
	  -> App(settings=settings)
		  -> health service / connectors / jobs
```

Connectors should receive the settings object rather than call `os.getenv()`:

```python
class PostgreSQLConnector(BaseConnector):
	def __init__(self, settings: Settings):
		super().__init__()
		self.settings = settings

	def connect(self) -> bool:
		password = self.settings.postgres_password.get_secret_value()
		# Use settings.postgres_host, postgres_port, database, and username here.
```

For backward-compatible construction during migration, constructors may temporarily use `settings: Settings | None = None` and resolve `get_settings()` internally. New application code should always inject the object explicitly. The legacy independent `os.getenv()` reads should be removed after migration.

The `.env.example` file must document the selected names, including `SQL_SERVER_AUTH_MODE`, `BATCH_SIZE`, and retry settings. The real `.env` remains local-only and is protected by `.gitignore`.

Required configuration tests:

| Test | Expected result |
|---|---|
| Valid `.env` | `Settings` loads successfully with correct typed values |
| Process variable overrides `.env` | Process value wins |
| Missing required PostgreSQL setting | `ValidationError` occurs before health check |
| Windows SQL Server authentication | Username/password are not required |
| SQL Server SQL authentication without credentials | Validation fails clearly |
| Default PostgreSQL password outside development | Validation fails |
| Invalid batch/retry values | Validation fails before pipeline execution |
| Secret logging | Password values never appear in logs or rendered reports |

### Bronze read, batch, and streaming assessment

| Step | Current implementation | Assessment |
|---|---|---|
| SQL Server read | `SalesExtractor` uses `cursor.fetchall()` | Reads the entire source table into memory; no batch or streaming read |
| DataFrame creation | All fetched rows are converted into one DataFrame | Memory usage grows with source table size |
| PostgreSQL write | `to_sql(..., method="multi", chunksize=1000)` | Has write batching only; this is not end-to-end streaming |
| Legacy Bronze utility | `bronze_ingest.py` also uses `fetchall()` and `executemany()` with all values prepared in memory | Same memory and failure-recovery limitations |

Target Bronze flow:

```text
SQL Server cursor
  -> fetchmany(batch_size)
	  -> DataFrame batch
		  -> lineage and record hash
			  -> batch validation
				  -> staging/load
					  -> batch audit
```

The implementation must explicitly decide batch size, commit scope, retry scope, ordering, and resume strategy. Batch-level `drop_duplicates()` alone is unsafe when the same business key can occur in different batches.

#### Decided Bronze implementation baseline

The following decisions are approved and must be used as the baseline for implementation. They are no longer open design alternatives:

| Decision area | Approved decision | Implementation rule |
|---|---|---|
| Source read | Cursor batching with `fetchmany()` | `SalesExtractor` exposes batch iteration; canonical Bronze loads must not use `fetchall()` |
| Batch representation | One pandas DataFrame per batch | Add lineage and deterministic record hash within the batch before loading |
| Initial batch size | `10,000` rows, configurable through `Settings` | Validate the value as a positive bounded setting |
| Source ordering | Stable `ORDER BY` on a source key | Offset/page number is not a resume checkpoint |
| Bronze deduplication | Preserve raw fidelity; no business-key deduplication in Bronze | Global deduplication belongs to Silver or an explicitly approved target rule |
| Write target | Run/load-specific staging table | Do not replace the published Bronze table while extraction is running |
| Commit scope | Commit each successful batch to staging | Write the checkpoint only after the corresponding batch commit succeeds |
| Table publication | Publish staging only after whole-table validation passes | Keep the previous valid Bronze target until publish succeeds |
| Retry scope | Retry the same logical batch, not a new batch | Reuse the same `run_id`, `load_id`, and `batch_id` for every attempt |
| Retry conditions | Only classified transient errors | Do not retry schema, authentication, SQL, contract, or deterministic validation errors |
| Retry policy | Maximum 3 attempts with bounded exponential backoff and jitter | Log attempt number, delay, error class, and final outcome |
| Idempotency protection | Deterministic record/batch identity plus unique protection or reconciliation | Reconcile an uncertain commit before retrying |
| Incremental checkpoint | Stable watermark or key range, advanced transactionally with the successful batch | Never advance a checkpoint before data commit |
| Rejected data | Use quarantine mode for isolatable row-level errors; store rejected rows separately with reason and load identity | Valid rows continue to staging; rejected rows never disappear silently and full payloads do not go to normal logs |

Canonical Bronze implementation flow:

```text
create run_id/load_id
  -> query source with stable ordering
	  -> fetchmany(10_000)
		  -> build DataFrame batch
			  -> add lineage/hash
				  -> validate batch
					  -> write staging batch
						  -> commit data and checkpoint atomically
							  -> retry same identity only for transient errors
								  -> validate complete staging table
									  -> publish staging to Bronze
```

Any implementation that uses `fetchall()`, appends directly to the published table, advances a checkpoint before commit, blindly retries an uncertain write, or silently drops a rejected row does not satisfy this Phase 4 baseline.

### Bronze error and rejected-data handling

| Area | Current behavior | Impact | Required enhancement |
|---|---|---|---|
| Table failure isolation | `SalesBronzeIngestionJob` has no per-table `try/except` around extract, load, and validation | One exception can stop the batch without a complete report | Return a result for every attempted table and apply an explicit stop/continue policy |
| Row-level quarantine | No rejected-record or error table exists | Invalid records are neither preserved for investigation nor explained | Use quarantine mode for isolatable row errors; add `bronze.rejected_records` or an equivalent design with reason and run ID |
| Audit | Main job does not persist complete start, finish, batch, count, and error details | There is no durable operational history | Record run ID, table, batch, rows read/written/rejected, status, error type, and timestamps |
| Full-load safety | `if_exists="replace"` may replace the target before a later load failure | A failed run can leave Bronze incomplete or remove the previous valid table | Load to staging and publish after validation, or use a recoverable transaction strategy |
| Logging | Connectors log connection failures, but the job does not consistently log table/batch context | Root-cause analysis is slow | Use structured logs with `run_id`, `stage`, `source_table`, `target_table`, `batch_id`, counts, status, and sanitized error details |

Bronze uses **quarantine mode** for a row-level error that can be isolated safely: valid rows in the batch continue to staging, while the rejected row is written to a controlled quarantine table or file with its reason and load identity. This is not allowed to hide system, schema, or contract failures. Do not write full record payloads to normal logs; log only identifiers and metadata unless the data classification explicitly allows more detail.

The default rejected-row threshold is `0` for Bronze raw loads, but quarantine mode is still the handling mechanism: a rejected row is recorded as `REJECTED`, the table is not reported as an unconditional `SUCCESS`, and the configured threshold determines whether the table may publish as `SUCCESS_WITH_REJECTIONS` or must fail. The threshold must be an explicit configuration and visible in the audit report.

Row-level error flow:

```text
batch
	-> validate each record
			-> valid records -> staging
			-> invalid records -> bronze.rejected_records
															-> reason + record_key + run/load/batch identity
	-> evaluate rejected threshold
			-> within policy: SUCCESS_WITH_REJECTIONS
			-> over policy: FAILED, do not publish
```

### Silver read, batch, and error assessment

| Area | Current behavior | Impact | Required enhancement |
|---|---|---|---|
| Read strategy | `_read_bronze()` calls `pd.read_sql_query()` without `chunksize` | Entire Bronze table is loaded into memory | Use controlled chunks or database-side SQL for large tables |
| Transformation | Rename, type conversion, deduplication, and selection operate on one full DataFrame | Large inputs increase memory pressure; batch-local deduplication can be incorrect | Use staging/database-side deduplication or maintain cross-batch state |
| Write safety | Silver writes directly with `to_sql(..., if_exists="replace")` | Mid-run failure may leave a partially replaced Silver table | Write to staging and publish only after transformation and validation pass |
| Missing Person data | A missing `bronze.person` table is handled with `print()` and fallback names | Warning is not structured and data-quality degradation may be overlooked | Log a warning with context and make fallback policy explicit in validation/reporting |
| Schema errors | Required columns are not validated before transformation | Missing or renamed source columns fail late | Add input/output schema contracts with table and column names in errors |

For Silver, streaming is not automatically correct: global deduplication by business key requires database-side window functions, staging, or state across all batches. A practical option is to perform the cleaning and deduplication in PostgreSQL, then publish the resulting Silver table atomically.

#### Decided Silver implementation baseline

The following decisions are approved and must be used as the implementation baseline:

| Decision area | Approved decision | Implementation rule |
|---|---|---|
| Source read | Chunked DataFrame read from PostgreSQL Bronze | Use controlled chunks with an initial size of `10,000` rows; do not load an entire Bronze table for canonical Silver processing |
| Processing model | Hybrid pandas and database approach | Use pandas for mapping, type conversion, trimming, flags, and enrichment; use database staging for global deduplication and publish |
| Input contract | Validate required Bronze columns before transformation | Missing table/column or schema mismatch is a table-level failure; do not quarantine an entire broken table as row errors |
| Type conversion | Detect coercion failures explicitly | Do not allow `errors="coerce"` to silently hide invalid values; isolate invalid rows with reason and source identity |
| Deduplication | Global deduplication after all chunks are staged | Do not call `drop_duplicates()` independently per chunk; use deterministic `ROW_NUMBER()`/`PARTITION BY` or an equivalent database rule |
| Deduplication order | Keep the latest deterministic record | Order by `_load_date DESC, _record_hash DESC` unless a more specific approved source-version rule exists |
| Row-level errors | Use quarantine mode | Valid rows continue to staging; invalid rows go to `silver.rejected_records` with reason and identity |
| System/schema errors | Fail closed | Do not publish a new Silver table when connection, schema, contract, or deterministic validation fails |
| Commit scope | Commit each transformed chunk to run-specific staging | Published Silver remains untouched while the run is incomplete |
| Retry scope | Retry the same logical chunk | Reuse `run_id`, `table_load_id`, and `batch_id`; retry only classified transient errors |
| Retry policy | Maximum 3 attempts with bounded exponential backoff and jitter | Log attempt, delay, error class, and final outcome |
| Checkpoint | Write checkpoint after successful staging commit | Never advance progress before the data commit succeeds; reconcile uncertain commits before retry |
| Rejected threshold | Default `0` | A rejected row is recorded, but the table cannot be unconditional `SUCCESS`; publish as `SUCCESS_WITH_REJECTIONS` only when an explicit threshold permits it |
| Person enrichment | `bronze.person` is required for the salesperson Silver table | Missing Person data fails the salesperson transformation unless an explicit degraded mode is approved; no silent ID fallback |
| Publication | Validate complete staging, then atomic publish/swap | Keep the previous valid Silver version until the replacement passes all checks |
| Logging | Structured stage/table/batch logging | Include run/table/batch/attempt/error/status context; never log secrets or full raw payloads |

Canonical Silver implementation flow:

```text
identify Bronze snapshot/load
  -> create run_id/table_load_id
	  -> validate input schema
		  -> read Bronze in chunks of 10,000 rows
			  -> transform each chunk with pandas
				  -> detect conversion/data errors
					  -> valid rows -> Silver staging
					  -> invalid rows -> silver.rejected_records
						  -> commit chunk and checkpoint
							  -> retry same identity only for transient errors
								  -> global deduplication on staging
									  -> validate complete staging table
										  -> publish/swap Silver atomically
```

Any implementation that reads the full Bronze table into memory, deduplicates independently per chunk, silently coerces invalid values to null, falls back to salesperson IDs without an approved degraded mode, publishes partial Silver, or retries an uncertain write blindly does not satisfy this Phase 4 baseline.

Required Silver result statuses:

| Status | Meaning |
|---|---|
| `SUCCESS` | All chunks and validations pass with no rejected rows |
| `SUCCESS_WITH_REJECTIONS` | Valid rows pass and rejected rows are within an explicitly approved threshold |
| `FAILED` | System/schema/contract error, rejected threshold exceeded, or validation failure; no new Silver publish |

### Gold read, batch, and publish safety assessment

| Area | Current behavior | Impact | Required enhancement |
|---|---|---|---|
| Read strategy | Gold reads all Silver tables with `pd.read_sql_query()` without `chunksize` | Full Silver tables are held in memory | Use database-side joins or controlled batches for large fact data |
| Fact construction | Header/detail join and measure calculation happen in pandas | Memory and runtime increase with fact size | Prefer SQL staging for the fact join, or use a stateful batch design that preserves grain and referential integrity |
| Publish order | `_reset_gold_tables()` drops Gold tables before Silver data is fully read and rebuilt | Highest current availability risk; a failure can remove the last valid Gold dataset | Build all dimensions/facts in staging, validate them, then publish/swap |
| Constraints | Constraints are added only after `to_sql(replace)` | A failure can leave missing constraints or an incomplete schema | Manage DDL separately and verify PK/FK/data types before publication |
| Validation timing | Referential integrity may be discovered when constraints are added | Failure happens late after destructive operations | Validate staging data before touching published Gold |

Target Gold flow:

```text
Read Silver
  -> build Gold staging tables
	  -> validate grain, keys, measures, and references
		  -> add/verify constraints
			  -> publish or swap
				  -> retain previous Gold until success
```


#### Decided Gold implementation baseline

The following decisions are approved and must be used as the implementation baseline:

| Decision area | Approved decision | Implementation rule |
|---|---|---|
| Dimension read | Full-read is acceptable for current small dimensions | Do not force streaming for `dim_date`, `dim_customer`, `dim_product`, `dim_territory`, or `dim_salesperson` unless volume requires it |
| Fact read/build | Process `fact_sales` by batch or database-side SQL | The initial target is database-side SQL JOIN/staging; pandas chunking is allowed as a migration step |
| Source ordering | Stable `ORDER BY sales_order_detail_id` | Checkpoint by key/range, never by offset or page number |
| Fact grain | One row per `sales_order_detail_id` | Duplicate fact key is a validation failure, not a row to silently discard |
| Transformation location | Prefer PostgreSQL for large fact JOIN and measure calculation | Keep pandas only for logic that still requires compatibility during migration |
| Input snapshot | Build from an identified Silver snapshot/load | Do not mix source versions within one Gold run |
| Write target | Run-specific staging tables | Never write directly to published Gold tables during build |
| Commit scope | Commit each fact batch to staging | The published Gold version remains untouched until the complete run is valid |
| Quarantine | No Gold quarantine by default | Resolve row-level conversion issues in Silver; Gold integrity/business-rule errors fail the build |
| Validation | Validate staging before publication | Check schema, grain, keys, measures, nulls, and referential integrity before publish or FK creation |
| Retry conditions | Only classified transient database errors | Do not retry missing tables/columns, duplicate keys, orphan keys, business-rule violations, or KPI mismatches |
| Retry policy | Maximum 3 attempts with bounded exponential backoff and jitter | Retry the same `run_id`, `table_load_id`, and `batch_id`; log attempt and final outcome |
| Uncertain commit | Reconcile staging/audit before retry | Never blindly append a fact batch after a client timeout |
| Idempotency | Unique `sales_order_detail_id` plus batch identity | Repeated execution must produce one logical fact row per detail key |
| Publication | Atomic publish/swap after all validations pass | Retain the previous valid Gold version when build or validation fails |
| Constraints | Add/verify constraints on staging before publish | Published Gold must retain PK/FK and data-type contract after every run |
| Logging | Structured run/table/batch/attempt/error/status logging | Never log secrets, full connection strings, or full raw payloads |

Canonical Gold implementation flow:

```text
identify Silver snapshot/load
  -> create run_id/table_load_id
      -> build small dimensions in staging
          -> build fact with database-side JOIN or stable batches
              -> validate schema, grain, measures, keys, nulls, and references
                  -> add/verify staging constraints
                      -> reconcile any uncertain commits
                          -> atomic publish/swap to Gold
                              -> keep previous Gold on failure
```

Gold error policy:

| Error type | Policy |
|---|---|
| Transient database error | Retry the same atomic batch/unit |
| Missing Silver table/column | Fail closed; do not publish |
| Duplicate fact key | Fail closed; do not silently deduplicate |
| Orphan dimension key | Fail closed; validate before publish |
| Measure/business-rule violation | Fail closed; investigate Silver or the rule |
| Row-level conversion issue from source | Must be handled in Silver quarantine before Gold |
| Build failure after staging commit | Keep published Gold unchanged; clean or expire failed staging |

Any implementation that drops published Gold before a successful build, writes directly to published Gold, uses offset checkpoints, silently drops fact rows, quarantines Gold integrity failures by default, or blindly retries an uncertain write does not satisfy this Phase 4 baseline.

### Connector context-manager behavior

| Finding | Current behavior | Impact | Proposed direction |
|---|---|---|---|
| Failed connection is not raised at entry | `BaseConnector.__enter__()` calls `connect()` but ignores a `False` result | The `with` block continues and later fails with `Not connected` | Raise a clear `ConnectionError` immediately when connection establishment fails |
| Error boundary is unclear | Connection failure and query failure are separated by several call sites | Troubleshooting loses the original failure context | Treat connection establishment as a strict boundary and include connector/config context without secrets |

### Priority for the next technical discussion

| Priority | Discussion decision |
|---|---|
| P0 | Central settings and `.env` loading; fail-fast health/config gate; safe Gold staging/publish; per-stage and per-table error handling |
| P1 | Bronze batch read; Silver/Gold batch or database-side transformation; audit tables; retry/backoff; rejected-data handling |
| P2 | Resume from failed batch; watermark-based incremental loading; row-level quarantine policy; operational metrics |

### Retry and idempotency review

#### Current assessment

| Area | Current behavior | Assessment |
|---|---|---|
| Retry implementation | No retry, backoff, attempt counter, or retryable-error classification exists in the current pipeline | A transient connection or load failure fails the operation immediately |
| Bronze write identity | Full mode uses `replace`; incremental mode uses `append` | Repeating an operation can replace valid data or append duplicates |
| Batch checkpoint | No batch ID, watermark, checkpoint, or committed-progress record is persisted | A restarted run cannot safely determine where to resume |
| Duplicate protection | No unique constraint or `ON CONFLICT`/upsert strategy is defined for loaded business records | A retry after an uncertain commit can create duplicate rows |
| Silver and Gold rebuilds | Silver and Gold use full DataFrames and destructive `replace`/drop flows | Retrying a partially completed transformation can expose incomplete or inconsistent tables |

#### Core principle

Retry and idempotency solve different problems:

- **Retry** answers: “Should the same operation be attempted again after a failure?”
- **Idempotency** answers: “If the same operation runs more than once, will the final data state remain correct?”

Retry must only be enabled after the operation has an idempotent boundary. A retry by itself can turn an uncertain database commit into duplicate data.

#### Recommended operation identity

Every pipeline run and batch should have a stable identity:

```text
run_id       = one complete pipeline execution
load_id      = one source-to-target table load within the run
batch_id     = one ordered batch within the load
record_key   = source primary key or deterministic record hash
```

The same retry must reuse the same `load_id` and `batch_id`; it must not generate a new logical load identity for every attempt.

#### Recommended retry policy

| Rule | Recommendation |
|---|---|
| Retryable errors | Network timeout, connection reset, temporary database unavailability, deadlock, and explicitly classified transient errors |
| Non-retryable errors | Missing table/column, authentication failure, invalid SQL, schema mismatch, data contract failure, and deterministic validation failure |
| Backoff | Exponential backoff with jitter, for example 1s, 2s, 4s, then a bounded maximum |
| Attempt limit | Small fixed maximum such as 3 attempts; configurable per environment |
| Transaction boundary | Retry the complete atomic unit; never blindly retry part of a transaction after an uncertain commit |
| Observability | Log attempt number, run/load/batch IDs, error class, wait time, and final outcome without secrets or full row payloads |
| Stop policy | Exhausted retries mark the batch/table/stage failed and prevent publication when the stage is required |

#### Bronze design recommendation

Use a staging table and deterministic uniqueness rule before publishing:

```text
fetch batch
  -> calculate record_key
	  -> write to staging with run_id/load_id/batch_id
		  -> commit atomically
			  -> record checkpoint
				  -> retry the same batch if transient failure
```

For incremental loads, read using a stable watermark or source key range, enforce a unique idempotency key, use upsert or `ON CONFLICT` where the business rule allows it, and advance the watermark in the same transaction as the successful batch commit.

The critical rule is: **never advance a checkpoint before the corresponding data commit succeeds**. If the commit outcome is unknown, reconcile by querying the idempotency key before retrying.

#### Silver and Gold design recommendation

Silver transformations should be deterministic for the same Bronze snapshot and transformation version. Retry should rerun the same staging transformation, not append a second Silver copy. Global deduplication requires database-side window functions, staging, or state across all batches.

Gold should be rebuilt into run-specific staging tables. A retry can safely rebuild only that staging run because published Gold is untouched. Validate grain, measures, keys, and references before atomic publish/swap. Do not retry the current `DROP Gold -> to_sql(replace)` sequence because its destructive boundary is not idempotent or availability-safe.

#### Required acceptance tests

| Scenario | Expected result |
|---|---|
| Transient read failure before any write | Same batch retries and loads once |
| Transient write failure before commit | Retry leaves one logical batch, not duplicates |
| Failure after commit but before checkpoint | Reconciliation detects the committed batch; retry does not duplicate it |
| Same full run executed twice | Published target remains correct and contains no duplicate records |
| Incremental run retried with same watermark | No duplicate business records; watermark advances once |
| Silver transformation retry | Previous published Silver remains valid until replacement passes validation |
| Gold build failure during retry | Previous published Gold remains available and unchanged |

## Domain separation and Bronze architecture baseline

### Current finding

`SalesBronzeIngestionJob` currently orchestrates seven tables across three business areas: Sales, Production, and Person. The technical components are partly separated into extractor, loader, and validator classes, but the domain ownership and table mapping remain coupled inside one Sales job.

### Decided architecture

The project will separate Bronze by **business domain**, not mechanically by one file per table:

```text
PipelineRunner
	-> SalesBronzeJob
	-> ProductionBronzeJob
	-> PersonBronzeJob
```

| Decision area | Approved decision | Implementation rule |
|---|---|---|
| Domain ownership | One Bronze job per business domain | Sales owns order header/detail, customer, territory, and salesperson; Production owns product; Person owns person data |
| Table granularity | Use table specifications instead of one class/file per simple table | Create a separate table class only when query, schema, watermark, quarantine, or business policy is materially different |
| Shared processing | Create one shared ingestion engine | Batch read, staging, retry, audit, checkpoint, quarantine, and publish behavior must not be duplicated in domain jobs |
| Table metadata | Move extraction maps into `TableSpec` definitions | Store source schema/table, target table, primary key, required columns, ordering key, and incremental column in the specification |
| Domain policy | Keep domain-specific rules at the feature/domain layer | Do not put Sales, Production, or Person business rules into the shared ingestion engine |
| Product and Person ownership | Remove `Production.Product` and `Person.Person` from the Sales Bronze job | Sales may declare Person as a downstream Silver enrichment dependency, but does not own Person ingestion |
| Orchestration | Domain jobs return independent structured results | Pipeline decides whether one domain failure produces partial success or fails the complete required workflow |
| Compatibility | Keep the existing Sales job as a temporary compatibility wrapper | Wrapper delegates to the new Sales domain job during migration; no new logic is added to the wrapper |
| Testing | Test shared engine, domain jobs, and table specifications separately | A failure in one domain must not require live execution of unrelated domains |

Recommended structure:

```text
src/
├── shared/
│   └── ingestion/
│       ├── batch_ingestion_engine.py
│       ├── checkpoint_manager.py
│       ├── ingestion_models.py
│       └── retry_policy.py
└── features/
		├── sales/
		│   ├── domain/bronze/table_specs.py
		│   └── jobs/sales_bronze_job.py
		├── production/
		│   ├── domain/bronze/table_specs.py
		│   └── jobs/production_bronze_job.py
		└── person/
				├── domain/bronze/table_specs.py
				└── jobs/person_bronze_job.py
```

Example table specification:

```python
@dataclass(frozen=True)
class TableSpec:
		source_schema: str
		source_table: str
		target_table: str
		primary_key: str
		required_columns: tuple[str, ...]
		ordering_key: str
		incremental_column: str | None = None
```

The shared engine owns execution mechanics. Domain jobs only provide table specifications, domain policies, and dependencies. A table that differs only by source/target names or key metadata must remain a specification, not become a new class.

Migration order:

1. Extract the current `extraction_map` into `TableSpec` definitions.
2. Create `SalesBronzeJob` with only Sales-owned tables.
3. Create `ProductionBronzeJob` for Product and `PersonBronzeJob` for Person.
4. Move batch/retry/audit/checkpoint/staging mechanics into the shared engine.
5. Inject settings, extractor, loader, validator, and policies into domain jobs.
6. Keep `SalesBronzeIngestionJob` as a delegating compatibility wrapper.
7. Add independent tests for the shared engine and each domain job.

Any implementation that creates one monolithic platform Bronze job, duplicates batch/retry mechanics in every domain, puts domain rules into shared infrastructure, or creates one class per simple table without distinct behavior does not satisfy this Phase 4 baseline.

## Main task checklist

| Done | Main task | Subtask | Description | Priority | Status | Impact current | Benefit after enhancement | Acceptance criteria / evidence | Dependencies |
|---|---|---|---|---|---|---|---|---|---|
| [ ] | Pipeline orchestration | Define pipeline contract | Define stages, inputs, outputs, statuses, failure policy, and run result schema | P0 | Not started | Current runtime stops at Bronze and has no unified contract | Every stage has predictable behavior and machine-readable results | Approved pipeline contract documented; result includes stage status and row counts | Scope confirmation |
| [ ] | Pipeline orchestration | Implement `PipelineRunner` | Orchestrate Bronze, Silver, Silver validation, Gold, and Gold KPI validation | P0 | Not started | Operators must run multiple commands manually | One controlled command can run the complete Sales pipeline | `python -m ...` runs stages in order and returns complete report | Pipeline contract |
| [ ] | Pipeline orchestration | Add CLI options | Support `full`, `incremental` where valid, stage selection, and log level | P0 | Not started | Runtime behavior is hidden in code and scripts | Repeatable operational execution with explicit parameters | Invalid options fail clearly; help text documents supported modes | Pipeline runner |
| [ ] | Pipeline orchestration | Add process exit codes | Return exit code `0` only for successful required stages | P0 | Not started | Automation cannot reliably detect failure | CI, scheduler, and operators can detect success/failure | Successful run exits `0`; failed validation/load exits non-zero | Pipeline runner |
| [ ] | Pipeline orchestration | Add stage-level exception handling | Capture errors per stage and stop or continue according to policy | P0 | Not started | Unhandled exceptions provide incomplete run context | Failures are summarized with stage and root error | Report contains failed stage, error message, and final status | Pipeline contract |
| [ ] | Health and readiness | Make health check a pipeline gate | Prevent data processing when required SQL Server/PostgreSQL checks fail | P0 | Not started | `degraded` health currently still allows Bootstrap and Bronze to run | Avoids predictable failures and partial processing | No downstream stage runs when required dependency is unavailable | Pipeline runner; health service |
| [ ] | Health and readiness | Validate schemas and required tables | Check Bronze/Silver/Gold schemas and required source/target objects | P1 | Not started | Connection success does not guarantee usable warehouse state | Early, actionable readiness failures | Readiness report identifies missing schema/table | Bootstrap and database access |
| [ ] | Bootstrap and migrations | Replace placeholder bootstrap | Create or verify schemas, metadata tables, and required objects idempotently | P1 | Not started | Application reports success without actually preparing the platform | Fresh and existing environments can be prepared consistently | Bootstrap can run repeatedly without destructive changes | PostgreSQL |
| [ ] | Bootstrap and migrations | Add schema versioning | Record and validate database schema version | P1 | Not started | Docker initialization only runs on first volume creation | Database changes become traceable and deployable | Version table and migration check are tested | Bootstrap implementation |
| [ ] | Bronze reliability | Validate job mode strictly | Reject unsupported modes instead of treating every non-`full` value as append | P0 | Not started | A typo can silently select unsafe behavior | Invalid operational requests fail before data changes | Only documented modes are accepted | Bronze job |
| [ ] | Bronze reliability | Isolate table failures | Produce a result for each table and apply an explicit stop/continue policy | P1 | Not started | One table exception can terminate the whole batch without a complete report | Operators see all attempted tables and failures | Failure report identifies table, stage, and counts | Pipeline contract |
| [ ] | Bronze reliability | Add retries and backoff | Retry transient source/target connection and load failures | P1 | Not started | Temporary connectivity issues cause avoidable full-run failures | More resilient scheduled execution | Retry count and final error are recorded and tested | Connector behavior |
| [ ] | Bronze reliability | Add run audit metadata | Persist run ID, timestamps, mode, table, counts, status, and error | P1 | Not started | No durable history exists for operational investigation | Load history and lineage are queryable | Audit row exists for every attempted table | PostgreSQL metadata tables |
| [ ] | Bronze reliability | Make incremental loads idempotent | Define watermark or hash strategy and prevent duplicate append rows | P1 | Not started | Append mode can duplicate records on rerun | Safe reruns and controlled incremental processing | Repeating the same incremental input does not duplicate business records | Incremental design decision |
| [ ] | Bronze quality | Use complete Bronze validation | Invoke lineage, critical-column, null-tolerance, and count checks in the job | P1 | Not started | `validate_table()` exists but the main job only uses basic count validation | Data quality failures are caught before transformation | Job result includes all configured quality checks | Validation contract |
| [x] | Silver transformation | Encapsulate Silver script as a job/service | Move orchestration responsibilities from standalone `run()` into a reusable class | P0 | Done | Silver cannot be dependency-injected or controlled consistently | Silver can run from CLI, App, tests, or scheduler | Job accepts dependencies/configuration and returns standard result | Pipeline contract |
| [x] | Silver transformation | Define transformation contracts | Validate required input columns and output schema before writing | P1 | Done | Missing columns fail late with low-context errors | Schema drift is detected early | Contract failures identify table and missing columns | Silver job |
| [x] | Silver quality | Make validation a gate | Return non-zero/failure status when duplicate, null, row-loss, or orphan checks fail | P0 | Done | Validation reports can be generated without preventing downstream publication | Invalid Silver data cannot silently feed Gold | Pipeline stops before Gold when Silver validation fails | Silver validation service |
| [ ] | Silver quality | Resolve customer/person enrichment scope | Decide whether customer names should be enriched from `Person.Person` or remain account-based | P2 | Needs clarification | Customer naming behavior is inconsistent with possible business expectations | Business meaning of customer dimension is explicit | Decision recorded with test and data-quality acceptance rule | User/business decision |
| [ ] | Gold reliability | Encapsulate Gold load as a job/service | Make Gold build callable through the common orchestration contract | P0 | Not started | Gold is only available as a standalone script | Gold participates in controlled pipeline execution | Job returns table counts and publication status | Pipeline contract |
| [ ] | Gold reliability | Replace destructive publish flow | Load staging tables and publish only after successful build and validation | P0 | Not started | `DROP` plus `to_sql(replace)` can leave Gold incomplete after failure | Existing published Gold remains available until replacement is valid | Failed build leaves previous Gold intact; successful build publishes all tables | PostgreSQL strategy decision |
| [ ] | Gold reliability | Preserve constraints and types | Manage DDL separately from data loading and reapply constraints deterministically | P1 | Not started | Replacing tables can remove constraints and produce inconsistent schema | Gold contract remains stable across reruns | PK/FK/type assertions pass after every publish | Gold publish design |
| [ ] | Gold quality | Validate referential integrity before publish | Check orphan keys and required dimensions before adding foreign keys | P0 | Not started | Constraint creation can fail late after data has been written | Errors are found before publication | Pre-publish validation report has zero invalid references | Gold job |
| [ ] | Configuration | Introduce central typed settings | Consolidate SQL Server, PostgreSQL, batch, retry, and logging configuration | P1 | Not started | Connectors independently interpret environment variables | Configuration is validated once and shared consistently | Missing/invalid settings produce clear startup errors | Scope of settings |
| [ ] | Configuration | Add environment template checks | Keep `.env.example` aligned with required runtime settings | P1 | Not started | New environments may miss required variables | Setup becomes reproducible | Automated check detects missing template keys | Settings model |
| [x] | Testing | Mark integration tests | Separate database-dependent tests from pure unit tests | P0 | Done | `pytest` can otherwise hit unavailable PostgreSQL/SQL Server | Developers can run fast unit tests independently | `pytest -m "not integration"` is deterministic; integration marker exists | `pytest.ini`, marked integration modules |
| [ ] | Testing | Add service fixtures and mocks | Test App and pipeline stage sequencing without live databases | P0 | Not started | Current App contract tests do not verify execution order or failure gates | Orchestration behavior is tested cheaply | Tests cover success, degraded health, stage failure, and exit status | Pipeline runner |
| [ ] | Testing | Add integration test setup | Provide controlled PostgreSQL startup/fixture and explicit SQL Server prerequisite | P1 | Blocked | Current integration tests fail because Docker/PostgreSQL is unavailable | Integration evidence becomes reproducible | Documented setup passes connectivity and schema tests | Docker daemon; SQL Server |
| [ ] | Testing | Add regression tests for reruns | Test full rerun, partial failure, duplicate prevention, and publish safety | P1 | Not started | Operational edge cases are unverified | Enhancements do not regress data integrity | Rerun and failure scenarios have automated assertions | Bronze/Gold reliability work |
| [ ] | Observability | Standardize structured logging | Include run ID, stage, table, status, duration, and error context | P1 | Not started | Current logs are mostly connection-level and text-based | Faster diagnosis and better automation integration | Logs can be filtered by run/stage/table | Pipeline runner |
| [x] | Observability | Add pipeline summary report | Generate Markdown/JSON summary for every run | P1 | Done | Results were scattered across console output and separate reports | Operators and reviewers get one evidence artifact | Report includes stage status, counts, durations, failures, and report metadata | `src/app/reporter.py`, `docs/project/PHASE_4E_EVIDENCE.md` |
| [x] | Delivery | Add CI checks | Run unit tests, integration-aware test selection, lint, formatting, and type checks | P1 | Done | Quality depended on manual local execution | Regressions are caught before merge | CI workflow runs unit/quality/integration-aware jobs and retains artifacts | `.github/workflows/ci.yml` |
| [x] | Delivery | Update operational documentation | Document one-command execution, prerequisites, recovery, and troubleshooting | P1 | Done | Manual workflow lacked target operational detail | Users can run and recover the platform consistently | README/runbook match implemented behavior | `README.md`, `docs/project/PHASE_4E_RUNBOOK.md` |

## Recommended implementation order

| Order | Scope | Reason |
|---:|---|---|
| 1 | Mark integration tests and document environment prerequisites | Establishes a trustworthy test baseline immediately |
| 2 | Define common result/status contract and implement `PipelineRunner` | Creates the control point for every later enhancement |
| 3 | Add health gating, exception handling, exit codes, and service-level tests | Makes execution behavior predictable and automatable |
| 4 | Implement real bootstrap and central settings | Removes hidden environment assumptions |
| 5 | Add Bronze audit, retry, strict modes, and idempotent incremental behavior | Protects source ingestion and reruns |
| 6 | Encapsulate Silver/Gold and make validation mandatory gates | Brings existing scripts under one lifecycle |
| 7 | Replace destructive Gold publishing with staging/publish | Protects the currently published warehouse |
| 8 | Add structured observability, CI, and runbook updates | Makes the solution maintainable in shared or scheduled environments |

## Definition of Done for Phase 4

- [ ] A single documented command runs the intended Sales pipeline.
- [ ] Health/readiness failures prevent unsafe downstream processing.
- [ ] Every stage returns a standard result with status, counts, duration, and errors.
- [ ] Silver and Gold validations are enforced as pipeline gates.
- [ ] Bronze loads have audit history and explicit rerun behavior.
- [ ] Gold publication does not destroy the last valid dataset when a build fails.
- [x] Unit tests run without external services; integration tests are explicitly marked and documented.
- [x] CI runs the agreed test, lint, formatting, and type-check commands.
- [x] Operational documentation and this checklist reflect the implemented behavior.

## Evidence log

| Date | Item | Result | Evidence |
|---|---|---|---|
| 2026-09-03 | Branch and worktree review | `Enhance_Project` at `f6a681a`; worktree clean | Git branch/status review |
| 2026-09-03 | Test baseline | `31 passed, 3 failed` | `pytest -q` |
| 2026-09-03 | Database environment | PostgreSQL unavailable at `localhost:5432`; Docker daemon unavailable | PostgreSQL connection errors and `docker compose ps` |
| 2026-09-07 | Phase 4E delivery evidence | Unit lane `182 passed, 22 deselected`; focused Phase 4E `25 passed`; full regression `204 passed`; integration lane `22 passed`; CLI help, compile, and diff checks pass | `docs/project/PHASE_4E_EVIDENCE.md`, `.github/workflows/ci.yml` |
