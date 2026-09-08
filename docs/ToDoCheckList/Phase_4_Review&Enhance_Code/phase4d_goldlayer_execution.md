# Phase 4D - Gold Layer Impact and Execution Plan

## 1. Purpose

This document translates the approved Phase 4 Gold baseline into an implementation scope, impact analysis, task breakdown, and verifiable acceptance criteria.

Phase 4D covers:

1. Encapsulate Gold loading as an injectable job/service.
2. Keep full-read processing for currently small dimensions.
3. Build `fact_sales` with stable-key batches.
4. Run integrity, grain, key, measure, and KPI validation before publication.
5. Create and verify constraints on staging before publication.
6. Publish Gold atomically with safe retry and reconciliation behavior.
7. Run focused Gold regression tests and the appropriate repository regression suite.

Phase 4D does not redesign the Silver transformation, add unrelated Gold facts, or change approved business measures without a recorded decision.

### 1.1 Approved decisions

The following decisions are the implementation contract for Phase 4D. Where earlier sections differ, these decisions take precedence.

| Topic | Approved decision |
|---|---|
| Canonical pipeline | Extend `PipelineRunner` to run Gold after Bronze and Silver; do not create a parallel production orchestration path. Gold failure returns pipeline `FAILED` with `failed_stage="gold"`; `App` maps it to `degraded`. |
| Stage gates | Use a generic `StageSnapshotGate` with thin `BronzeSnapshotGate`, `SilverSnapshotGate`, and `GoldPublishGate` wrappers. Gates validate required tables, status, published state, identity, and target contract; they do not validate KPI or business rules. |
| Silver snapshot | Phase 4D uses the existing metadata-based snapshot gate and does not redesign Silver as an atomic multi-table publication. Gold runs only when all six Silver targets are published and share the same `source_snapshot_id`. |
| Identity | Gold results distinguish `pipeline_snapshot_id`, `source_snapshot_id`, `gold_run_id`, and `gold_load_id`. A retry of the same load keeps `gold_load_id`; a batch retry keeps `batch_id`; a pipeline rerun creates a new `gold_run_id`; the same Silver snapshot may produce a new Gold version. |
| Gold publication | Use a versioned schema and current pointer; do not publish Gold tables independently. Build the candidate in its own schema version and update the current pointer only after every validation and KPI check passes. |
| Gold schema | Phase 4D builds six tables: `dim_date`, `dim_customer`, `dim_product`, `dim_territory`, `dim_salesperson`, and `fact_sales`. Keep `created_at`, and add `gold_version` and `source_snapshot_id`. Map `class -> product_class` and `style -> product_style`. `dim_customer` preserves the approved Silver semantics for `customer_name`, `customer_type`, and `customer_name_source`. |
| Fact strategy | Use the stable-key batch strategy on `sales_order_detail_id`; do not use offsets/page numbers and do not implement a SQL-side fact path in Phase 4D. |
| KPI gate | Run KPI validation against the candidate version before updating the current pointer, using the Silver baseline from the same `source_snapshot_id` and a default 2% tolerance. KPI mismatch is deterministic and is not retried. |
| Builder compatibility | Preserve the current pure-builder APIs and return types. The new Gold job owns pre-validation, staging, retry, audit, and publication; legacy `run()` only delegates. |
| Result model | Reuse `ExecutionIdentity`, `IngestionStatus`, and base result fields; add `GoldTableResult` and `GoldRunResult` without duplicating identity or status logic. |
| Concurrency | Allow only one active Gold run in Phase 4D. |
| Retention | Keep at least one previous Gold version; retain failed candidates for 24-72 hours; retain backups until audit completion; cleanup must be idempotent and rerunnable. |

### 1.2 Current pointer and KPI rules

Each Gold run creates a `gold_version` and a separate candidate schema, for example `gold_v002`. Consumers continue to use stable views or pointer objects under the `gold` namespace; consumers must not query versioned schemas directly.

Publication order:

```text
build candidate schema
  -> validate schema, grain, references, measures, and constraints
      -> validate KPI against candidate and Silver snapshot
          -> atomically update current pointer
              -> write publication audit
```

If any step before the pointer update fails, the pointer remains unchanged and the current Gold version continues serving data. KPI policy:

- The candidate and Silver baseline must use the same `source_snapshot_id`.
- The default tolerance is 2% relative variance.
- When the baseline is zero, only a zero candidate passes; a non-zero candidate fails.
- KPI failure is not retried, published, or allowed to change the current pointer.
- The report records `gold_run_id`, `gold_version`, `source_snapshot_id`, candidate identity, tolerance, and each metric comparison.

## 2. Baseline and current implementation

### 2.1 Confirmed implementation surface

| Area | Current location | Current behavior | Phase 4D concern |
|---|---|---|---|
| Gold builders | `scripts/warehouse/postgres/gold/sales_gold_load.py` | Builds five dimensions and `fact_sales` with pandas | Preserve pure builders while moving orchestration and safety into a job/service |
| Gold orchestration | `scripts/warehouse/postgres/gold/sales_gold_load.py:run()` | Drops published Gold tables, reads all Silver tables, writes with `to_sql(..., if_exists="replace")`, then adds constraints | Destructive build can remove the last valid Gold version and expose partial output |
| Silver read | `_read()` in `sales_gold_load.py` | Uses full-table `pd.read_sql_query()` for all tables | Full-read remains only for small dimensions; fact input must use stable-key batches |
| Fact construction | `build_fact_sales()` | pandas many-to-one join of all detail and header rows | The Gold job must use stable-key batches with explicit grain and no silent row loss |
| Dimension construction | `build_dim_*()` | pandas selection and `drop_duplicates()` | Full-read remains allowed, but key uniqueness and deterministic selection must be validated |
| Constraints | `_add_constraints()` | Adds PK/FK to published tables after direct writes | Constraints must be created/verified on staging before publish |
| Destructive reset | `_reset_gold_tables()` | Drops six Gold tables with `CASCADE` before build | Must be removed from the canonical path; previous Gold must survive failures |
| KPI validation | `scripts/warehouse/postgres/gold/validate_sales_kpis.py` | Compares Gold metrics with Silver baseline after publication | Must become a pre-publish gate against the candidate Gold version |
| Gold schema | `scripts/warehouse/postgres/schema/03_create_gold_schema.sql` | Current DDL is incomplete for dimensions, fact PK/FK, and version metadata | Add versioned schemas, a current pointer, explicit types/constraints, and metadata contract |
| Existing tests | `tests/test_sales_gold.py` | Covers dimension builders, fact calculations, grain, null handling, and output columns | Add service, staging, integrity, retry, publish-preservation, and rerun coverage |

### 2.2 Important schema contract observations

The implementation must reconcile the current code/schema contract before publication:

- `fact_sales` grain is one row per `sales_order_detail_id`.
- `dim_customer`, `dim_product`, `dim_date`, `dim_territory`, and `dim_salesperson` require stable primary keys.
- `salesperson_id` may be NULL for online orders; the FK policy must allow this explicitly while rejecting non-null orphan keys.
- The pandas product builder currently emits `class` and `style`, while the SQL schema names the corresponding columns `product_class` and `product_style`. This mismatch must be resolved by an explicit mapping or schema decision and covered by a contract test; it must not be left to `to_sql` inference.
- `created_at` and other database-managed metadata must not be lost when a staging table is published or when constraints are applied.
- The Gold schema must contain exactly the six Phase 4D tables: five dimensions and `fact_sales`; the job must not build the other facts.
- `fact_sales.sales_order_detail_id` is the primary key, and all foreign keys must be declared and verified on the candidate before publication.
- Every candidate table contains `gold_version` and `source_snapshot_id`; `created_at` remains row metadata.
- Stable views or pointer objects under the `gold` namespace are the stable read surface; consumers must not query versioned schemas directly.

### 2.3 Falsifiable implementation hypothesis

The highest-risk Gold behavior is controlled by `_reset_gold_tables()`, the direct `to_sql(..., if_exists="replace")` loop, and `_add_constraints()` running after publication. A focused fake-publish test should prove that a build or validation exception leaves the previous published Gold version unchanged. A fact-grain test using duplicate or orphan keys should prove that invalid staging fails before any publish call. If either test reaches the published tables before failure, the publication boundary is still too late and must move into the staging/publish service.

### 2.4 Current public compatibility surface

The following builder functions and legacy entrypoint remain available during migration unless a separate compatibility decision is recorded:

- `build_dim_date(headers)`
- `build_dim_customer(customers)`
- `build_dim_product(products)`
- `build_dim_territory(territories)`
- `build_dim_salesperson(salespeople)`
- `build_fact_sales(details, headers)`
- `run()` as a legacy entrypoint delegating to the new Gold job/service

The compatibility wrapper must delegate only. Staging, retries, constraint management, validation, and publication logic belong to the new service and shared infrastructure.

## 3. Target architecture

```mermaid
flowchart LR
    A[Validated Silver snapshot/load] --> B[PipelineRunner]
    B --> C[GoldSnapshotGate]
    C --> D[GoldJob]
    D --> E[Small dimensions full-read]
    D --> F[Stable-key fact batches]
    E --> G[Candidate version schema]
    F --> G
    G --> H[Pre-publish integrity validation]
    H --> I[Constraints and KPI validation]
    I -->|pass| J[Atomic current pointer update]
    I -->|fail| K[Keep previous Gold version]
```

### 3.1 Gold tables and ownership

| Target | Grain/key | Build strategy | Publication requirement |
|---|---|---|---|
| `gold.dim_date` | One row per calendar date; PK `date_id` | Full-read header date range is allowed | Complete date range and unique key before publish |
| `gold.dim_customer` | One row per `customer_id`; PK `customer_id` | Full-read dimension is allowed | Required columns, unique key, customer references, and approved customer naming/source fields validated |
| `gold.dim_product` | One row per `product_id`; PK `product_id` | Full-read dimension is allowed | Output/schema naming aligned with DDL |
| `gold.dim_territory` | One row per `territory_id`; PK `territory_id` | Full-read dimension is allowed | Unique key and referenced territory keys validated |
| `gold.dim_salesperson` | One row per `salesperson_id`; PK `salesperson_id` | Full-read dimension is allowed | Nullable fact FK policy is explicit |
| `gold.fact_sales` | One row per `sales_order_detail_id`; PK `sales_order_detail_id` | Stable-key batches ordered by `sales_order_detail_id`, using `(lower_bound, upper_bound]` | Duplicate, orphan, null, measure, and KPI violations fail closed |

### 3.2 Component boundaries

| Component | Owns | Must not own |
|---|---|---|
| `GoldJob` or `SalesGoldJob` | Snapshot selection, table order, dimension/fact build coordination, result aggregation, and Gold policy | Environment parsing or duplicated retry/publish mechanics |
| Gold table/build specifications | Source tables, target tables, keys, required columns, data types, and FK map | Database transaction state and retry loops |
| Pure `build_dim_*`/`build_fact_sales` functions | Deterministic pandas transformation and measure calculation | Database writes, destructive DDL, retries, or publication |
| Fact staging builder | Stable-key batch extraction by `sales_order_detail_id`, bounds, and content hash | Offset/page-number checkpoints or changing published Gold tables |
| Gold validator | Schema, grain, key, null, reference, measure, and KPI checks | Silently dropping invalid rows or quarantining integrity failures |
| Constraint manager | PK/FK/type constraint creation and verification on staging | Applying constraints to an unvalidated published table |
| Shared staging/publish service | Run identity, candidate schema lifecycle, current pointer, cleanup, and reconciliation | Gold-specific business rules |
| Pipeline runner | Call Gold only after the Silver gate; block publication on Gold failure and return `failed_stage="gold"` | Gold transformation internals |

### 3.3 Standard result contract

Every Gold table and stage result must use the shared result vocabulary and include at least:

```text
pipeline_snapshot_id, source_snapshot_id, gold_run_id, gold_load_id,
batch_id when applicable, stage, source_table, target_table, status,
rows_read, rows_written, rows_rejected, attempt_count,
started_at, finished_at, error_type, error_message,
gold_version, candidate_schema, staging_identity, published,
validation_passed, constraints_verified, kpi_passed, previous_version
```

Model contract:

- Reuse `ExecutionIdentity`, `IngestionStatus`, and shared timing/count fields.
- `GoldTableResult` describes one dimension/fact load; `GoldRunResult` aggregates the six table results and publication state.
- A retry of the same load keeps `gold_load_id`; a batch retry keeps `batch_id` and bounds; a pipeline rerun creates a new `gold_run_id`.
- `GoldRunResult` is `SUCCESS` only after the current pointer has been updated successfully.

Required terminal statuses:

| Status | Gold meaning |
|---|---|
| `SUCCESS` | Build, constraints, all validation, and publication pass |
| `FAILED` | Build, schema, grain, integrity, measure, KPI, constraint, or publication failure; previous Gold remains unchanged |
| `RETRYING` | A classified transient operation is being retried; not a terminal result |

Gold does not use quarantine as the default response to integrity or business-rule errors. Those errors fail the build and are investigated upstream or in the rule definition.

## 4. Impact analysis

### 4.1 Code impact

| Impacted area | Expected change | Risk | Mitigation/evidence |
|---|---|---|---|
| Gold loader | Extract `run()` orchestration into an injectable job/service | Existing scripts/tests may rely on a constructor-free entrypoint | Keep builder signatures and delegating `run()`; add compatibility test |
| Dimension reads | Keep full-read for small dimensions | Dimensions may grow beyond current assumptions | Record volume assumption and add a future threshold/strategy boundary |
| Fact read/build | Use stable-key batches by `sales_order_detail_id` | Chunked pandas joins can lose header context or duplicate rows | Use `(lower_bound, upper_bound]`, preserve batch identity/content hash, and validate many-to-one joins |
| Fact grain | Enforce one row per `sales_order_detail_id` | Duplicate detail keys may be silently removed by current behavior | Duplicate key is a validation failure, never an implicit dedup rule |
| Gold publication | Replace drop-and-recreate flow with a candidate schema and current pointer | Pointer or swap failures can leave orphaned candidates or versions | Publish the complete candidate transactionally, keep the previous version, and clean up idempotently |
| Constraints | Apply PK/FK and verify types on staging first | `to_sql(replace)` can erase DDL constraints or infer wrong types | Explicit DDL/type contract plus staging constraint integration tests |
| KPI validation | Adapt `validate_sales_kpis.py` to candidate staging/version | Validating after publication cannot prevent bad Gold exposure | Run KPI comparison before publish; preserve report rendering |
| Silver snapshot | Use `SilverSnapshotGate` over existing published metadata | Result identity alone does not prove physical data consistency | Require all six targets, `published=true`, and one `source_snapshot_id` before Gold |
| Shared infrastructure | Reuse settings, identity, retry, audit, checkpoint, staging, and publish services | Gold-specific copies can diverge from Bronze/Silver behavior | Architecture review and shared contract tests |

### 4.2 Data and operational impact

- Gold is built in run-specific staging. The existing published Gold version remains available until every required table and validation has passed.
- Fact processing must use a deterministic source order based on `sales_order_detail_id`; offset/page-number checkpoints are prohibited.
- The fact join must preserve line-item grain and must not silently drop details with missing headers, products, customers, territories, or required dates.
- A nullable salesperson reference is valid only when the business rule permits online orders; a non-null orphan salesperson key fails validation.
- Gold integrity/business-rule errors are fail-closed. They are not converted into Gold quarantine rows.
- Constraint creation is part of staging validation, not a post-publication repair step.
- Retry applies only to transient database failures around an atomic unit. A failed deterministic build is not retried as a blind append.
- An unknown commit outcome must be reconciled by staging/audit/unique-key evidence before another fact batch attempt.
- The final result and audit must distinguish dimension rows, fact rows, rejected count (normally zero), duplicate/orphan counts, KPI status, and publication state.
- Only one Gold run may be active; a concurrent run must wait or be rejected according to the lock policy.
- Failed candidate schemas are retained for 24-72 hours; at least one previous Gold version is retained; cleanup is idempotent.

### 4.3 Out of scope and decisions required

| Item | Treatment |
|---|---|
| Additional facts | `fact_customer_orders`, `fact_inventory`, and `fact_purchasing` are out of scope; they may remain in the schema, but the Gold job must not build them. |
| Slowly changing dimensions | Not introduced in Phase 4D; current dimensions remain deterministic snapshots. |
| Gold row quarantine | Disabled by default; source conversion errors belong to Silver. |
| Dimension scaling | Full-read is accepted for current small dimensions; add a separate decision if volume thresholds are exceeded. |
| Product column naming | Keep DDL names `product_class`/`product_style`, map from builder `class`/`style`, and add a contract test. |
| KPI tolerance | Use the Silver baseline with the same `source_snapshot_id`, relative 2% variance, and require exact zero when the baseline is zero. |
| Gold pointer | Consumers read stable views/pointer objects under the `gold` namespace; they do not read versioned schemas directly. |
| Concurrency/retention | Allow one active Gold run; keep the previous version, retain failed candidates 24-72 hours, and make cleanup idempotent. |

## 5. Dependencies and execution order

```text
W1-W3 settings/shared contracts
    -> W4-W5 Bronze identity and publish evidence
        -> W6 Silver validated snapshot/load
            -> 6.1 Gold specs and injectable job
                -> 6.2 input/snapshot contracts
                    -> 6.3 dimensions and fact staging
                        -> 6.4 pre-publish integrity validation
                            -> 6.5 staging constraints
                                -> 6.6 KPI gate and atomic publish
                                    -> 6.6 retry/reconciliation and cleanup
                                        -> 6.7 Gold regression
```

Do not keep the destructive Gold flow in the canonical path. Do not add retry before fact batch identity, transaction scope, and unknown-commit reconciliation are testable. Do not create FK constraints on candidate data before orphan checks are complete.

## 6. Task breakdown

### 6.1 Package Gold as a job/service

| ID | Task | Output | Acceptance criteria | Dependency | Status |
|---|---|---|---|---|---|
| 6.1.1 | Define Gold table specifications | Immutable specs for five dimensions and `fact_sales` | Each table has source, target, key, required columns, expected types, and FK metadata | W3/W6 | Done |
| 6.1.2 | Create injectable Gold job | `SalesGoldJob` or equivalent with settings, reader, builders, validator, constraint manager, audit, and publisher | Job runs with fakes and returns standard results without a live database | 6.1.1 | Done |
| 6.1.3 | Require Silver snapshot gate | `SilverSnapshotGate` and source snapshot contract | All six Silver targets exist, are `published=true`, and share one `source_snapshot_id` before Gold runs | 6.1.2 | Done |
| 6.1.4 | Preserve legacy builders and `run()` | Delegating compatibility surface | Existing builder tests and legacy callers remain functional; wrapper contains no new mechanics | 6.1.2 | Done |

**Implementation note for 6.1.4:** `scripts/warehouse/postgres/gold/sales_gold_load.py:run()` is now delegation-only and requires the gated `pipeline_snapshot_id`/`silver_result`; `_reset_gold_tables()` and direct `to_sql(..., if_exists="replace")` are not part of the canonical path.

### 6.2 Dimension full-read strategy

| ID | Task | Output | Acceptance criteria | Dependency | Status |
|---|---|---|---|---|---|
| 6.2.1 | Read small dimensions fully | Injectable dimension reader | Full-read is limited to documented small dimensions and never used for the large fact path | 6.1.2 | Done |
| 6.2.2 | Validate dimension input/output schema | Dimension contract validator | Missing Silver table/column or mismatched output type fails before staging publish | 6.2.1 | Done |
| 6.2.3 | Make dimension selection deterministic | Stable key selection and explicit duplicate policy | Dimension keys are unique and the selected row is deterministic; no accidental `drop_duplicates()` behavior remains undocumented | 6.2.2 | Done |
| 6.2.4 | Resolve product column contract | Builder/schema mapping and test | Product output columns match the Gold DDL and downstream consumers exactly | 6.2.2 | Done |
| 6.2.5 | Build date dimension safely | Date-range builder contract | Empty or invalid date input returns a clear `FAILED` result; valid input produces a complete continuous date range | 6.2.1 | Done |

### 6.3 Stable-key fact batches

| ID | Task | Output | Acceptance criteria | Dependency | Status |
|---|---|---|---|---|---|
| 6.3.1 | Define fact grain contract | `sales_order_detail_id` uniqueness rule | Any duplicate fact key is a validation failure and is never silently removed | 6.1.1 | Done |
| 6.3.2 | Read fact by stable key | Batch reader ordered by `sales_order_detail_id` | Query uses `(lower_bound, upper_bound]`, no offset/page-number, and only reads the gated source snapshot | 6.3.1 | Done |
| 6.3.3 | Preserve batch identity on retry | Batch audit/checkpoint integration | Retry keeps `gold_load_id`, `batch_id`, bounds, and content hash; reconcile unknown commits before rewriting | 6.3.2 | Done |
| 6.3.4 | Validate many-to-one header join | Join contract | Header key is unique; missing required headers are integrity failures and never silently drop details | 6.3.2 | Done |
| 6.3.5 | Calculate and validate measures | Fact measures and type contract | `discount_amount`, `net_sales`, quantities, prices, and date keys match approved formulas and types | 6.3.2 | Done |
| 6.3.6 | Commit atomic fact batches | Batch audit/checkpoint integration | Data commit precedes checkpoint; transient retry does not create duplicate fact rows | W5/6.3.3 | Done |

### 6.4 Pre-publish integrity validation

| ID | Task | Output | Acceptance criteria | Dependency | Status |
|---|---|---|---|---|---|
| 6.4.1 | Validate staging schema and types | Candidate schema report | All target columns, names, nullability, and database types match the Gold contract | 6.2/6.3 | Done |
| 6.4.2 | Validate dimensions and keys | Dimension quality report | Required dimension PKs are non-null and unique; duplicate keys fail closed | 6.2 | Done |
| 6.4.3 | Validate fact grain and nulls | Fact quality report | One row per detail key; required fact fields are non-null; permitted nullable salesperson values are explicit | 6.3 | Done |
| 6.4.4 | Validate referential integrity | Orphan-key report | Zero orphan references for date, customer, product, territory, and non-null salesperson keys before FK creation | 6.3/6.4.2 | Done |
| 6.4.5 | Validate measures and business rules | Measure report | No invalid negative/overflow values or KPI rule violations under the approved contract | 6.3.5 | Done |
| 6.4.6 | Adapt KPI validation to staging | Candidate KPI report | Gold candidate is compared with the independent Silver baseline before publication; current tolerance is 2% | 6.3/6.4.5 | Done |

### 6.5 Constraints on staging

| ID | Task | Output | Acceptance criteria | Dependency | Status |
|---|---|---|---|---|---|
| 6.5.1 | Create candidate schema DDL | Versioned Gold schema by `gold_version` | Candidate has explicit types, `created_at`, `gold_version`, `source_snapshot_id`, and no `replace` inference | 6.1/6.3 | Done |
| 6.5.2 | Add staging primary keys | PK DDL/verification | All dimension keys and `fact_sales.sales_order_detail_id` have valid PKs before publish | 6.4.2/6.4.3 | Done |
| 6.5.3 | Add staging foreign keys | FK DDL/verification | FK creation succeeds only after orphan validation and preserves nullable salesperson semantics | 6.4.4 | Done |
| 6.5.4 | Verify constraints and types | Constraint inspection report | Database metadata confirms PK/FK/type contract on every candidate table | 6.5.1-6.5.3 | Done |
| 6.5.5 | Handle constraint failure | Fail-closed cleanup | Constraint failure does not alter published Gold and leaves staging recoverable or cleanable | 6.5.4 | Done |

**6.5 implementation note:** The database-independent `GoldConstraintManager` now creates explicit candidate DDL and verifies PK/FK/metadata contracts before publication. PostgreSQL execution and catalog inspection remain intentionally deferred to the Gold publish adapter in 6.6; no live database constraint result is claimed by the current unit evidence.

### 6.6 Atomic publication and retry safety

Gold uses the same shared retry policy: `retry_max_attempts` defaults to `3`, accepts only `1..3`, and counts the initial attempt. Candidate publication retries must preserve run/table/batch identity and reconcile uncertain commits before another attempt.

| ID | Task | Output | Acceptance criteria | Dependency | Status |
|---|---|---|---|---|---|
| 6.6.1 | Create candidate schema identity | Gold staging/version manager integration | Published Gold is untouched during build; `gold_version` and identifiers are validated and safe for SQL | W3/6.1 | Done |
| 6.6.2 | Remove destructive reset from canonical flow | Refactored loader | No canonical execution drops published Gold before candidate build and validation complete | 6.6.1 | Done |
| 6.6.3 | Publish version and current pointer | `PostgresGoldPublishService` | Candidate schema is promoted and the current pointer is updated in one transaction; failure preserves the previous version | 6.5/6.6.1 | Done |
| 6.6.4 | Retry transient atomic units | Shared retry policy integration | Only classified transient database errors retry; retries retain run/table/batch identity | W5/6.3.6 | Done |
| 6.6.5 | Reconcile unknown commits | Shared reconciliation integration | Client timeout after commit is checked against staging/audit/unique key before retry | 6.6.4 | Done |
| 6.6.6 | Clean failed/abandoned candidates | Lifecycle and retention behavior | Failed builds are not published; candidates are retained 24-72 hours and cleanup is idempotent | 6.6.3 | Done |
| 6.6.7 | Record publication/version audit | Gold run/table audit | Result includes candidate version, previous version, counts, validation, constraints, publish state, and failure reason | 6.6.3 | Done |

**6.6 implementation note:** `GoldPublishService` and PostgreSQL adapters provide version/pointer, retry, reconciliation, concurrency, retention, and audit contracts. Legacy destructive orchestration has been removed from the canonical path; remaining work is tracked by individual task status.

### 6.7 Gold regression tests

| ID | Task | Output | Acceptance criteria | Dependency | Status |
|---|---|---|---|---|---|
| 6.7.1 | Preserve builder regression tests | Existing `tests/test_sales_gold.py` coverage | Date, dimensions, fact grain, measures, nullable salesperson, and output columns remain covered | 6.2/6.3 | Done |
| 6.7.2 | Add job contract tests | `tests/test_gold_job.py` | Injected fakes prove table order, snapshot propagation, standard results, and no live DB requirement | 6.1 | Done |
| 6.7.3 | Add fact strategy tests | `tests/test_gold_fact_staging.py` | Stable ordering, `(lower_bound, upper_bound]` bounds, batch identity, many-to-one join, and no offset checkpoint | 6.3 | Done |
| 6.7.4 | Add integrity/constraint tests | `tests/test_gold_validation.py` | Duplicate keys, null required keys, orphan references, type mismatch, and measure violations fail closed | 6.4/6.5 | Done |
| 6.7.5 | Add publish preservation tests | `tests/test_gold_publish.py` | Build, validation, constraint, or publish failure leaves the old Gold version unchanged | 6.6 | Done |
| 6.7.6 | Add retry/reconciliation tests | `tests/test_gold_retry.py` | Transient retry reuses identity; deterministic errors do not retry; unknown commit does not duplicate fact rows | 6.6.4/6.6.5 | Done |
| 6.7.7 | Add rerun/idempotency tests | Gold integration or fake acceptance tests | Same Silver snapshot rerun produces deterministic dimensions/facts and one logical row per detail key | 6.3/6.6 | Done |
| 6.7.8 | Add KPI gate tests | KPI validator tests | KPI mismatch blocks publish; an in-tolerance candidate can publish | 6.4.6/6.6.3 | Done |
| 6.7.9 | Run Gold regression | Focused and repository suites | Focused Gold tests pass; unit tests are independent of external services; integration prerequisites are recorded | All prior tasks | Done |

## 7. Verifiable acceptance criteria

### 7.1 Job and compatibility

- [x] An injectable Gold job accepts settings and its database/build/validation/publish dependencies.
- [x] The job runs only after `SilverSnapshotGate`; all six Silver targets are published and share one `source_snapshot_id`.
- [x] Gold results distinguish `pipeline_snapshot_id`, `source_snapshot_id`, `gold_run_id`, and `gold_load_id`.
- [x] Every attempted table returns the shared result contract with counts, timing, identity, error, staging, and publication fields.
- [x] Existing `build_dim_*`, `build_fact_sales`, and legacy `run()` callers remain functional through delegation.
- [x] The compatibility wrapper contains no new destructive, retry, or publication logic.

### 7.2 Dimensions and fact construction

- [x] Full-read is used only for the documented small dimensions.
- [x] Dimension keys are non-null, unique, deterministic, and aligned with the target DDL.
- [x] Product builder output names and Gold schema names are reconciled and covered by a test.
- [x] `fact_sales` has exactly one row per `sales_order_detail_id`; duplicate keys fail closed.
- [x] Fact processing uses stable `sales_order_detail_id` batches with `(lower_bound, upper_bound]` bounds.
- [x] Fact processing does not use offset/page-number checkpoints.
- [x] Fact measures and data types match the approved formulas and Gold schema.
- [x] Missing required header/dimension references are reported as integrity failures, not silently dropped.

### 7.3 Pre-publish validation and constraints

- [x] Candidate schema, column names, types, nullability, `gold_version`, `source_snapshot_id`, and required metadata are validated before publication.
- [x] Duplicate PKs, required NULLs, orphan references, invalid measures, and grain violations block publication.
- [x] Non-null salesperson references must resolve; NULL salesperson is accepted only under the documented online-order rule.
- [x] KPI validation runs against the candidate schema before the current pointer changes, uses the same `source_snapshot_id`, and enforces relative 2% tolerance; a zero baseline passes only with a zero candidate.
- [x] PK/FK constraints are created and verified on staging before the candidate is eligible for publish.
- [x] Constraint creation failure leaves published Gold unchanged.

### 7.4 Atomic publish, retry, and rerun

- [x] The canonical path never drops or replaces published Gold before candidate build and validation succeed.
- [x] Gold is published through a versioned schema and current pointer; consumers use stable views or pointer objects under the `gold` namespace.
- [x] A failed build, validation, constraint operation, or publish leaves the previous Gold version unchanged.
- [x] Successful publication atomically promotes the complete validated version and updates the current pointer in one transaction.
- [x] Only classified transient database errors are retried.
- [x] Retry preserves run/table/batch identity and uses the same atomic unit.
- [x] Unknown commit outcomes are reconciled before another write attempt.
- [x] Failed or abandoned staging is marked/cleaned according to the shared lifecycle policy.
- [x] Rerunning the same Silver snapshot creates a new `gold_run_id`, remains deterministic, does not create duplicate fact rows, and may create a new Gold version.
- [x] Only one Gold run is active; concurrent runs wait or are rejected according to the lock policy.

### 7.5 Observability and pipeline gate

- [x] Audit records include snapshot, run/table/batch identity, source/target, counts, attempts, validation, constraint, publish, and error outcomes.
- [x] Structured logs include applicable stage/table/batch/status fields without secrets or full raw payloads.
- [x] Gold failure or KPI validation failure prevents downstream success, returns `failed_stage="gold"`, and does not roll back Bronze or Silver.
- [x] A machine-readable summary distinguishes dimension counts, fact counts, orphan/duplicate counts, KPI status, and publication state.

## 8. Test matrix and commands

### 8.1 Required scenarios

| Scenario | Test type | Expected result |
|---|---|---|
| Valid dimension builder | Unit | Existing dimension output and key behavior remain correct |
| Valid fact builder | Unit | Measures, date key, types, and line-item grain are correct |
| Product schema mapping | Unit/integration | Builder columns match Gold DDL exactly |
| Empty/invalid date range | Unit | Clear failed result; no partial publication |
| Duplicate fact key | Unit/integration | Validation fails; no silent dedup or publish |
| Missing required reference | Unit/integration | Orphan report fails before FK/publish |
| Nullable salesperson | Unit/integration | NULL is allowed only under explicit policy; non-null orphan fails |
| Large fact input | Unit/integration | Stable-key batches avoid full fact read and do not use offsets |
| Stable fact ordering | Unit | Same snapshot produces same key bounds and batch identity |
| Transient fact write failure | Unit | Same batch retries with bounded attempts and no duplicate |
| Unknown commit | Integration | Reconciliation detects committed batch before retry |
| Constraint failure | Integration | Old Gold remains unchanged |
| KPI mismatch | Unit/integration | Candidate is rejected before publish |
| Build/publish failure | Unit/integration | Existing Gold remains unchanged |
| Successful atomic publish | Integration | Complete constrained candidate becomes the current Gold version through the pointer |
| Same snapshot rerun | Integration | Deterministic output and one fact row per detail key |
| Secret/redaction behavior | Unit | Credentials and raw payload do not appear in logs/reports |

### 8.2 Suggested commands

Run from the repository root:

```powershell
python -m pytest tests/test_sales_gold.py -q
python -m pytest tests/test_sales_gold.py tests/test_ingestion_models.py tests/test_retry_policy.py tests/test_staging_manager.py tests/test_checkpoint_manager.py tests/test_audit_service.py -q
python -m pytest -m "not integration" -q
python -m pytest -q
```

Suggested new focused test files:

```text
tests/test_gold_job.py
tests/test_gold_fact_staging.py
tests/test_gold_validation.py
tests/test_gold_publish.py
tests/test_gold_retry.py
```

Integration tests must be marked with the repository integration marker and must distinguish unavailable PostgreSQL/SQL Server prerequisites from application failures.

## 9. Definition of Done for Phase 4D

Phase 4D is complete only when all applicable items below have implementation and test evidence:

- [x] Gold is callable through an injectable job/service and the legacy entrypoint delegates to it.
- [x] Small dimensions use documented full-read behavior with explicit schema/key validation.
- [x] `fact_sales` uses stable-key batches and preserves `sales_order_detail_id` grain.
- [x] Fact duplicate, orphan, null, type, measure, and KPI violations fail closed.
- [x] Gold candidate tables are built in run-specific staging; published Gold is untouched during the build.
- [x] PK/FK constraints and data types are verified on staging before publication.
- [x] Gold publication is atomic and preserves the previous version after any failure.
- [x] Retry is limited to transient errors, preserves identity, and reconciles unknown commits.
- [x] Same-snapshot rerun is deterministic and idempotent.
- [x] Failed/abandoned staging cleanup is tested.
- [x] Focused Gold unit tests pass without external services.
- [x] Integration tests pass when prerequisites are available, or blockers are clearly recorded.
- [x] Structured audit/log/report output is complete and redacted.
- [x] README/runbook and the Phase 4 master checklist match the implemented runtime.

Production DoD must not be inferred from the existing pandas builder tests alone. Staging transaction, constraint, integrity, KPI gate, retry/reconciliation, rerun, and publish-preservation evidence are required.

## 10. Risks and mitigations

| Risk | Impact | Mitigation |
|---|---|---|
| Published Gold is dropped before build | Dashboard availability loss | Use a versioned candidate schema and update the current pointer only after all validation |
| Fact full-read causes memory pressure | Runtime failure or resource exhaustion | Use stable-key batches by `sales_order_detail_id`; do not use offsets |
| Duplicate detail key is silently removed | Incorrect sales totals and grain | Treat duplicate `sales_order_detail_id` as a failed validation |
| Orphan key is discovered only during FK creation | Late failure after destructive changes | Validate references before creating constraints or publishing |
| `to_sql(replace)` removes constraints | Schema contract drift | Explicit staging DDL and constraint inspection |
| Unknown commit is retried blindly | Duplicate fact rows | Reconcile staging/audit/unique-key evidence before retry |
| Dimension builder/schema mismatch | Runtime load or downstream query failure | Resolve `class`/`style` naming contract with a focused schema test |
| KPI validation runs after publish | Invalid metrics become visible | Compare candidate staging with independent Silver baseline before publish |
| Same snapshot uses mixed source versions | Inconsistent dimensions/facts | Require and audit one Silver snapshot/load identity |
| Legacy API breaks | Existing tests/scripts fail | Delegating wrapper and compatibility regression tests |

## 11. Evidence log

Update this table after each task. A task may be marked `Done` only when implementation, focused test, and relevant regression evidence are recorded.

| Date | Task | Files/symbols | Validation command | Result | Status |
|---|---|---|---|---|---|
| 2026-09-04 | Create Phase 4D Gold implementation scope | `docs/ToDoCheckList/Phase_4_Review&Enhance_Code/phase4d_goldlayer_execution.md` | Markdown review | Document created; implementation not started | Done |
| 2026-09-04 | Baseline current Gold behavior | `sales_gold_load.py`, `03_create_gold_schema.sql`, `validate_sales_kpis.py`, `tests/test_sales_gold.py` | Source/test inspection | Full Silver reads, destructive reset, direct replace, post-write constraints, and standalone KPI validation confirmed | Done |
| 2026-09-06 | Package Gold as injectable job/service (6.1.1-6.1.3) | `sales_gold_job.py`, `pipeline_runner.py`, `app.py`, `tests/test_gold_job.py` | `python -m pytest tests/test_gold_job.py tests/test_pipeline_runner.py tests/test_architecture_contract.py tests/test_app_bronze_to_silver.py tests/test_sales_gold.py -q` | 31 passed; specs, Silver gate, injectable job, identity propagation and Gold failure stage implemented. Legacy `run()` delegation remains blocked until staging/publish dependencies exist. | Partial |
| 2026-09-06 | Implement dimension full-read strategy (6.2.1-6.2.5) | `sales_gold_load.py`, `sales_gold_job.py`, `tests/test_sales_gold.py`, `tests/test_gold_job.py` | `python -m pytest tests/test_sales_gold.py tests/test_gold_job.py tests/test_pipeline_runner.py -q` | 28 passed; full-read dimension contract, deterministic selection, type/key validation, product mapping, and invalid-date failure implemented. | Done |
| 2026-09-06 | Implement stable-key fact batches (6.3.1-6.3.6) | `sales_gold_load.py`, `sales_gold_job.py`, `tests/test_gold_fact_staging.py`, `tests/test_gold_job.py` | `python -m pytest tests/test_gold_fact_staging.py tests/test_sales_gold.py tests/test_gold_job.py tests/test_pipeline_runner.py -q` | 34 passed; stable `(lower_bound, upper_bound]` query contract, deterministic batch identity/content hash, grain/header/measure validation, and commit-before-checkpoint ordering implemented. Unknown-commit reconciliation remains with 6.6.5. | Partial |
| 2026-09-06 | Implement pre-publish integrity validation (6.4.1-6.4.6) | `sales_gold_job.py`, `tests/test_gold_validation.py`, `tests/test_gold_job.py` | `python -m pytest tests/test_gold_validation.py tests/test_gold_fact_staging.py tests/test_sales_gold.py tests/test_gold_job.py tests/test_pipeline_runner.py -q` | 38 passed; default validator now checks candidate schema/types, keys/grain/nulls, orphans, measures, and configurable 2% KPI baseline before prepare/publish. | Done |
| 2026-09-06 | Implement staging constraint contract (6.5.1-6.5.5) | `sales_gold_job.py`, `tests/test_gold_constraints.py`, `src/shared/ingestion/staging_manager.py` | `python -m pytest tests/test_gold_constraints.py tests/test_gold_validation.py tests/test_gold_fact_staging.py tests/test_sales_gold.py tests/test_gold_job.py tests/test_pipeline_runner.py -q` | 41 passed; explicit candidate CREATE TABLE/PK/FK DDL, Gold metadata, nullable-FK contract and fail-closed verification implemented. PostgreSQL execution/catalog inspection remains pending in 6.6 adapter work. | Partial |
| 2026-09-06 | Implement atomic publication and retry safety contract (6.6.1-6.6.7) | `sales_gold_job.py`, `tests/test_gold_publish.py` | `python -m pytest tests/test_gold_publish.py tests/test_gold_constraints.py tests/test_gold_validation.py tests/test_gold_fact_staging.py tests/test_sales_gold.py tests/test_gold_job.py tests/test_pipeline_runner.py -q` | 48 passed; versioned candidate/pointer, preservation, active-run lock, transient retry, unknown-commit reconciliation, failed-candidate retention and idempotent cleanup implemented. Legacy destructive wrapper, PostgreSQL adapter and publication audit remain open. | Partial |
| 2026-09-06 | Complete PostgreSQL staging constraints (6.5.1-6.5.4) | `postgres_gold_constraint_service.py`, `tests/test_postgres_gold_constraints.py`, `tests/test_postgres_gold_constraints_integration.py` | `python -m pytest tests/test_postgres_gold_constraints.py tests/test_postgres_gold_constraints_integration.py -q` | 22 passed; live PostgreSQL candidate schema creation, data load, PK/FK creation, catalog inspection and cleanup verified. | Done |
| 2026-09-06 | Complete database candidate schema identity (6.6.1) | `postgres_gold_constraint_service.py`, `tests/test_postgres_gold_candidate_integration.py` | `python -m pytest tests/test_postgres_gold_candidate_integration.py tests/test_postgres_gold_constraints_integration.py -q` | 16 passed; advisory-locked `gold_vNNN` allocation, real candidate schema/table creation, rollback on candidate failure, and published `gold` namespace preservation verified. | Done |
| 2026-09-06 | Complete PostgreSQL atomic publish/current pointer (6.6.3) | `postgres_gold_publish_service.py`, `tests/test_postgres_gold_publish_integration.py` | `python -m pytest tests/test_postgres_gold_publish_integration.py tests/test_postgres_gold_candidate_integration.py tests/test_postgres_gold_constraints_integration.py -q` | 5 passed; candidate completeness check, atomic pointer update, previous-version tracking, transaction rollback, and published Gold preservation verified on live PostgreSQL. | Done |
| 2026-09-06 | Complete database unknown-commit reconciliation (6.6.5) | `postgres_gold_reconciliation_service.py`, `tests/test_postgres_gold_reconciliation_integration.py` | `python -m pytest tests/test_postgres_gold_reconciliation_integration.py tests/test_postgres_gold_publish_integration.py tests/test_postgres_gold_candidate_integration.py tests/test_postgres_gold_constraints_integration.py tests/test_gold_publish.py -q` | 15 passed; matching registry hash skips, missing evidence retries, conflicting hash fails closed, candidate unique-key evidence skips, and DB evidence is persisted under `gold.gold_batch_registry`. | Done |
| 2026-09-06 | Complete staging constraint failure handling (6.5.5) | `postgres_gold_constraint_service.py`, `tests/test_postgres_gold_constraints_integration.py` | `python -m pytest tests/test_postgres_gold_constraints_integration.py -q` | 2 passed; live duplicate-PK failure rolls back candidate schema, preserves current pointer/published version, and leaves no failed candidate schema behind. | Done |
| 2026-09-06 | Complete fact batch retry identity (6.3.3) | `postgres_gold_reconciliation_service.py`, `tests/test_postgres_gold_reconciliation_integration.py` | `python -m pytest tests/test_postgres_gold_reconciliation_integration.py tests/test_postgres_gold_publish_integration.py tests/test_gold_publish.py tests/test_gold_fact_staging.py tests/test_gold_job.py -q` | 25 passed; retry wrapper preserves `gold_load_id`, `batch_id`, bounds and content hash, records after commit, and reconciles unknown commit before retry. | Done |
| 2026-09-06 | Complete publication/version audit (6.6.7) | `postgres_gold_publish_service.py`, `tests/test_postgres_gold_publish_integration.py`, `sales_gold_job.py` | `python -m pytest tests/test_postgres_gold_publish_integration.py tests/test_postgres_gold_reconciliation_integration.py tests/test_postgres_gold_candidate_integration.py tests/test_postgres_gold_constraints_integration.py tests/test_gold_publish.py tests/test_gold_job.py tests/test_gold_validation.py -q` | 24 passed; transactional audit records identity, versions, counts, validation, constraints, KPI comparisons, publish state and previous version; publisher failures rollback pointer and audit together. | Done |
| 2026-09-06 | Preserve legacy builders and delegate `run()` (6.1.4) | `sales_gold_load.py:run()` | Superseded by completed delegation evidence below | Historical blocker; replaced by delegate-only implementation after 6.3-6.6 dependencies completed. | Superseded |
| 2026-09-06 | Complete legacy Gold delegation and destructive-flow removal (6.1.4, 6.6.2) | `sales_gold_load.py`, `tests/test_gold_job.py` | `python -m pytest tests/test_gold_job.py tests/test_sales_gold.py tests/test_gold_fact_staging.py tests/test_gold_validation.py tests/test_gold_constraints.py tests/test_gold_publish.py -q` | Delegate-only `run()` verified; gated snapshot metadata required; canonical path no longer resets Gold or uses direct `to_sql(..., if_exists="replace")`. | Done |
| 2026-09-06 | Implement Gold regression modules (6.7.1-6.7.8) | `tests/test_gold_retry.py`, `tests/test_gold_rerun.py`, `tests/test_gold_kpi.py`, existing Gold test modules | `python -m pytest tests/test_gold_retry.py tests/test_gold_rerun.py tests/test_gold_kpi.py tests/test_gold_publish.py tests/test_gold_validation.py tests/test_gold_fact_staging.py tests/test_gold_job.py tests/test_sales_gold.py -q` | 49 passed; builder/job/fact/integrity/constraint/publish/retry/rerun/KPI regression coverage verified. | Done |
| 2026-09-06 | Run Gold and repository regression (6.7.9) | Full `tests/` suite and Phase 4A contract | `python -m pytest -q` | 183 passed; full repository regression passed after restoring the Phase 4A `App` constructor signature. | Done |
| TBD | Implement dimension and fact staging | TBD | TBD | TBD | Not started |
| TBD | Implement pre-publish integrity/KPI validation | TBD | TBD | TBD | Not started |
| TBD | Implement staging constraints and atomic publish | TBD | TBD | TBD | Not started |
| TBD | Implement retry/reconciliation and rerun safety | TBD | TBD | TBD | Not started |
| TBD | Run focused Gold and repository regression tests | TBD | TBD | TBD | Not started |

## 12. Related documents

- `docs/internal/phase4_review_enhance_spec.md`
- `docs/internal/phase4_enhancement_execution_plan_spec.md`
- `docs/ToDoCheckList/Phase_4_Review&Enhance_Code/phase4a_foundation_execution.md`
- `docs/ToDoCheckList/Phase_4_Review&Enhance_Code/phase4b_bronzelayer_execution.md`
- `docs/ToDoCheckList/Phase_4_Review&Enhance_Code/phase4c_silverlayer_execution.md`
- `src/shared/ingestion/ingestion_models.py`
- `src/shared/ingestion/staging_manager.py`
- `src/shared/ingestion/postgres_publish_service.py`
- `src/shared/ingestion/retry_policy.py`
- `src/shared/ingestion/checkpoint_manager.py`
- `src/shared/ingestion/audit_service.py`
- `scripts/warehouse/postgres/gold/sales_gold_load.py`
- `scripts/warehouse/postgres/gold/validate_sales_kpis.py`
- `scripts/warehouse/postgres/schema/03_create_gold_schema.sql`
- `tests/test_sales_gold.py`
