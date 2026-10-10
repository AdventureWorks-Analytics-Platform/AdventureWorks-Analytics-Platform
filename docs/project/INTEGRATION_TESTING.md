# Integration Testing

Integration tests are split by dependency so PostgreSQL evidence does not
depend on an external SQL Server:

- `postgres_integration`: PostgreSQL-only tests.
- `sqlserver_integration`: SQL Server tests. The Bronze source-load test also
  writes to PostgreSQL and is included in this lane.
- `integration`: umbrella marker used to keep all database integration tests
  out of the unit-test lane.

## Local PostgreSQL tests

The integration compose file provisions PostgreSQL 15 on `127.0.0.1:55432`,
loads the warehouse schemas, and uses an ephemeral data directory. It does not
reuse the application's normal PostgreSQL volume or database.

From the repository root in PowerShell:

```powershell
docker compose -p adventureworks-integration -f docker-compose.integration.yml up -d --wait
$env:ENVIRONMENT = "test"
$env:POSTGRES_HOST = "127.0.0.1"
$env:POSTGRES_PORT = "55432"
$env:POSTGRES_DATABASE = "adventureworks_test"
$env:POSTGRES_USERNAME = "postgres"
$env:POSTGRES_PASSWORD = "integration_test_password"
.\.venv\Scripts\python.exe -m pytest -m "postgres_integration and not sqlserver_integration" -q
docker compose -p adventureworks-integration -f docker-compose.integration.yml down -v
```

Run the `down` command after tests, including after a failure. The CI PostgreSQL
job uses the same compose file and marker expression.

## SQL Server tests

SQL Server integration requires a reachable `AdventureWorks2012` database and
ODBC Driver 18. Local runs use `SQL_SERVER_HOST`, `SQL_SERVER_PORT`,
`SQL_SERVER_DATABASE`, `SQL_SERVER_DRIVER`, and `SQL_SERVER_AUTH_MODE`; SQL
authentication additionally requires `SQL_SERVER_USERNAME` and
`SQL_SERVER_PASSWORD`. Windows authentication remains available for compatible
local environments.

```powershell
.\.venv\Scripts\python.exe -m pytest -m sqlserver_integration -q
```

GitHub Actions runs this lane only when the repository variable
`SQL_SERVER_HOST` is set. Configure `SQL_SERVER_USERNAME` and
`SQL_SERVER_PASSWORD` as repository secrets for SQL authentication. The
configured host must contain the AdventureWorks source database; PostgreSQL for
the cross-system ingestion test is provisioned by the job.
Without that external source, the SQL Server lane is skipped rather than
blocking the independent PostgreSQL integration evidence.
