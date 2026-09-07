from src.shared.ingestion.ingestion_models import RejectedRecord
from src.core.settings import Settings, get_settings
from src.shared.connectors.postgres_connector import PostgreSQLConnector


class QuarantineService:
    """Database-independent rejected-record repository contract."""

    def __init__(self):
        self.records: list[RejectedRecord] = []

    def record(self, rejected_record: RejectedRecord) -> RejectedRecord:
        self.records.append(rejected_record)
        return rejected_record

    def records_for_load(self, load_id: str) -> tuple[RejectedRecord, ...]:
        return tuple(record for record in self.records if record.load_id == load_id)

    def count_for_load(self, load_id: str) -> int:
        return len(self.records_for_load(load_id))


class PostgresQuarantineService:
    """Durable rejected-record repository; raw payload is intentionally excluded."""

    def __init__(self, settings: Settings | None = None):
        self.settings = settings or get_settings()

    def record(self, rejected_record: RejectedRecord) -> RejectedRecord:
        with PostgreSQLConnector(settings=self.settings) as connection:
            connection.execute_query(
                """
                INSERT INTO bronze.rejected_records
                    (run_id, load_id, batch_id, source_table, record_key,
                     source_hash, reason, rejected_at, transform_version,
                     error_type)
                 VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (run_id, load_id, batch_id, record_key, source_hash)
                DO NOTHING
                """,
                (rejected_record.run_id, rejected_record.load_id,
                 rejected_record.batch_id, rejected_record.source_table,
                 rejected_record.record_key, rejected_record.source_hash,
                 rejected_record.reason, rejected_record.rejected_at,
                 rejected_record.transform_version, rejected_record.error_type),
            )
        return rejected_record

    def records_for_load(self, load_id: str) -> tuple[RejectedRecord, ...]:
        with PostgreSQLConnector(settings=self.settings) as connection:
            rows = connection.fetch_results(
                """
                SELECT run_id, load_id, batch_id, source_table, record_key,
                         source_hash, reason, rejected_at, transform_version,
                         error_type
                FROM bronze.rejected_records
                WHERE load_id = %s ORDER BY rejected_id
                """,
                (load_id,),
            )
        return tuple(RejectedRecord(*row) for row in rows)

    def count_for_load(self, load_id: str) -> int:
        return len(self.records_for_load(load_id))