from typing import ClassVar, Dict, Optional

from dlt.common.arithmetics import DEFAULT_NUMERIC_PRECISION, DEFAULT_NUMERIC_SCALE
from dlt.common.destination import DestinationCapabilitiesContext
from dlt.common.data_types import TDataType
from dlt.common.schema import TColumnSchema, TColumnHint, Schema

from dlt.destinations.insert_job_client import InsertValuesJobClient

from dlt.destinations.sqlite import capabilities
from dlt.destinations.sqlite.sql_client import SqliteSqlClient
from dlt.destinations.sqlite.configuration import SqliteClientConfiguration


SCT_TO_PGT: Dict[TDataType, str] = {
    "complex": "JSON",
    "text": "TEXT",
    "double": "DOUBLE",
    "bool": "BOOLEAN",
    "date": "DATE",
    "timestamp": "DATETIME",
    "bigint": "INTEGER",
    "binary": "BLOB",
    "decimal": f"DECIMAL({DEFAULT_NUMERIC_PRECISION},{DEFAULT_NUMERIC_SCALE})"
}

PGT_TO_SCT: Dict[str, TDataType] = {
    "TEXT": "text",
    "JSON": "complex",
    "DOUBLE": "double",
    "BOOLEAN": "bool",
    "DATE": "date",
    "DATETIME": "timestamp",
    "INTEGER": "bigint",
    "BLOB": "binary",
    "DECIMAL": "decimal"
}

HINT_TO_POSTGRES_ATTR: Dict[TColumnHint, str] = {
    "unique": "UNIQUE"
}


class DuckDbClient(InsertValuesJobClient):

    capabilities: ClassVar[DestinationCapabilitiesContext] = capabilities()

    def __init__(self, schema: Schema, config: SqliteClientConfiguration) -> None:
        sql_client = SqliteSqlClient(
            self.make_dataset_name(schema, config.dataset_name, config.default_schema_name),
            config.credentials
        )
        super().__init__(schema, config, sql_client)
        self.config: SqliteClientConfiguration = config
        self.sql_client: SqliteSqlClient = sql_client  # type: ignore
        self.active_hints = HINT_TO_POSTGRES_ATTR if self.config.create_indexes else {}

    def _get_column_def_sql(self, c: TColumnSchema) -> str:
        hints_str = " ".join(self.active_hints.get(h, "") for h in self.active_hints.keys() if c.get(h, False) is True)
        column_name = self.capabilities.escape_identifier(c["name"])
        return f"{column_name} {self._to_db_type(c['data_type'])} {hints_str} {self._gen_not_null(c['nullable'])}"

    @staticmethod
    def _to_db_type(sc_t: TDataType) -> str:
        if sc_t == "wei":
            return "DECIMAL(38,0)"
        return SCT_TO_PGT[sc_t]

    @staticmethod
    def _from_db_type(pq_t: str, precision: Optional[int], scale: Optional[int]) -> TDataType:
        # duckdb provides the types with scale and precision
        pq_t = pq_t.split("(")[0].upper()
        if pq_t == "DECIMAL":
            if precision == 38 and scale == 0:
                return "wei"
        return PGT_TO_SCT[pq_t]
