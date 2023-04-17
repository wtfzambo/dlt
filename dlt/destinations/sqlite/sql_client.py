import sqlite3

from contextlib import contextmanager
from typing import Any, AnyStr, ClassVar, Iterator, Optional, Sequence
from dlt.common.destination import DestinationCapabilitiesContext

from dlt.destinations.exceptions import DatabaseTerminalException, DatabaseTransientException, DatabaseUndefinedRelation
from dlt.destinations.typing import DBApi, DBApiCursor, DBTransaction, DataFrame
from dlt.destinations.sql_client import SqlClientBase, DBApiCursorImpl, raise_database_error, raise_open_connection_error

from dlt.destinations.sqlite import capabilities
from dlt.destinations.sqlite.configuration import SqliteCredentials


class SqliteSqlClient(SqlClientBase[sqlite3.Connection], DBTransaction):

    dbapi: ClassVar[DBApi] = sqlite3
    capabilities: ClassVar[DestinationCapabilitiesContext] = capabilities()

    def __init__(self, dataset_name: str, credentials: SqliteCredentials) -> None:
        super().__init__(dataset_name)
        self._conn: sqlite3.Connection = None
        self.credentials = credentials

    @raise_open_connection_error
    def open_connection(self) -> sqlite3.Connection:
        self._conn = sqlite3.connect(database=self.credentials.database)
        self._reset_connection()
        return self._conn

    def close_connection(self) -> None:
        if self._conn:
            self._conn.close()
            self._conn = None

    @contextmanager
    @raise_database_error
    def begin_transaction(self) -> Iterator[DBTransaction]:
        try:
            self._conn.autocommit = False
            yield self
            self.commit_transaction()
        except Exception:
            # in some cases duckdb rollback the transaction automatically
            try:
                self.rollback_transaction()
            except DatabaseTransientException:
                pass
            raise

    @raise_database_error
    def commit_transaction(self) -> None:
        self._conn.commit()
        self._conn.autocommit = True

    @raise_database_error
    def rollback_transaction(self) -> None:
        self._conn.rollback()
        self._conn.autocommit = True

    @property
    def native_connection(self) -> sqlite3.Connection:
        return self._conn

    def has_dataset(self) -> bool:
        query = """
                SELECT 1
                    FROM INFORMATION_SCHEMA.SCHEMATA
                    WHERE schema_name = %s;
                """
        rows = self.execute_sql(query, self.fully_qualified_dataset_name(escape=False))
        return len(rows) > 0

    def create_dataset(self) -> None:
        self.execute_sql("CREATE SCHEMA %s" % self.fully_qualified_dataset_name())

    def drop_dataset(self) -> None:
        self.execute_sql("DROP SCHEMA %s CASCADE;" % self.fully_qualified_dataset_name())

    def execute_sql(self, sql: AnyStr, *args: Any, **kwargs: Any) -> Optional[Sequence[Sequence[Any]]]:
        with self.execute_query(sql, *args, **kwargs) as curr:
            if curr.description is None:
                return None
            else:
                f = curr.fetchall()
                return f

    @contextmanager
    @raise_database_error
    def execute_query(self, query: AnyStr, *args: Any, **kwargs: Any) -> Iterator[DBApiCursor]:
        assert isinstance(query, str)
        db_args = args if args else kwargs if kwargs else None
        if db_args:
            # TODO: must provide much better refactoring of params
            query = query.replace("%s", "?")
        try:
            self._conn.execute(query, db_args)
            yield DBApiCursorImpl(self._conn)  # type: ignore
        except sqlite3.Error as outer:
            self.close_connection()
            self.open_connection()
            raise outer

    def fully_qualified_dataset_name(self, escape: bool = True) -> str:
        return self.capabilities.escape_identifier(self.dataset_name) if escape else self.dataset_name

    def _reset_connection(self) -> None:
        self._conn.autocommit = True

    @classmethod
    def _make_database_exception(cls, ex: Exception) -> Exception:
        # if isinstance(ex, (sqlite3.CatalogException)):
        #     raise DatabaseUndefinedRelation(ex)
        # elif isinstance(ex, sqlite3.InvalidInputException):
        #     # duckdb raises TypeError on malformed query parameters
        #     return DatabaseTransientException(sqlite3.ProgrammingError(ex))
        # , sqlite3.SyntaxException, sqlite3.ParserException)
        if isinstance(ex, (sqlite3.OperationalError, sqlite3.InternalError)):
            term = cls._maybe_make_terminal_exception_from_data_error(ex)
            if term:
                return term
            else:
                return DatabaseTransientException(ex)
        elif isinstance(ex, (sqlite3.DataError, sqlite3.ProgrammingError, sqlite3.IntegrityError)):
            return DatabaseTerminalException(ex)
        elif cls.is_dbapi_exception(ex):
            return DatabaseTransientException(ex)
        else:
            return ex

    @staticmethod
    def _maybe_make_terminal_exception_from_data_error(pg_ex: sqlite3.Error) -> Optional[Exception]:
        return None

    @staticmethod
    def is_dbapi_exception(ex: Exception) -> bool:
        return isinstance(ex, sqlite3.Error)
