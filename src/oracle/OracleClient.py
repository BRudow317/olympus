"""OracleClient.py
Example Oracle .env configs:

# Dev01
AUTO_DEV01_USER=INPRS_AUTOMATION
AUTO_DEV01_PASS=examplepassword
AUTO_DEV01_HOST=devoraoel24.cloud.inprs.in.gov
AUTO_DEV01_PORT=1536
AUTO_DEV01_SID=ERMPASD1"""

from __future__ import annotations

import logging
import os
from collections.abc import Iterator, Iterable, Callable
from pathlib import Path
from typing import Any

import oracledb
from oracledb import (
    LOB, Connection, Cursor, DataFrame, DbObjectType, DbType,
    Queue, AsyncQueue, DbObject, MessageProperties,
)

logger: logging.Logger = logging.getLogger(__name__)


class OracleClient:
    _oracle_user: str
    _oracle_pass: str
    _oracle_host: str
    _oracle_port: int
    _oracle_service: str
    _current_connection: Connection | None

    __slots__ = (
        "_oracle_user",
        "_oracle_pass",
        "_oracle_host",
        "_oracle_port",
        "_oracle_service",
        "_current_connection",
    )

    def __init__(
        self,
        oracle_user: str = '',
        oracle_pass: str = '',
        oracle_host: str = '',
        oracle_port: int | str = 1521,
        oracle_service: str = '',
    ) -> None:
        """Initializes raw parameter strings, enforces port scaling, and tracks connection states."""
        self._oracle_user = oracle_user
        self._oracle_pass = oracle_pass
        self._oracle_host = oracle_host
        self._oracle_port = int(oracle_port)
        self._oracle_service = oracle_service
        self._current_connection = None
        
        if not self._oracle_pass:
            raise RuntimeError(f"No Password detected:\n{self.__repr__()}")

    def __repr__(self) -> str:
        return (
            f"{type(self).__name__}("
            f"user={self._oracle_user!r}, "
            f"host={self._oracle_host!r}, "
            f"port={self._oracle_port!r}, "
            f"service={self._oracle_service!r})"
        )

    def __call__(self) -> Connection:
        return self.connect()

    def __del__(self) -> None:
        """Safely tears down active database instances on system cleanup boundaries."""
        try:
            self.close()
        except Exception as e:
            logger.warning(
                f'Error during OracleClient cleanup: {self.__repr__()}\nError: {e}'
            )

    @classmethod
    def client_constructor(cls, environment: str) -> OracleClient:
        """Constructs an instance using automated OS environment parameter lookups."""
        environment = environment.upper()
        
        user: str | None = os.getenv(f"AUTO_{environment}_USER") or None
        pwd: str | None = os.getenv( f"AUTO_{environment}_PASS") or None
        host: str | None = os.getenv(f"AUTO_{environment}_HOST") or None
        port: str | int = os.getenv( f"AUTO_{environment}_PORT") or 1521
        svc: str | None = os.getenv( f"AUTO_{environment}_SERVICE") or os.getenv(f"AUTO_{environment}_SID") or None

        if (not user or not pwd or not host or not port or not svc):
            raise ValueError(f"Missing Oracle env vars for '{environment}'\n"
                             f"user: {user}\n"
                             f"host: {host}\n"
                             f"port: {port}\n"
                             f"svc: {svc}"
                             )

        return cls(
            oracle_user=user,
            oracle_pass=pwd,
            oracle_host=host,
            oracle_port=int(port),
            oracle_service=svc,
        )

    def _new_connect(self) -> None:
        """Invokes the oracledb thick/thin connection factory and sets strict transaction bounds."""
        self._current_connection = oracledb.connect(
            user=self._oracle_user,
            password=self._oracle_pass,
            host=self._oracle_host,
            port=self._oracle_port,
            service_name=self._oracle_service,
        )
        self._current_connection.autocommit = False
        logger.debug("New Oracle Connection Established")

    def connect(self) -> oracledb.Connection:
        """Evaluates pool health states dynamically, spawning fresh targets if drops are caught."""
        try:
            if self._current_connection is not None and self._current_connection.is_healthy():
                return self._current_connection
            else:
                self._new_connect()
                
                if self._current_connection is None:
                    raise RuntimeError(f"Failed to establish Oracle connection: {self.__repr__()}")
                return self._current_connection
        except Exception as e:
            logger.error("\n\n\n#########################################\n")
            logger.error(f"!!!! Failed to Connect to Database: {self.__repr__()}\nError: {e}\n\n")
            raise e

    def cursor(self, scrollable: bool = False) -> Cursor:
        return Cursor(self.connect(), scrollable)

    def is_healthy(self) -> bool:
        if not self._current_connection:
            return False
        else:
            return self._current_connection.is_healthy()

    def close(self) -> None:
        if self._current_connection:
            self._current_connection.close()

    def commit(self) -> None:
        self.connect().commit()

    def rollback(self) -> None:
        self.connect().rollback()


    def execute_many(
        self, 
        sql: str, 
        records: Iterable[dict[str, Any]], 
        input_sizes: dict[str, Any] | None = None, 
        *, 
        batcherrors: bool = True, 
        batch_size: int = 1000,
    ) -> list[Any]:
        """Pushes structured datasets in chunks via execute_many, binding input properties."""
        all_errors: list[Any] = []
        batch: list[dict[str, Any]] = []
        
        with self.connect().cursor() as cursor:
            if input_sizes:
                sized = {k: v for k, v in input_sizes.items() if v is not None}
                if sized:
                    cursor.setinputsizes(**sized)
                    
            def _flush() -> None:
                if not batch:
                    return
                cursor.executemany(sql, batch, batcherrors=batcherrors)
                if batcherrors:
                    all_errors.extend(cursor.getbatcherrors())
                batch.clear()
                
            for record in records:
                batch.append(record)
                if len(batch) >= batch_size:
                    _flush()
                    
            _flush()
            
        return all_errors

    def json_factory(self, cursor: Cursor) -> Cursor:
        """Injects custom rows mapping interceptors to automatically stream LOB values cleanly."""
        name_list = []
        lob_indexes = []
        
        for index, d in enumerate(cursor.description or []):
            name_list.append(d.name)
            if d.type_code in (oracledb.DB_TYPE_CLOB, oracledb.DB_TYPE_BLOB):
                lob_indexes.append(index)
                
        def process_row(*row):
            row_list = list(row)
            for idx in lob_indexes:
                lob_val = row_list[idx]
                if lob_val is not None:
                    row_list[idx] = lob_val.read()
            return dict(zip(name_list, row_list))
            
        cursor.rowfactory = process_row
        return cursor


    def lazy_query(
        self,
        statement: str,
        binds: dict[str, Any] | None = None,
        array_size: int | None = None,
        batch_size: int = 10_000,
    ) -> Iterator[dict[str, Any]]:
        """Generator lazily streaming table data blocks from JSON-factored cursor arrays."""
        binds_ = binds or {}
        cursor = self.cursor()
        try:
            cursor.arraysize = array_size or batch_size
            cursor.execute(statement, binds_)
            cursor = self.json_factory(cursor)
            for row in cursor:
                yield row
        finally:
            cursor.close()

    def query(
        self,
        statement: str,
        binds: dict[str, Any] | None = None,
        array_size: int | None = None,
        batch_size: int = 10_000,
    ) -> list[dict[str, Any]]:
        """Eagerly extracts complete query arrays straight into internal memory caches."""
        binds_ = binds or {}
        cursor = self.cursor()
        try:
            cursor.arraysize = array_size or batch_size
            cursor.execute(statement, binds_)
            cursor = self.json_factory(cursor)
            return cursor.fetchall()
        finally:
            cursor.close()

    def record_count(self, table: str) -> int:
        """Return the row count of a table via SELECT COUNT(*)."""
        rows = self.query(f"SELECT COUNT(*) AS CNT FROM {table}")
        return int(rows[0]["CNT"])

    def execute(self, statement: str) -> None:
        """Execute a DDL statement (CREATE, ALTER, DROP)."""
        with self.connect().cursor() as cursor:
            try:
                cursor.execute(statement)
            except oracledb.Error as e:
                logger.error("Oracle DDL failed: %s | %s", statement, e)
                raise

    def fetch_df_all(
        self,
        statement: str,
        parameters: list | tuple | dict | None = None,
        arraysize: int | None = None,
        *,
        fetch_decimals: bool | None = None,
        requested_schema: Any | None = None,
    ) -> DataFrame:
        return self.connect().fetch_df_all(
            statement,
            parameters,
            arraysize=arraysize,
            fetch_decimals=fetch_decimals,
            requested_schema=requested_schema,
        )

    def fetch_df_batches(
        self,
        statement: str,
        parameters: list | tuple | dict | None = None,
        size: int | None = None,
        *,
        fetch_decimals: bool | None = None,
        requested_schema: Any | None = None,
    ) -> Iterator[DataFrame]:
        return self.connect().fetch_df_batches(
            statement,
            parameters,
            size=size,
            fetch_decimals=fetch_decimals,
            requested_schema=requested_schema,
        )

    def direct_path_load(
        self,
        schema_name: str,
        table_name: str,
        column_names: list[str],
        data: Any,
        *,
        batch_size: int = 2**32 - 1,
    ) -> None:
        self.connect().direct_path_load(
            schema_name,
            table_name,
            column_names,
            data,
            batch_size=batch_size,
        )


    def plus_query(self, sql: str) -> tuple[int, str | None, str | None]:
        import sys
        import subprocess
        cmd = ["sqlplus", "-s", self.con_str]
        cmpl_prc = subprocess.run(
            cmd,
            input=sql,
            capture_output=True,
            check=False,
            text=True,
        )
        return (cmpl_prc.returncode, cmpl_prc.stdout, cmpl_prc.stderr)

    def script_runner(self, path: str | Path):
        sql_path = str(path)
        with open(sql_path, 'r', encoding='utf-8') as file:
            statement=file.read()
            self.execute(statement)
            self.commit()
            logger.info(f"OracleClient.script_runner('{sql_path}') => Completed Successfully")

    def all_schemas(self) -> list[Any]:
        sql = """SELECT DISTINCT OWNER FROM ALL_TABLES"""
        return self.query(sql)

    def all_tables(self, schema: str) -> list[dict[str, str]]:
        sql = """
        SELECT DISTINCT table_name
        FROM all_tables
        WHERE upper(owner) = upper(:schema)
        """
        binds = {"schema": schema}
        return self.query(sql, binds)

    def all_tab_columns(self, schema: str, table: str) -> list[dict[str, Any]]:
        sql = """
        SELECT
            column_name,
            column_id,
            data_type,
            data_length,
            char_length,
            char_used,
            data_precision,
            data_scale,
            nullable,
            data_default,
            default_length
        FROM all_tab_columns
        WHERE upper(owner) = upper(:schema)
        AND upper(table_name) = upper(:table_name)
        ORDER BY column_id
        """
        binds = {"schema": schema.upper(), "table_name": table.upper()}
        return self.query(sql, binds)

    def all_constraints(
        self,
        schema: str = '',
        table_name: str = '',
        constraint_type: str = '*',
        column_name: str | None = None
    ) -> list[dict[str, str]]:
        binds = {}
        sql = """
        SELECT
            con.owner,
            con.table_name,
            col.column_name,
            con.constraint_name,
            con.constraint_type,
            CASE con.constraint_type
                WHEN 'C' THEN 'CHECK / NOT NULL'
                WHEN 'P' THEN 'PRIMARY KEY'
                WHEN 'U' THEN 'UNIQUE'
                WHEN 'R' THEN 'FOREIGN KEY'
                WHEN 'V' THEN 'VIEW CHECK OPTION'
                WHEN 'O' THEN 'VIEW READ ONLY'
                WHEN 'F' THEN 'REF COLUMN'
                WHEN 'H' THEN 'HASH EXPRESSION'
                WHEN 'S' THEN 'SUPPLEMENTAL LOGGING'
                ELSE 'UNKNOWN (' || con.constraint_type || ')'
            END AS constraint_type_desc,
            con.r_owner,
            ac_cols_ref.table_name AS r_table,
            ac_cols_ref.column_name AS r_column,
            con.r_constraint_name,
            con.delete_rule,
            con.status,
            con.deferrable,
            con.deferred,
            con.validated,
            con.generated,
            con.search_condition,
            con.search_condition_vc,
            con.bad,
            con.rely,
            con.last_change,
            con.index_owner,
            con.index_name,
            con.invalid,
            con.view_related,
            con.origin_con_id
        FROM all_constraints con
        JOIN all_cons_columns col
            ON con.constraint_name = col.constraint_name
            AND con.owner = col.owner
        LEFT JOIN all_cons_columns ac_cols_ref
            ON con.r_constraint_name = ac_cols_ref.constraint_name
            AND con.r_owner = ac_cols_ref.owner
            AND col.position = ac_cols_ref.position
        WHERE 1=1
        """.strip()
        
        if schema != '*':
            sql += " AND upper(con.owner) = upper(:schema)"
            binds["schema"] = schema.upper()
        if table_name != '*':
            sql += " AND upper(con.table_name) = upper(:table_name)"
            binds["table_name"] = table_name.upper()
        if constraint_type != '*':
            sql += " AND upper(con.constraint_type) = upper(:constraint_type)"
            binds["constraint_type"] = constraint_type.upper()
        if column_name:
            sql += " AND upper(col.column_name) = upper(:column_name)"
            binds["column_name"] = column_name
            
        return self.query(sql, binds)


    def get_composite_keys(self, schema: str, table_name: str) -> list[dict[str, Any]]:
        """Aggregates multikey composite relationships into unified catalog strings using LISTAGG."""
        sql = """
        SELECT
            con.owner,
            con.table_name,
            con.constraint_name,
            con.constraint_type,
            -- Merges composite columns into a single string
            LISTAGG(col.column_name, ' ') WITHIN GROUP (ORDER BY col.position) AS column_names,
            -- Identifies if it is composite right in the dataset
            CASE
                WHEN COUNT(col.column_name) OVER(PARTITION BY con.owner, con.constraint_name) > 1 THEN 'YES'
                ELSE 'NO'
            END AS is_composite,
            con.r_owner,
            con.r_constraint_name
        FROM all_constraints con
        JOIN all_cons_columns col
            ON con.constraint_name = col.constraint_name
            AND con.owner = col.owner
        WHERE upper(con.owner) = upper(:schema)
        AND upper(con.table_name) = upper(:table_name)
        GROUP BY
            con.owner,
            con.table_name,
            con.constraint_name,
            con.constraint_type,
            con.r_owner,
            con.r_constraint_name
        """.strip()
        binds = {"schema": schema.upper(), "table_name": table_name.upper()}
        return self.query(sql, binds)


    def check_object_exists(
        self,
        object_name: str,
        schema: str | None = None,
        object_type: str | None = None
    ) -> bool:
        """Verifies physical representation flags of tables or views inside the active catalog maps."""
        sql = """
        SELECT count(object_name) AS "count"
        FROM all_objects
        WHERE 1=1 AND upper(object_name) = upper(:object_name)
        """
        binds = {}
        binds['object_name'] = object_name
        group = " GROUP BY object_name"
        
        if schema:
            sql += " AND upper(owner) = upper(:schema)"
            binds['schema'] = schema
            group += ", owner"
        if object_type:
            sql += " AND upper(object_type) = upper(:object_type)"
            binds['object_type'] = object_type
            group += ", object_type"
            
        sql += group
        result = self.query(sql, binds)
        
        if len(result) > 0:
            if result[0].get('count', 0) > 0:
                return True
        return False

    # region Misc Methods
    def cancel(self) -> None:
        self.connect().cancel()

    def dbop(self, value: str) -> None:
        self.connect().dbop = value

    def action(self, value: str) -> None:
        self.connect().action = value

    def gettype(self, name: str) -> DbObjectType:
        return self.connect().gettype(name)

    def ping(self) -> None:
        self.connect().ping()

    def createlob(self, lob_type: DbType, data: str | bytes | None = None) -> LOB:
        return self.connect().createlob(lob_type, data)

    def msgproperties(
        self,
        payload: bytes | str | DbObject | None = None,
        correlation: str | None = None,
        delay: int | None = None,
        exceptionq: str | None = None,
        expiration: int | None = None,
        priority: int | None = None,
        recipients: list | None = None,
    ) -> MessageProperties:
        return self.connect().msgproperties(
            payload,
            correlation,
            delay,
            exceptionq,
            expiration,
            priority,
            recipients,
        )

    def queue(self, name: str, payload_type: DbObjectType | str | None = None) -> Queue:
        q: Queue | AsyncQueue = self.connect().queue(name, payload_type)
        if isinstance(q, Queue):
            return q
        raise TypeError(f"Expected Queue object from connection.queue(), got {type(q)}")

    def subscribe(
        self,
        namespace: int = oracledb.SUBSCR_NAMESPACE_DBCHANGE,
        protocol: int = oracledb.SUBSCR_PROTO_CALLBACK,
        callback: Callable | None = None,
        timeout: int = 0,
        operations: int = oracledb.OPCODE_ALLOPS,
        port: int = 0,
        qos: int = oracledb.SUBSCR_QOS_DEFAULT,
        ip_address: str | None = None,
        grouping_class: int = oracledb.SUBSCR_GROUPING_CLASS_NONE,
        grouping_value: int = 0,
        grouping_type: int = oracledb.SUBSCR_GROUPING_TYPE_SUMMARY,
        name: str | None = None,
        client_initiated: bool = False,
    ) -> oracledb.Subscription:
        """Establishes an active server notification instance callback rule mapping."""
        return self.connect().subscribe(
            namespace,
            protocol,
            callback,
            timeout,
            operations,
            port,
            qos,
            ip_address,
            grouping_class,
            grouping_value,
            grouping_type,
            name,
            client_initiated,
        )
    # endregion

    # region Properties
    @property
    def current_schema(self) -> str | None:
        return self.connect().current_schema or self.connect().username

    @current_schema.setter
    def current_schema(self, schema_name: str) -> None:
        self.connect().current_schema = schema_name

    @property
    def con_str(self) -> str:
        return f"{self._oracle_user}/{self._oracle_pass}@{self._oracle_host}:{str(self._oracle_port)}/{self._oracle_service}"

    @property
    def username(self) -> str:
        return self.connect().username

    @property
    def user(self) -> str:
        return self._oracle_user

    @property
    def host(self) -> str:
        return self._oracle_host

    @property
    def port(self) -> int:
        return self._oracle_port

    @property
    def service(self) -> str:
        return self._oracle_service

    @property
    def password(self) -> str:
        return '*' * len(self._oracle_pass or "")

    @property
    def auto_commit(self) -> bool:
        return self.connect().autocommit

    @auto_commit.setter
    def auto_commit(self, auto_commit: bool) -> None:
        self.connect().autocommit = auto_commit

    @property
    def max_open_cursors(self) -> int:
        return self.connect().max_open_cursors

    @property
    def session_id(self) -> int:
        return self.connect().session_id

    @property
    def is_thin(self) -> bool:
        return self.connect().thin

    @property
    def version(self) -> str:
        return self.connect().version

    @property
    def ltxid(self) -> bytes:
        return self.connect().ltxid

    @property
    def dsn(self) -> str:
        return self.connect().dsn

    @property
    def internal_name(self) -> str:
        return self.connect().internal_name

    @internal_name.setter
    def internal_name(self, value: str) -> None:
        self.connect().internal_name = value

    @property
    def inputtypehandler(self) -> Callable:
        return self.connect().inputtypehandler

    @inputtypehandler.setter
    def inputtypehandler(self, value: Callable) -> None:
        self.connect().inputtypehandler = value

    @property
    def external_name(self) -> str:
        return self.connect().external_name

    @external_name.setter
    def external_name(self, value: str) -> None:
        self.connect().external_name = value

    @property
    def edition(self) -> str:
        return self.connect().edition

    @property
    def econtext_id(self) -> str:
        return self.connect().econtext_id

    @econtext_id.setter
    def econtext_id(self, value: str) -> None:
        self.connect().econtext_id = value

    @property
    def db_name(self) -> str:
        return self.connect().db_name

    @property
    def db_domain(self) -> str:
        return self.connect().db_domain

    @property
    def client_identifier(self) -> str:
        return self.connect().client_identifier

    @client_identifier.setter
    def client_identifier(self, value: str) -> None:
        self.connect().client_identifier = value

    @property
    def call_timeout(self) -> int:
        return self.connect().call_timeout

    @call_timeout.setter
    def call_timeout(self, value: int) -> None:
        self.connect().call_timeout = value
    # endregion