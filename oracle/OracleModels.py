from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, TYPE_CHECKING, Iterable
import re
import logging
import oracledb
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
if TYPE_CHECKING:
    from .OracleClient import OracleClient
    from .OracleJob import Job
logger = logging.getLogger(__name__)

_NULL_BYTE_RE = re.compile(r'\x00')
_COMMA_RE = re.compile(r',')
_DATE_FMT = '%Y-%m-%d'
_TIMESTAMP_FMTS = ['%Y-%m-%dT%H:%M:%S.%fZ', '%Y-%m-%dT%H:%M:%S', '%Y-%m-%d %H:%M:%S']
_TZ_OFFSET_RE = re.compile(r'[+-]\d{2}:\d{2}$')

ORACLE_MAX_VARCHAR2_CHAR = 4000
varchar2_growth_buffer = 50

_ORACLE_RESERVED = frozenset({
"ACCESS","ADD","ALL","ALTER","AND","ANY","AS","ASC","AUDIT","BETWEEN","BY","CHAR","CHECK","CLUSTER","COLUMN",
"COMMENT","COMPRESS","CONNECT","CREATE","CURRENT","DATE","DECIMAL","DEFAULT","DELETE","DESC","DISTINCT","DROP","ELSE",
"EXCLUSIVE","EXISTS","FILE","FLOAT","FOR","FROM","GRANT","GROUP","HAVING","IDENTIFIED","IMMEDIATE","IN","INCREMENT",
"INDEX","INITIAL","INSERT","INTEGER","INTERSECT","INTO","IS","LEVEL","LIKE","LOCK","LONG","MAXEXTENTS","MINUS",
"MLSLABEL","MODE","MODIFY","NOAUDIT","NOCOMPRESS","NOT","NOWAIT","NULL","NUMBER","OF","OFFLINE","ON","ONLINE",
"OPTION","OR","ORDER","PCTFREE","PRIOR","PRIVILEGES","PUBLIC","RAW","RENAME","RESOURCE","REVOKE","ROW","ROWID",
"ROWNUM","ROWS","SELECT","SESSION","SET","SHARE","SIZE","SMALLINT","START","SUCCESSFUL","SYNONYM","SYSDATE",
"TABLE","THEN","TO","TRIGGER","UID","UNION","UNIQUE","UPDATE","USER","VALIDATE","VALUES","VARCHAR","VARCHAR2",
"VIEW","WHENEVER","WHERE","WITH"})


@dataclass
class OracleTable:
    oracle_client: OracleClient
    table_name: str = ''
    schema_name: str = ''
    column_map: dict[str, OracleColumn] = field(default_factory=dict)
    _fetched_db_col: list[dict[str, Any]] | None = field(default=None, init=False)
    _insert_sql_stmt: str | None = field(default=None, init=False)
    @property
    def qualified_name(self) -> str:
        if self.table_name is None: raise ValueError('Error: table_name cannot be None')
        if self.schema_name is None: return self.table_name
        return f'{self.schema_name}.{self.table_name}'
    @property
    def insert_sql_stmt(self) -> str:
        if self._insert_sql_stmt is not None:
            return self._insert_sql_stmt
        if not self.column_map:
            raise ValueError(f'Cannot generate insert_sql for {self.qualified_name}; columns empty')
        columns=[]; binds=[]
        for key, col in self.column_map.items():
            oracle_col = col.oracle_name or key
            columns.append(oracle_col)
            if not col.csv_header_name: raise ValueError(f"Cannot build bind for column '{oracle_col}': csv_header_name missing")
            binds.append(col.bind_name if col.bind_name.startswith(':') else f':{col.bind_name}')
        self._insert_sql_stmt = f"INSERT INTO {self.qualified_name} ({','.join(columns)}) VALUES ({','.join(binds)})"
        return self._insert_sql_stmt
    @property
    def _fetch_tab_columns(self):
        logger.debug('Enter: OracleTable._fetch_tab_columns')
        try:
            if self._fetched_db_col is not None:
                return self._fetched_db_col
            sql = """SELECT COLUMN_NAME, DATA_TYPE, CHAR_LENGTH, CHAR_USED, COLUMN_ID, NULLABLE
                     FROM ALL_TAB_COLUMNS WHERE OWNER = :owner 
                     AND TABLE_NAME = :table_name 
                     ORDER BY COLUMN_ID
                    """
            res = self.oracle_client.fetchall(sql, {'owner': self.schema_name, 'table_name': self.table_name})
            self._fetched_db_col = list(res)
            return res
        except Exception as e:
            logger.error(f'Error: OracleTable._fetch_tab_columns Error: {e}')
            raise
    def _wipe_fetch_cache(self) -> None:
        self._fetched_db_col = None
        self._insert_sql_stmt = None
    
    def _align_columns(self) -> None:
        try:
            if len(self._fetch_tab_columns) > 0:
                for _, col_obj in self.column_map.items():
                    all_tab_row = [row for row in self._fetch_tab_columns if row.get('COLUMN_NAME') == col_obj.target_name]
                    if all_tab_row:
                        oracle_column = self.column_map.get(str(all_tab_row[0].get('COLUMN_NAME')))
                        if oracle_column:
                            oracle_column.column_id = int(str(all_tab_row[0].get('COLUMN_ID')))
                            oracle_column.oracle_name = str(all_tab_row[0].get('COLUMN_NAME'))
                            oracle_column.data_type = str(all_tab_row[0].get('DATA_TYPE'))
                            oracle_column.char_length = int(str(all_tab_row[0].get('CHAR_LENGTH') or 0))
                            oracle_column.char_used = str(all_tab_row[0].get('CHAR_USED'))
                            oracle_column.nullable = str(all_tab_row[0].get('NULLABLE')) == 'Y'
                            oracle_column.is_new = False
                self._check_existing_column_size()
            else:
                self._build_new_table()
        except Exception as e:
            logger.error(f'Error: OracleTable._align_columns Error: {e}')
            raise
    
    def _check_existing_column_size(self) -> None:
        all_tab_columns = self._fetch_tab_columns
        all_tab_names = [row.get('COLUMN_NAME') for row in all_tab_columns]
        new_cols=[]
        for oc_obj in self.column_map.values():
            if oc_obj.target_name not in all_tab_names:
                oc_obj.oracle_name = oc_obj.target_name
                oc_obj.is_new = True
                new_cols.append(oc_obj)
            else:
                oc_obj.is_new = False
                all_tab_row = [row for row in all_tab_columns if row['COLUMN_NAME'] == oc_obj.target_name][0]
                if all_tab_row.get('DATA_TYPE') == 'VARCHAR2':
                    current_db_limit = int(str(all_tab_row.get('CHAR_LENGTH') or 0))
                    current_data_max = oc_obj.detected_max_length or oc_obj.char_length or 0
                    if current_data_max > ORACLE_MAX_VARCHAR2_CHAR:
                        logger.warning(f"Column '{oc_obj.oracle_name}' data exceeds VARCHAR2 limit ({current_data_max}); cannot convert existing column to CLOB — skipping.")
                    elif current_data_max > current_db_limit:
                        self._alter_modify_existing_column(oc_obj)
        if new_cols:
            self._alter_add_columns(new_cols)
    
    def _build_new_table(self) -> None:
        for oracle_column in self.column_map.values():
            oracle_column.oracle_name = oracle_column.target_name
            oracle_column.is_new = True
        cols_sql = ','.join(col.column_definition() for col in self.column_map.values())
        create_table_ddl = f'CREATE TABLE {self.qualified_name} ({cols_sql})'
        self.oracle_client.execute_sql(create_table_ddl)
        self._wipe_fetch_cache(); self._align_columns()
    
    def _alter_modify_existing_column(self, col: OracleColumn) -> None:
        if col.data_type != 'VARCHAR2':
            raise ValueError(f'build_alter_modify only supports VARCHAR2 columns: {col.oracle_name}')
        data_max = col.detected_max_length or col.char_length or 0
        if data_max > ORACLE_MAX_VARCHAR2_CHAR:
            raise ValueError(f'{col.oracle_name} observed length exceeds VARCHAR2 limit')
        new_size = effective_max_varchar2(data_max)
        stmt = f'ALTER TABLE {self.qualified_name} MODIFY ({col.oracle_name} VARCHAR2({new_size} CHAR))'
        self.oracle_client.execute_sql(stmt)
        self._wipe_fetch_cache(); self._align_columns()
    
    def _alter_add_columns(self, new_columns: list[OracleColumn]) -> None:
        if not new_columns: raise ValueError('build_alter_add called with empty new_columns')
        col_defs = ','.join(col.column_definition() for col in new_columns)
        stmt = f'ALTER TABLE {self.qualified_name} ADD ({col_defs})'
        self.oracle_client.execute_sql(stmt); self._wipe_fetch_cache(); self._align_columns()
    
    def build_input_sizes(self) -> dict[str, object]:
        try:
            sizes={}
            for bind_name, col in self.column_map.items():
                dt=(col.data_type or '').upper()
                if dt in ('VARCHAR2','NVARCHAR2','CHAR'):
                    max_len = col.char_length or col.detected_max_length or 4000
                    sizes[bind_name] = int(max_len)
                elif dt in ('NUMBER','FLOAT'): sizes[bind_name] = oracledb.DB_TYPE_NUMBER
                elif dt == 'DATE': sizes[bind_name] = oracledb.DB_TYPE_DATE
                elif dt.startswith('TIMESTAMP'): sizes[bind_name] = oracledb.DB_TYPE_TIMESTAMP
                elif dt == 'CLOB': pass  # omit: DB_TYPE_CLOB causes per-row LOB round-trips in executemany
                elif dt == 'BLOB': sizes[bind_name] = oracledb.DB_TYPE_BLOB
                elif dt == 'RAW': sizes[bind_name] = oracledb.DB_TYPE_RAW
                elif dt == 'JSON': sizes[bind_name] = oracledb.DB_TYPE_JSON
                else: sizes[bind_name] = None
            return sizes
        except Exception as e:
            logger.error(f'Error: OracleTable.build_input_sizes Error: {e}')
            raise
    
    @staticmethod
    def construct_column_map(col_dict: dict[str,dict[str,str]]) -> dict[str, OracleColumn]:
        column_map={}
        if not col_dict: return column_map
        for target_name, col_info in col_dict.items():
            detected = int(col_info.get('max_col_size') or 0)
            data_type = 'CLOB' if detected > ORACLE_MAX_VARCHAR2_CHAR else 'VARCHAR2'
            column_map[target_name] = OracleColumn(target_name=str(col_info.get('target_name')), csv_header_name=col_info.get('csv_col_name'), csv_index=int(str(col_info.get('index'))), detected_max_length=detected, data_type=data_type)
        return column_map
    @staticmethod
    def construct_table(col_dict: dict[str, dict[str, str]], table: str, schema: str, oracle_client: OracleClient) -> OracleTable:
        oracle_table = OracleTable(table_name=table, schema_name=schema, oracle_client=oracle_client)
        oracle_table.column_map = OracleTable.construct_column_map(col_dict)
        oracle_table._align_columns()
        return oracle_table

@dataclass(slots=True)
class OracleColumn:
    csv_header_name: str | None = None
    csv_index: int | None = None
    target_name: str = ''
    detected_max_length: int | None = None
    oracle_name: str | None = None
    data_type: str = 'VARCHAR2'
    column_id: int | None = None
    nullable: bool = True
    char_length: int | None = None
    char_used: str | None = None
    is_new: bool = False
    def __post_init__(self) -> None:
        if not self.target_name and self.csv_header_name:
            object.__setattr__(self, 'target_name', to_oracle_snake(self.csv_header_name))
    @property
    def bind_name(self) -> str:
        return self.oracle_name or self.target_name or (self.csv_header_name or '')
    
    def _type_clause(self) -> str:
        if self.data_type == 'VARCHAR2':
            observed = self.char_length or self.detected_max_length or 0
            if observed > ORACLE_MAX_VARCHAR2_CHAR:
                raise ValueError(f"Column '{self.oracle_name}' observed length {observed} exceeds VARCHAR2 limit.")
            sized = effective_max_varchar2(observed) if observed else varchar2_growth_buffer
            return f'VARCHAR2({sized} CHAR)'
        if self.data_type == 'NUMBER': return 'NUMBER'
        if self.data_type == 'DATE': return 'DATE'
        if self.data_type == 'TIMESTAMP': return 'TIMESTAMP'
        if self.data_type == 'CLOB': return 'CLOB'
        raise ValueError(f"Unrecognised data_type '{self.data_type}' on column '{self.oracle_name}'.")
    
    def column_definition(self) -> str:
        if self.data_type == 'UNKNOWN':
            raise ValueError(f"Cannot generate DDL for column '{self.oracle_name}': data_type is UNKNOWN.")
        nullable_clause = 'NULL' if self.nullable else 'NOT NULL'
        type_clause = self._type_clause()
        return f'{self.oracle_name} {type_clause} {nullable_clause}'



def to_oracle_snake(value: str, max_len: int = 128, reserved: Iterable[str] = _ORACLE_RESERVED) -> str:
    s = str(value).strip()
    if not s:
        return 'COL'
    s = re.sub(r'([a-z0-9])([A-Z])', r'\1_\2', s)
    s = re.sub(r'([A-Za-z])([0-9])', r'\1_\2', s)
    s = re.sub(r'([0-9])([A-Za-z])', r'\1_\2', s)
    s = re.sub(r'[^A-Za-z0-9_]+', '_', s)
    s = re.sub(r'_{2,}', '_', s).strip('_').upper()
    if not s:
        return 'COL'
    if s[0].isdigit():
        s = 'C_' + s
    if s in reserved:
        s = f'{s}_COL'
    if len(s) > max_len:
        if s.endswith('_COL'):
            base = s[:max_len - 4].rstrip('_')
            s = f'{base}_COL'
        else:
            s = s[:max_len].rstrip('_')
    return s if s else 'COL'


def effective_max_varchar2(observed_char_len: int) -> int:
    if observed_char_len > ORACLE_MAX_VARCHAR2_CHAR:
        raise ValueError(f"observed_char_len {observed_char_len} exceeds Oracle limit {ORACLE_MAX_VARCHAR2_CHAR}")
    buffered = observed_char_len + varchar2_growth_buffer
    return min(buffered, ORACLE_MAX_VARCHAR2_CHAR)

def normalize_cell(raw: str, data_type: str) -> Any:
    value = _NULL_BYTE_RE.sub('', raw)
    if not value.strip(): return None
    if data_type == 'NUMBER': return _to_decimal(value)
    elif data_type == 'DATE': return _to_date(value)
    elif data_type == 'TIMESTAMP': return _to_datetime(value)
    else: return normalize_apos(value.strip())

def _to_decimal(value: str) -> Decimal | None:
    cleaned = _COMMA_RE.sub('', value.strip())
    try: return Decimal(cleaned)
    except InvalidOperation: return None

def _to_date(value: str) -> date | str:
    stripped = value.strip()
    try: return datetime.strptime(stripped[:10], _DATE_FMT).date()
    except ValueError: return stripped

def _to_datetime(value: str) -> datetime | str:
    stripped = value.strip(); cleaned = _TZ_OFFSET_RE.sub('', stripped)
    for fmt in _TIMESTAMP_FMTS:
        try: return datetime.strptime(cleaned, fmt)
        except ValueError: continue
    return stripped

def normalize_apos(cell: str) -> str:
    return (cell or '').replace("'", "''")
