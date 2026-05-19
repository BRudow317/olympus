"""OracleDialect.py"""
from __future__ import annotations
import logging
logger = logging.getLogger(__name__)

SQL_Schema_COLUMNS = """
SELECT
    t.table_name,
    c.column_name,
    c.column_id,
    c.data_type,
    c.data_length,
    c.char_length,
    c.data_precision,
    c.data_scale,
    c.nullable,
    c.data_default
FROM all_tables t
JOIN all_tab_columns c
    ON  t.owner      = c.owner
    AND t.table_name = c.table_name
WHERE t.owner = :owner
ORDER BY t.table_name, c.column_id
"""

SQL_Schema_PKS = """
SELECT
    col.table_name,
    col.column_name
FROM all_constraints con
JOIN all_cons_columns col
    ON  con.constraint_name = col.constraint_name
    AND con.owner           = col.owner
WHERE con.constraint_type = 'P'
  AND con.owner = :owner
"""

SQL_Schema_FKS = """
SELECT
    fk_col.table_name  AS table_name,
    fk_col.column_name AS column_name,
    pk_col.table_name  AS ref_table,
    pk_col.column_name AS ref_column
FROM all_constraints fk_con
JOIN all_cons_columns fk_col
    ON  fk_con.constraint_name = fk_col.constraint_name
    AND fk_con.owner           = fk_col.owner
JOIN all_constraints pk_con
    ON  fk_con.r_constraint_name = pk_con.constraint_name
    AND fk_con.r_owner           = pk_con.owner
JOIN all_cons_columns pk_col
    ON  pk_con.constraint_name = pk_col.constraint_name
    AND pk_con.owner           = pk_col.owner
    AND fk_col.position        = pk_col.position
WHERE fk_con.constraint_type = 'R'
  AND fk_con.owner = :owner
"""

SQL_Schema_INDEXES = """
SELECT
    i.table_name,
    ic.column_name,
    i.uniqueness
FROM all_indexes i
JOIN all_ind_columns ic
    ON  i.index_name  = ic.index_name
    AND i.owner       = ic.index_owner
WHERE i.owner      = :owner
  AND i.index_type != 'LOB'
  AND i.generated  = 'N'
"""

SQL_Table_COLUMNS = """
SELECT
    column_name,
    column_id,
    data_type,
    data_length,
    char_length,
    data_precision,
    data_scale,
    nullable,
    data_default
FROM all_tab_columns
WHERE owner      = :owner
  AND table_name = :table_name
ORDER BY column_id
"""

SQL_Table_PKS = """
SELECT col.column_name
FROM all_constraints con
JOIN all_cons_columns col
    ON  con.constraint_name = col.constraint_name
    AND con.owner           = col.owner
WHERE con.constraint_type = 'P'
  AND con.owner      = :owner
  AND col.table_name = :table_name
"""

SQL_Table_FKS = """
SELECT
    fk_col.column_name AS column_name,
    pk_col.table_name  AS ref_table,
    pk_col.column_name AS ref_column
FROM all_constraints fk_con
JOIN all_cons_columns fk_col
    ON  fk_con.constraint_name = fk_col.constraint_name
    AND fk_con.owner           = fk_col.owner
JOIN all_constraints pk_con
    ON  fk_con.r_constraint_name = pk_con.constraint_name
    AND fk_con.r_owner           = pk_con.owner
JOIN all_cons_columns pk_col
    ON  pk_con.constraint_name = pk_col.constraint_name
    AND pk_con.owner           = pk_col.owner
    AND fk_col.position        = pk_col.position
WHERE fk_con.constraint_type = 'R'
  AND fk_con.owner      = :owner
  AND fk_col.table_name = :table_name
"""

SQL_Table_INDEXES = """
SELECT
    ic.column_name,
    i.uniqueness
FROM all_indexes i
JOIN all_ind_columns ic
    ON  i.index_name  = ic.index_name
    AND i.owner       = ic.index_owner
WHERE i.owner      = :owner
  AND i.table_name = :table_name
  AND i.index_type != 'LOB'
  AND i.generated  = 'N'
"""


