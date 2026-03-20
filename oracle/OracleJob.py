from __future__ import annotations
import csv, itertools, logging
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator
import pandas as pd
from .OracleModels import OracleTable, to_oracle_snake, normalize_cell
from .OracleClient import OracleClient
logger = logging.getLogger(__name__)

class Job:
    oracle_table: OracleTable
    oracle_client: OracleClient
    table: str
    schema: str
    batch_size: int
    batch: list[Batch]
    col_dict: dict[str, dict[str, str]]

    def __init__(self, 
                 source_path: Path|str, 
                 oracle_client: OracleClient = OracleClient(), 
                 table: str = '', 
                 schema: str = '', 
                 batch_size: int = 10000
                 ):
        self.col_dict = {}
        self.oracle_client = oracle_client
        self.schema = schema or oracle_client.oracle_user or ''
        self.source_path = Path(source_path).expanduser().resolve()
        if not self.source_path.is_file(): raise FileNotFoundError(f'Source file not found: {self.source_path}')
        self.file_name = self.source_path.name
        self.table = table or to_oracle_snake(self.file_name.rsplit('.', 1)[0])
        self.schema = schema or oracle_client.oracle_user or ''
        self.batch_size = batch_size
        self.file_name = self.source_path.name
        
        with self.source_path.open(mode='r', newline='', encoding='utf-8') as f:
            reader = csv.reader(f, dialect='excel')
            file_headers = next(reader, [])
            if not file_headers: raise ValueError('CSV file has no headers.')
            self.col_count = len(file_headers)
        
        for i, h in enumerate(file_headers):
            if not h.strip(): raise ValueError(f'Header at position {i} is blank.')
            base_name = to_oracle_snake(h)
            target_name = base_name
            counter=2
            while target_name in self.col_dict:
                target_name = f'{base_name}_{counter}'; counter += 1
            self.col_dict[target_name] = {
                                          "target_name": target_name, 
                                          "csv_col_name": h, 
                                          "index": str(i)
                                          }        
        self.validate_row_alignment()
    
    def rows(self) -> Iterator[list[str]]:
        with self.source_path.open(mode='r', newline='', encoding='utf-8') as f:
            reader = csv.reader(f, dialect='excel')
            # skip headers
            next(reader, None)
            for row in reader: yield row
    
    def validate_row_alignment(self) -> None:
        max_lengths = [0] * self.col_count
        row_count = 0
        for chunk in pd.read_csv(self.source_path, dtype=str, keep_default_na=False,
                                  encoding='utf-8', on_bad_lines='error', chunksize=50000):
            if len(chunk.columns) != self.col_count:
                raise ValueError(f'Column count mismatch: expected {self.col_count}, got {len(chunk.columns)}')
            chunk_max = chunk.apply(lambda s: s.str.len().max()).fillna(0).astype(int).tolist()
            max_lengths = [max(a, b) for a, b in zip(max_lengths, chunk_max)]
            row_count += len(chunk)
        for col in self.col_dict.values():
            col['max_col_size'] = str(max_lengths[int(col['index'])])
        self.row_count = row_count
        self.alignment_validated = True

    def run_job(self) -> int:
        self.oracle_table = OracleTable.construct_table(self.col_dict, self.table, self.schema, self.oracle_client)
        self.batch = []
        row_stream = self.rows()
        batch_start = 2  # row 1 is the header
        while True:
            batch = Batch.batch_exec(self, row_stream, batch_start)
            if batch.total_rows == 0:
                break
            self.batch.append(batch)
            batch_start += batch.total_rows
            if batch.all_rows_failed:
                return 1
        return 0




@dataclass
class Batch:
    rows_processed_count: int = 0
    total_rows: int = 0
    error_count: int = 0
    message: str = ''
    all_rows_failed: bool = False
    @staticmethod
    def batch_exec(job: Job, row_stream: Iterator[list[str]], batch_start: int = 2) -> Batch:
        batch = Batch()
        raw_rows = list(itertools.islice(row_stream, job.batch_size))
        if not raw_rows: return batch
        batch.total_rows = len(raw_rows)
        active_plan = job.oracle_table.active_plan
        formatted_data = [
            {bind_name: normalize_cell(row[idx], dtype) for idx, bind_name, dtype in active_plan}
            for row in raw_rows
        ]
        batch.rows_processed_count = len(formatted_data)
        batch.error_count = batch.batch_execute_inserts(sql=job.oracle_table.insert_sql_stmt, row_list=formatted_data, input_sizes=job.oracle_table.build_input_sizes(), connection=job.oracle_client.get_con(), batch_start=batch_start)
        if batch.error_count > 0:
            batch.all_rows_failed = batch.error_count == batch.total_rows
            batch.message = f"""------ Batch Has Errors ------
                                total_rows={batch.total_rows}
                                error_count={batch.error_count}
                                all_rows_failed={batch.all_rows_failed}
                                """
            logger.error(batch.message)
        else:
            batch.message = 'Batch Success'; logger.info(batch.message)
        return batch


    def batch_execute_inserts(self, sql, row_list, connection, input_sizes=None, batcherrors=True, batch_start: int = 2) -> int:
        try:
            cursor = connection.cursor()
            if input_sizes: cursor.setinputsizes(**input_sizes)
            cursor.executemany(sql, row_list, batcherrors=batcherrors)
            batch_errors = cursor.getbatcherrors()
            cursor.close()
            if batch_errors:
                batch_end = batch_start + len(row_list) - 1
                failed_lines = '\n'.join(f'  Row {batch_start + e.offset}: {e.message.strip()}' for e in batch_errors)
                logger.error(
                    f'Batch failed | rows {batch_start}-{batch_end} | {len(batch_errors)} error(s):\n{failed_lines}\n'
                    f'  Example: Row {batch_start + batch_errors[0].offset}: {batch_errors[0].message.strip()}'
                )
                connection.rollback()
                return len(batch_errors)
            connection.commit()
            return 0
        except Exception:
            connection.rollback(); raise