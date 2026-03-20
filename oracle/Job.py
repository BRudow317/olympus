from __future__ import annotations
import csv
from pathlib import Path
from typing import Iterator
import logging
from .OracleModels import OracleTable, to_oracle_snake
from .OracleClient import OracleClient
from .Batch import Batch

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
                 batch_size: int = 1000
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
        line_no = 1
        max_lengths = [0] * self.col_count
        for line_no, row in enumerate(self.rows(), start=2):
            if len(row) != self.col_count: raise ValueError(f'Row {line_no} has {len(row)} fields, expected {self.col_count}.')
            for i, cell in enumerate(row):
                if len(cell) > max_lengths[i]:
                    max_lengths[i] = len(cell)
        for col in self.col_dict.values():
            col['max_col_size'] = str(max_lengths[int(col['index'])])
        self.row_count = max(line_no - 1, 0)
        self.alignment_validated = True

    def run_job(self) -> int:
        self.oracle_table = OracleTable.construct_table(self.col_dict, self.table, self.schema, self.oracle_client)
        self.batch = []
        row_stream = self.rows()
        while True:
            batch = Batch.batch_exec(self, row_stream, self.batch_size)
            if batch.total_rows == 0:
                break
            self.batch.append(batch)
            if batch.all_rows_failed:
                return 1
        return 0
