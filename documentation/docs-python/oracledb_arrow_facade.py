from __future__ import annotations
import pyarrow as pa
import polars as pl
from typing import Any, Iterator, Iterable, TYPE_CHECKING

if TYPE_CHECKING:
    import oracledb

class OracleDataFrameFacade:
    """Wraps the native oracledb.OracleDataFrame to provide direct zero-copy ecosystem conversions."""
    
    def __init__(self, odf: oracledb.OracleDataFrame):
        self._odf = odf

    def column_arrays(self) -> list:
        """Return the underlying Arrow PyCapsule arrays."""
        return self._odf.column_arrays()

    def column_names(self) -> list[str]:
        """Return the column names of the data frame."""
        return self._odf.column_names()

    def num_rows(self) -> int:
        """Return the number of rows in the data frame."""
        return self._odf.num_rows()

    def num_columns(self) -> int:
        """Return the number of columns in the data frame."""
        return self._odf.num_columns()

    def to_pyarrow(self) -> pa.Table:
        """Zero-copy conversion to a PyArrow Table via the PyCapsule interface."""
        return pa.Table.from_arrays(self.column_arrays(), names=self.column_names())

    def to_polars(self) -> pl.DataFrame:
        """Zero-copy conversion to a Polars DataFrame."""
        return pl.from_arrow(self.to_pyarrow())

    def to_batches(self) -> Iterator[pa.RecordBatch]:
        """Yield PyArrow RecordBatches directly from the underlying data."""
        return self.to_pyarrow().to_batches()


class OracleArrowFacade:
    """Facade for executing queries and streaming native Arrow data from Oracle."""
    
    def __init__(self, client: OracleClient):
        self.client = client

    def fetch_df_all(
        self, 
        statement: str, 
        parameters: Iterable[Any] | None = None, 
        arraysize: int = 1000, 
        fetch_decimals: bool = True
    ) -> OracleDataFrameFacade:
        """Fetch all rows into a single zero-copy OracleDataFrame facade."""
        odf = self.client.connection.fetch_df_all(
            statement=statement,
            parameters=parameters or [],
            arraysize=arraysize,
            fetch_decimals=fetch_decimals
        )
        return OracleDataFrameFacade(odf)

    def fetch_df_batches(
        self, 
        statement: str, 
        parameters: Iterable[Any] | None = None, 
        size: int = 50_000, 
        fetch_decimals: bool = True
    ) -> Iterator[OracleDataFrameFacade]:
        """Yield batches of zero-copy OracleDataFrame facades for memory-safe streaming."""
        iterator = self.client.connection.fetch_df_batches(
            statement=statement,
            parameters=parameters or [],
            size=size,
            fetch_decimals=fetch_decimals
        )
        for odf in iterator:
            yield OracleDataFrameFacade(odf)

    def stream_record_batches(
        self, 
        statement: str, 
        parameters: Iterable[Any] | None = None, 
        batch_size: int = 50_000
    ) -> Iterator[pa.RecordBatch]:
        """End-to-end memory-safe stream yielding PyArrow RecordBatches for the orchestration layer."""
        for odf_facade in self.fetch_df_batches(statement, parameters, size=batch_size):
            yield from odf_facade.to_batches()