use proto::{
    QueryResult as ProtoQueryResult, QueryResultColumn as ProtoQueryResultColumn, Row as ProtoRow,
};
use query::{QueryResult, QueryResultColumn};
use value::Row;

use crate::{QueryServiceError, value_from_proto, value_to_proto};

pub fn row_to_proto(row: Row) -> ProtoRow {
    ProtoRow {
        values: row.values.into_iter().map(value_to_proto).collect(),
    }
}

pub fn row_from_proto(row: ProtoRow) -> Result<Row, QueryServiceError> {
    row.values
        .into_iter()
        .map(value_from_proto)
        .collect::<Result<_, _>>()
        .map(Row::new)
}

pub fn result_column_to_proto(column: QueryResultColumn) -> ProtoQueryResultColumn {
    ProtoQueryResultColumn {
        name: column.name,
        source_table: column.source_table,
        source_column: column.source_column,
    }
}

pub fn result_column_from_proto(column: ProtoQueryResultColumn) -> QueryResultColumn {
    QueryResultColumn {
        name: column.name,
        source_table: column.source_table,
        source_column: column.source_column,
    }
}

pub fn query_result_to_proto(result: QueryResult) -> ProtoQueryResult {
    ProtoQueryResult {
        rows: result.rows.into_iter().map(row_to_proto).collect(),
        columns: result
            .columns
            .into_iter()
            .map(result_column_to_proto)
            .collect(),
    }
}

pub fn query_result_from_proto(result: ProtoQueryResult) -> Result<QueryResult, QueryServiceError> {
    Ok(QueryResult {
        rows: result
            .rows
            .into_iter()
            .map(row_from_proto)
            .collect::<Result<_, _>>()?,
        columns: result
            .columns
            .into_iter()
            .map(result_column_from_proto)
            .collect(),
    })
}
