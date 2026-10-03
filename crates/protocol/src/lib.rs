#![forbid(unsafe_code)]

extern crate alloc;

mod error;
mod executor;
mod query;
mod result;
mod schema;
mod value;

#[cfg(test)]
mod tests;

pub use error::QueryServiceError;
pub use executor::QueryExecutor;
pub use query::{
    query_column_from_proto, query_column_to_proto, query_expr_from_proto, query_expr_to_proto,
    query_expr_value_from_proto, query_expr_value_to_proto, query_from_proto,
    query_select_from_proto, query_select_to_proto, query_to_proto,
    query_update_assignment_from_proto, query_update_assignment_to_proto, statement_from_proto,
    statement_to_proto,
};
pub use result::{
    query_result_from_proto, query_result_to_proto, result_column_from_proto,
    result_column_to_proto, row_from_proto, row_to_proto,
};
pub use schema::{
    column_from_proto, column_to_proto, index_from_proto, index_to_proto, table_from_proto,
    table_to_proto,
};
pub use value::{value_from_proto, value_to_proto};
