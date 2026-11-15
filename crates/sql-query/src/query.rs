#[cfg(not(feature = "std"))]
use alloc::{boxed::Box, string::String, vec::Vec};
#[cfg(all(not(feature = "std"), feature = "wasm"))]
use alloc::{format, string::ToString};

use schema::{ColumnSchema, IndexSchema, TableSchema};
use value::{Row, Value};

#[derive(Debug, Clone, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
pub struct QueryColumn {
    pub table: String,
    pub column: String,
}

impl QueryColumn {
    #[must_use]
    pub const fn new(table: String, column: String) -> Self {
        Self { table, column }
    }
}

#[derive(Debug, Clone, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
pub enum QueryJoinKind {
    Inner,
    Left,
    Right,
    Full,
}

#[derive(Debug, Clone, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
pub struct QueryJoin {
    pub kind: QueryJoinKind,
    pub table: String,
    pub on: QueryExpr,
}

#[derive(Debug, Clone, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
pub enum QuerySortDirection {
    Asc,
    Desc,
}

#[derive(Debug, Clone, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
pub struct QueryOrderBy {
    pub by: QueryColumn,
    pub direction: QuerySortDirection,
}

#[derive(Debug, Clone, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
pub enum QueryExprValue {
    Column(QueryColumn),
    ExcludedColumn(String),
    Value(Value),
}

#[derive(Debug, Clone, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
pub enum QueryExpr {
    Value(QueryExprValue),
    Not(Box<Self>),
    Exists(Box<Self>),
    IsNull(Box<Self>),
    IsNotNull(Box<Self>),
    Equals(Box<Self>, Box<Self>),
    NotEquals(Box<Self>, Box<Self>),
    LessThan(Box<Self>, Box<Self>),
    LessThanOrEquals(Box<Self>, Box<Self>),
    GreaterThan(Box<Self>, Box<Self>),
    GreaterThanOrEquals(Box<Self>, Box<Self>),
    InList {
        expr: Box<Self>,
        list: Vec<Self>,
        negated: bool,
    },
    InSubquery {
        expr: Box<Self>,
        subquery: Box<QuerySelect>,
        negated: bool,
    },
    Like {
        expr: Box<Self>,
        pattern: Box<Self>,
    },
    And(Box<Self>, Box<Self>),
    Or(Box<Self>, Box<Self>),
}

#[derive(Debug, Clone, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
pub enum QueryCountTarget {
    AllRows,                    // COUNT(*)
    Single(String),             // COUNT(col)
    Distinct(String),           // COUNT(DISTINCT col)
    DistinctMulti(Vec<String>), // COUNT(DISTINCT col1, col2)
}

#[derive(Debug, Clone, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
pub enum QueryAggregate {
    Count(QueryCountTarget),
    Sum(QueryColumn),
    Avg(QueryColumn),
    Min(QueryColumn),
    Max(QueryColumn),
}

#[derive(Debug, Default, Clone, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
pub struct QueryFrom {
    pub table: String,
    pub joins: Vec<QueryJoin>,
}

#[derive(Debug, Clone, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
pub struct QueryUpdateAssignment {
    pub column: QueryColumn,
    pub value: QueryExprValue,
}

#[derive(Debug, Clone, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
pub struct QueryResultColumn {
    pub name: String,
    pub source_table: Option<String>,
    pub source_column: Option<String>,
}

#[derive(Debug, Clone, Default)]
#[cfg_attr(
    feature = "wasm",
    derive(serde::Serialize, serde::Deserialize, tsify::Tsify)
)]
pub struct QueryResult {
    pub rows: Vec<Row>,
    pub columns: Vec<QueryResultColumn>,
}

impl QueryResult {
    #[must_use]
    pub const fn new(rows: Vec<Row>) -> Self {
        Self {
            rows,
            columns: Vec::new(),
        }
    }

    #[must_use]
    pub const fn new_with_columns(rows: Vec<Row>, columns: Vec<QueryResultColumn>) -> Self {
        Self { rows, columns }
    }
}

#[derive(Debug, Clone, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
pub enum QueryHavingCountOperator {
    GreaterThan,
    GreaterThanOrEquals,
}

#[derive(Debug, Clone, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
pub struct QueryHavingCount {
    pub operator: QueryHavingCountOperator,
    pub value: i64,
}

#[derive(Debug, Clone, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
pub struct QueryTextConcat {
    pub column: QueryColumn,
    pub literal: String,
    pub alias: String,
}

#[derive(Debug, Default, Clone, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
pub struct QuerySelect {
    pub from: QueryFrom,
    pub projection: Vec<QueryColumn>,
    pub text_concats: Vec<Option<QueryTextConcat>>,
    pub distinct: bool,
    pub predicate: Option<QueryExpr>,
    pub aggregates: Vec<QueryAggregate>,
    pub group_by: Vec<QueryColumn>,
    pub order_by: Vec<QueryOrderBy>,
    pub limit: Option<usize>,
    pub offset: Option<usize>,
    pub having: Option<QueryHavingCount>,
}
#[derive(Debug, Clone, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
pub struct QueryInsert {
    pub table: String,
    pub row: Row,
    pub returning: Option<Vec<String>>,
}

#[derive(Debug, Clone, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
pub enum QueryInsertValue {
    Value(Value),
    Default,
}

#[derive(Debug, Clone, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
pub struct QueryInsertValues {
    pub table: String,
    pub columns: Vec<String>,
    pub rows: Vec<Vec<QueryInsertValue>>,
    pub returning: Option<Vec<String>>,
    pub on_conflict_do_nothing: Option<Vec<String>>,
    pub on_conflict_do_update: Option<(Vec<String>, Vec<QueryUpdateAssignment>)>,
}
#[derive(Debug, Clone, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
pub struct QueryUpdate {
    pub from: QueryFrom,
    pub assignments: Vec<QueryUpdateAssignment>,
    pub predicate: Option<QueryExpr>,
    pub returning: Option<Vec<QueryColumn>>,
}
#[derive(Debug, Clone, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
pub struct QueryDelete {
    pub from: QueryFrom,
    pub predicate: Option<QueryExpr>,
    pub returning: Option<Vec<QueryColumn>>,
}

#[derive(Debug, Clone, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
pub enum Query {
    Select(QuerySelect),
    Insert(QueryInsert),
    InsertValues(QueryInsertValues),
    Update(QueryUpdate),
    Delete(QueryDelete),
}

#[derive(Debug, Clone, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
pub enum AlterTableOperation {
    AddColumn(ColumnSchema),
    DropColumn(String),
    RenameColumn { old_name: String, new_name: String },
    RenameTable { new_name: String },
    AddIndex(IndexSchema),
    RenameIndex { old_name: String, new_name: String },
    DropIndex(String),
}

#[derive(Debug, Clone, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
pub enum AlterIndexOperation {
    Rename { new_name: String },
}

#[derive(Debug, Clone, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
pub enum DataDefinition {
    CreateTable {
        schema: TableSchema,
        if_not_exists: bool,
    },
    /// A unique index covering exactly the primary-key columns supplies the primary-key
    /// index name. Otherwise natural/composite keys get an implicit `pk_<table>` index.
    CreateTableWithIndexes {
        schema: TableSchema,
        indexes: Vec<IndexSchema>,
        if_not_exists: bool,
    },
    AlterTable {
        table_name: String,
        operations: Vec<AlterTableOperation>,
        if_exists: bool,
    },
    DropTable {
        table_name: String,
        if_exists: bool,
    },
    CreateIndex {
        schema: IndexSchema,
        if_not_exists: bool,
    },
    CreateIndexUnresolved {
        index_name: String,
        table_name: String,
        column_names: Vec<String>,
        unique: bool,
        if_not_exists: bool,
    },
    AlterIndex {
        index_name: String,
        operation: AlterIndexOperation,
        if_exists: bool,
    },
    DropIndex {
        index_name: String,
        if_exists: bool,
    },
}

#[derive(Debug, Clone, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
pub enum Statement {
    Query(Query),
    DataDefinition(DataDefinition),
}

impl Statement {
    #[must_use]
    pub fn into_query(self) -> Option<Query> {
        match self {
            Self::Query(query) => Some(query),
            Self::DataDefinition(_) => None,
        }
    }

    #[must_use]
    pub fn into_data_definition(self) -> Option<DataDefinition> {
        match self {
            Self::Query(_) => None,
            Self::DataDefinition(def) => Some(def),
        }
    }
}
