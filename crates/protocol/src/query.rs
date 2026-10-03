use alloc::boxed::Box;

use proto::{
    BinaryExpr, InListExpr, InSubqueryExpr, LikeExpr, OptionalQueryTextConcat,
    QueryAggregate as ProtoQueryAggregate, QueryColumn as ProtoQueryColumn,
    QueryExpr as ProtoQueryExpr, QueryExprValue as ProtoQueryExprValue,
    QueryFrom as ProtoQueryFrom, QueryHavingCount as ProtoHaving,
    QueryHavingCountOperator as ProtoHavingOperator, QueryJoin as ProtoQueryJoin,
    QueryJoinKind as ProtoJoinKind, QueryOrderBy as ProtoOrderBy, QuerySelect as ProtoSelect,
    QuerySortDirection as ProtoSortDirection, QueryTextConcat as ProtoTextConcat, query_aggregate,
    query_count_target, query_expr, query_expr_value,
};
use query::{
    AlterIndexOperation, AlterTableOperation, DataDefinition, Query, QueryAggregate, QueryColumn,
    QueryCountTarget, QueryDelete, QueryExpr, QueryExprValue, QueryFrom, QueryHavingCount,
    QueryHavingCountOperator, QueryInsert, QueryInsertValue, QueryInsertValues, QueryJoin,
    QueryJoinKind, QueryOrderBy, QuerySelect, QuerySortDirection, QueryTextConcat, QueryUpdate,
    QueryUpdateAssignment, Statement,
};

use crate::{
    QueryServiceError, column_from_proto, column_to_proto, index_from_proto, index_to_proto,
    table_from_proto, table_to_proto, value_from_proto, value_to_proto,
};

pub fn query_column_to_proto(column: QueryColumn) -> ProtoQueryColumn {
    ProtoQueryColumn {
        table: column.table,
        column: column.column,
    }
}

pub fn query_column_from_proto(column: ProtoQueryColumn) -> QueryColumn {
    QueryColumn {
        table: column.table,
        column: column.column,
    }
}

pub fn query_expr_value_to_proto(value: QueryExprValue) -> ProtoQueryExprValue {
    let kind = match value {
        QueryExprValue::Column(column) => {
            query_expr_value::Kind::Column(query_column_to_proto(column))
        }
        QueryExprValue::ExcludedColumn(column) => query_expr_value::Kind::ExcludedColumn(column),
        QueryExprValue::Value(value) => query_expr_value::Kind::Value(value_to_proto(value)),
    };
    ProtoQueryExprValue { kind: Some(kind) }
}

pub fn query_expr_value_from_proto(
    value: ProtoQueryExprValue,
) -> Result<QueryExprValue, QueryServiceError> {
    match value
        .kind
        .ok_or_else(|| QueryServiceError::Invalid("missing QueryExprValue kind".into()))?
    {
        query_expr_value::Kind::Column(column) => {
            Ok(QueryExprValue::Column(query_column_from_proto(column)))
        }
        query_expr_value::Kind::ExcludedColumn(column) => {
            Ok(QueryExprValue::ExcludedColumn(column))
        }
        query_expr_value::Kind::Value(value) => value_from_proto(value).map(QueryExprValue::Value),
    }
}

pub fn query_expr_to_proto(expr: QueryExpr) -> ProtoQueryExpr {
    use QueryExpr as Q;
    use query_expr::Kind as K;
    let kind = match expr {
        Q::Value(value) => K::Value(query_expr_value_to_proto(value)),
        Q::Not(value) => K::Not(Box::new(query_expr_to_proto(*value))),
        Q::Exists(value) => K::Exists(Box::new(query_expr_to_proto(*value))),
        Q::IsNull(value) => K::IsNull(Box::new(query_expr_to_proto(*value))),
        Q::IsNotNull(value) => K::IsNotNull(Box::new(query_expr_to_proto(*value))),
        Q::Equals(a, b) => binary(|v| K::Equals(Box::new(v)), *a, *b),
        Q::NotEquals(a, b) => binary(|v| K::NotEquals(Box::new(v)), *a, *b),
        Q::LessThan(a, b) => binary(|v| K::LessThan(Box::new(v)), *a, *b),
        Q::LessThanOrEquals(a, b) => binary(|v| K::LessThanOrEquals(Box::new(v)), *a, *b),
        Q::GreaterThan(a, b) => binary(|v| K::GreaterThan(Box::new(v)), *a, *b),
        Q::GreaterThanOrEquals(a, b) => binary(|v| K::GreaterThanOrEquals(Box::new(v)), *a, *b),
        Q::InList {
            expr,
            list,
            negated,
        } => K::InList(Box::new(InListExpr {
            expr: Some(Box::new(query_expr_to_proto(*expr))),
            list: list.into_iter().map(query_expr_to_proto).collect(),
            negated,
        })),
        Q::InSubquery {
            expr,
            subquery,
            negated,
        } => K::InSubquery(Box::new(InSubqueryExpr {
            expr: Some(Box::new(query_expr_to_proto(*expr))),
            subquery: Some(Box::new(query_select_to_proto(*subquery))),
            negated,
        })),
        Q::Like { expr, pattern } => K::Like(Box::new(LikeExpr {
            expr: Some(Box::new(query_expr_to_proto(*expr))),
            pattern: Some(Box::new(query_expr_to_proto(*pattern))),
        })),
        Q::And(a, b) => binary(|v| K::And(Box::new(v)), *a, *b),
        Q::Or(a, b) => binary(|v| K::Or(Box::new(v)), *a, *b),
    };
    ProtoQueryExpr { kind: Some(kind) }
}

fn binary(
    build: impl FnOnce(BinaryExpr) -> query_expr::Kind,
    left: QueryExpr,
    right: QueryExpr,
) -> query_expr::Kind {
    build(BinaryExpr {
        left: Some(Box::new(query_expr_to_proto(left))),
        right: Some(Box::new(query_expr_to_proto(right))),
    })
}

pub fn query_expr_from_proto(expr: ProtoQueryExpr) -> Result<QueryExpr, QueryServiceError> {
    use query_expr::Kind as K;
    let kind = expr.kind.ok_or_else(|| invalid("QueryExpr kind"))?;
    match kind {
        K::Value(value) => Ok(QueryExpr::Value(query_expr_value_from_proto(value)?)),
        K::Not(value) => unary_from_proto(*value, UnaryOperator::Not),
        K::Exists(value) => unary_from_proto(*value, UnaryOperator::Exists),
        K::IsNull(value) => unary_from_proto(*value, UnaryOperator::IsNull),
        K::IsNotNull(value) => unary_from_proto(*value, UnaryOperator::IsNotNull),
        K::Equals(value) => binary_from_proto(*value, BinaryOperator::Equals),
        K::NotEquals(value) => binary_from_proto(*value, BinaryOperator::NotEquals),
        K::LessThan(value) => binary_from_proto(*value, BinaryOperator::LessThan),
        K::LessThanOrEquals(value) => binary_from_proto(*value, BinaryOperator::LessThanOrEquals),
        K::GreaterThan(value) => binary_from_proto(*value, BinaryOperator::GreaterThan),
        K::GreaterThanOrEquals(value) => {
            binary_from_proto(*value, BinaryOperator::GreaterThanOrEquals)
        }
        K::And(value) => binary_from_proto(*value, BinaryOperator::And),
        K::Or(value) => binary_from_proto(*value, BinaryOperator::Or),
        K::InList(value) => in_list_from_proto(*value),
        K::InSubquery(value) => in_subquery_from_proto(*value),
        K::Like(value) => like_from_proto(*value),
    }
}

enum UnaryOperator {
    Not,
    Exists,
    IsNull,
    IsNotNull,
}

fn unary_from_proto(
    value: ProtoQueryExpr,
    operator: UnaryOperator,
) -> Result<QueryExpr, QueryServiceError> {
    let value = Box::new(query_expr_from_proto(value)?);
    Ok(match operator {
        UnaryOperator::Not => QueryExpr::Not(value),
        UnaryOperator::Exists => QueryExpr::Exists(value),
        UnaryOperator::IsNull => QueryExpr::IsNull(value),
        UnaryOperator::IsNotNull => QueryExpr::IsNotNull(value),
    })
}

enum BinaryOperator {
    Equals,
    NotEquals,
    LessThan,
    LessThanOrEquals,
    GreaterThan,
    GreaterThanOrEquals,
    And,
    Or,
}

fn binary_from_proto(
    value: BinaryExpr,
    operator: BinaryOperator,
) -> Result<QueryExpr, QueryServiceError> {
    let left = Box::new(query_expr_from_proto(
        *value.left.ok_or_else(|| invalid("BinaryExpr.left"))?,
    )?);
    let right = Box::new(query_expr_from_proto(
        *value.right.ok_or_else(|| invalid("BinaryExpr.right"))?,
    )?);
    Ok(match operator {
        BinaryOperator::Equals => QueryExpr::Equals(left, right),
        BinaryOperator::NotEquals => QueryExpr::NotEquals(left, right),
        BinaryOperator::LessThan => QueryExpr::LessThan(left, right),
        BinaryOperator::LessThanOrEquals => QueryExpr::LessThanOrEquals(left, right),
        BinaryOperator::GreaterThan => QueryExpr::GreaterThan(left, right),
        BinaryOperator::GreaterThanOrEquals => QueryExpr::GreaterThanOrEquals(left, right),
        BinaryOperator::And => QueryExpr::And(left, right),
        BinaryOperator::Or => QueryExpr::Or(left, right),
    })
}

fn in_list_from_proto(value: InListExpr) -> Result<QueryExpr, QueryServiceError> {
    Ok(QueryExpr::InList {
        expr: Box::new(query_expr_from_proto(
            *value.expr.ok_or_else(|| invalid("InListExpr.expr"))?,
        )?),
        list: value
            .list
            .into_iter()
            .map(query_expr_from_proto)
            .collect::<Result<_, _>>()?,
        negated: value.negated,
    })
}

fn in_subquery_from_proto(value: InSubqueryExpr) -> Result<QueryExpr, QueryServiceError> {
    Ok(QueryExpr::InSubquery {
        expr: Box::new(query_expr_from_proto(
            *value.expr.ok_or_else(|| invalid("InSubqueryExpr.expr"))?,
        )?),
        subquery: Box::new(query_select_from_proto(
            *value
                .subquery
                .ok_or_else(|| invalid("InSubqueryExpr.subquery"))?,
        )?),
        negated: value.negated,
    })
}

fn like_from_proto(value: LikeExpr) -> Result<QueryExpr, QueryServiceError> {
    Ok(QueryExpr::Like {
        expr: Box::new(query_expr_from_proto(
            *value.expr.ok_or_else(|| invalid("LikeExpr.expr"))?,
        )?),
        pattern: Box::new(query_expr_from_proto(
            *value.pattern.ok_or_else(|| invalid("LikeExpr.pattern"))?,
        )?),
    })
}

pub fn query_select_to_proto(select: QuerySelect) -> ProtoSelect {
    ProtoSelect {
        from: Some(query_from_to_proto(select.from)),
        projection: select
            .projection
            .into_iter()
            .map(query_column_to_proto)
            .collect(),
        predicate: select
            .predicate
            .map(|expr| Box::new(query_expr_to_proto(expr))),
        aggregates: select
            .aggregates
            .into_iter()
            .map(aggregate_to_proto)
            .collect(),
        group_by: select
            .group_by
            .into_iter()
            .map(query_column_to_proto)
            .collect(),
        order_by: select.order_by.into_iter().map(order_to_proto).collect(),
        limit: select.limit.map(|v| v as u64),
        offset: select.offset.map(|v| v as u64),
        having: select.having.map(having_to_proto),
        text_concats: select
            .text_concats
            .into_iter()
            .map(|v| OptionalQueryTextConcat {
                value: v.map(text_concat_to_proto),
            })
            .collect(),
        distinct: select.distinct,
    }
}

pub fn query_select_from_proto(select: ProtoSelect) -> Result<QuerySelect, QueryServiceError> {
    Ok(QuerySelect {
        from: query_from_from_proto(select.from.ok_or_else(|| invalid("QuerySelect.from"))?)?,
        projection: select
            .projection
            .into_iter()
            .map(query_column_from_proto)
            .collect(),
        text_concats: select
            .text_concats
            .into_iter()
            .map(|v| v.value.map(text_concat_from_proto).transpose())
            .collect::<Result<_, _>>()?,
        distinct: select.distinct,
        predicate: select
            .predicate
            .map(|expr| query_expr_from_proto(*expr))
            .transpose()?,
        aggregates: select
            .aggregates
            .into_iter()
            .map(aggregate_from_proto)
            .collect::<Result<_, _>>()?,
        group_by: select
            .group_by
            .into_iter()
            .map(query_column_from_proto)
            .collect(),
        order_by: select
            .order_by
            .into_iter()
            .map(order_from_proto)
            .collect::<Result<_, _>>()?,
        limit: select
            .limit
            .map(|v| usize::try_from(v).map_err(|_| invalid("QuerySelect.limit overflow")))
            .transpose()?,
        offset: select
            .offset
            .map(|v| usize::try_from(v).map_err(|_| invalid("QuerySelect.offset overflow")))
            .transpose()?,
        having: select.having.map(having_from_proto).transpose()?,
    })
}

fn query_from_to_proto(from: QueryFrom) -> ProtoQueryFrom {
    ProtoQueryFrom {
        table: from.table,
        joins: from.joins.into_iter().map(join_to_proto).collect(),
    }
}
fn query_from_from_proto(from: ProtoQueryFrom) -> Result<QueryFrom, QueryServiceError> {
    Ok(QueryFrom {
        table: from.table,
        joins: from
            .joins
            .into_iter()
            .map(join_from_proto)
            .collect::<Result<_, _>>()?,
    })
}
fn join_to_proto(join: QueryJoin) -> ProtoQueryJoin {
    ProtoQueryJoin {
        kind: match join.kind {
            QueryJoinKind::Inner => ProtoJoinKind::Inner as i32,
            QueryJoinKind::Left => ProtoJoinKind::Left as i32,
            QueryJoinKind::Right => ProtoJoinKind::Right as i32,
            QueryJoinKind::Full => ProtoJoinKind::Full as i32,
        },
        table: join.table,
        on: Some(query_expr_to_proto(join.on)),
    }
}
fn join_from_proto(join: ProtoQueryJoin) -> Result<QueryJoin, QueryServiceError> {
    let kind =
        match ProtoJoinKind::try_from(join.kind).map_err(|_| invalid("unknown QueryJoinKind"))? {
            ProtoJoinKind::Inner => QueryJoinKind::Inner,
            ProtoJoinKind::Left => QueryJoinKind::Left,
            ProtoJoinKind::Right => QueryJoinKind::Right,
            ProtoJoinKind::Full => QueryJoinKind::Full,
            ProtoJoinKind::Unspecified => {
                return Err(invalid("unspecified QueryJoinKind"));
            }
        };
    Ok(QueryJoin {
        kind,
        table: join.table,
        on: query_expr_from_proto(join.on.ok_or_else(|| invalid("QueryJoin.on"))?)?,
    })
}
fn order_to_proto(order: QueryOrderBy) -> ProtoOrderBy {
    ProtoOrderBy {
        by: Some(query_column_to_proto(order.by)),
        direction: match order.direction {
            QuerySortDirection::Asc => ProtoSortDirection::Asc as i32,
            QuerySortDirection::Desc => ProtoSortDirection::Desc as i32,
        },
    }
}
fn order_from_proto(order: ProtoOrderBy) -> Result<QueryOrderBy, QueryServiceError> {
    let direction = match ProtoSortDirection::try_from(order.direction)
        .map_err(|_| invalid("unknown QuerySortDirection"))?
    {
        ProtoSortDirection::Asc => QuerySortDirection::Asc,
        ProtoSortDirection::Desc => QuerySortDirection::Desc,
        ProtoSortDirection::Unspecified => {
            return Err(invalid("unspecified QuerySortDirection"));
        }
    };
    Ok(QueryOrderBy {
        by: query_column_from_proto(order.by.ok_or_else(|| invalid("QueryOrderBy.by"))?),
        direction,
    })
}
fn aggregate_to_proto(value: QueryAggregate) -> ProtoQueryAggregate {
    use query_aggregate::Kind as K;
    let kind = match value {
        QueryAggregate::Count(target) => K::Count(count_target_to_proto(target)),
        QueryAggregate::Sum(column) => K::Sum(query_column_to_proto(column)),
        QueryAggregate::Avg(column) => K::Avg(query_column_to_proto(column)),
        QueryAggregate::Min(column) => K::Min(query_column_to_proto(column)),
        QueryAggregate::Max(column) => K::Max(query_column_to_proto(column)),
    };
    ProtoQueryAggregate { kind: Some(kind) }
}
fn aggregate_from_proto(value: ProtoQueryAggregate) -> Result<QueryAggregate, QueryServiceError> {
    use query_aggregate::Kind as K;
    Ok(
        match value.kind.ok_or_else(|| invalid("QueryAggregate.kind"))? {
            K::Count(target) => QueryAggregate::Count(count_target_from_proto(target)?),
            K::Sum(c) => QueryAggregate::Sum(query_column_from_proto(c)),
            K::Avg(c) => QueryAggregate::Avg(query_column_from_proto(c)),
            K::Min(c) => QueryAggregate::Min(query_column_from_proto(c)),
            K::Max(c) => QueryAggregate::Max(query_column_from_proto(c)),
        },
    )
}
fn count_target_to_proto(target: QueryCountTarget) -> proto::QueryCountTarget {
    use query_count_target::Kind as K;
    proto::QueryCountTarget {
        kind: Some(match target {
            QueryCountTarget::AllRows => K::AllRows(()),
            QueryCountTarget::Single(c) => K::Single(c),
            QueryCountTarget::Distinct(c) => K::Distinct(c),
            QueryCountTarget::DistinctMulti(columns) => {
                K::DistinctMulti(proto::DistinctMulti { columns })
            }
        }),
    }
}
fn count_target_from_proto(
    target: proto::QueryCountTarget,
) -> Result<QueryCountTarget, QueryServiceError> {
    use query_count_target::Kind as K;
    Ok(
        match target
            .kind
            .ok_or_else(|| invalid("QueryCountTarget.kind"))?
        {
            K::AllRows(()) => QueryCountTarget::AllRows,
            K::Single(c) => QueryCountTarget::Single(c),
            K::Distinct(c) => QueryCountTarget::Distinct(c),
            K::DistinctMulti(v) => QueryCountTarget::DistinctMulti(v.columns),
        },
    )
}
fn having_to_proto(value: QueryHavingCount) -> ProtoHaving {
    ProtoHaving {
        operator: match value.operator {
            QueryHavingCountOperator::GreaterThan => ProtoHavingOperator::GreaterThan as i32,
            QueryHavingCountOperator::GreaterThanOrEquals => {
                ProtoHavingOperator::GreaterThanOrEquals as i32
            }
        },
        value: value.value,
    }
}
fn having_from_proto(value: ProtoHaving) -> Result<QueryHavingCount, QueryServiceError> {
    let operator = match ProtoHavingOperator::try_from(value.operator)
        .map_err(|_| invalid("unknown QueryHavingCountOperator"))?
    {
        ProtoHavingOperator::GreaterThan => QueryHavingCountOperator::GreaterThan,
        ProtoHavingOperator::GreaterThanOrEquals => QueryHavingCountOperator::GreaterThanOrEquals,
        ProtoHavingOperator::Unspecified => {
            return Err(invalid("unspecified QueryHavingCountOperator"));
        }
    };
    Ok(QueryHavingCount {
        operator,
        value: value.value,
    })
}
fn text_concat_to_proto(value: QueryTextConcat) -> ProtoTextConcat {
    ProtoTextConcat {
        column: Some(query_column_to_proto(value.column)),
        literal: value.literal,
        alias: value.alias,
    }
}
fn text_concat_from_proto(value: ProtoTextConcat) -> Result<QueryTextConcat, QueryServiceError> {
    Ok(QueryTextConcat {
        column: query_column_from_proto(
            value
                .column
                .ok_or_else(|| invalid("QueryTextConcat.column"))?,
        ),
        literal: value.literal,
        alias: value.alias,
    })
}
pub fn query_update_assignment_to_proto(
    value: QueryUpdateAssignment,
) -> proto::QueryUpdateAssignment {
    proto::QueryUpdateAssignment {
        column: Some(query_column_to_proto(value.column)),
        value: Some(query_expr_value_to_proto(value.value)),
    }
}

pub fn query_update_assignment_from_proto(
    value: proto::QueryUpdateAssignment,
) -> Result<QueryUpdateAssignment, QueryServiceError> {
    Ok(QueryUpdateAssignment {
        column: query_column_from_proto(
            value
                .column
                .ok_or_else(|| invalid("QueryUpdateAssignment.column"))?,
        ),
        value: query_expr_value_from_proto(
            value
                .value
                .ok_or_else(|| invalid("QueryUpdateAssignment.value"))?,
        )?,
    })
}

pub fn query_to_proto(query: Query) -> proto::Query {
    use proto::query::Kind as K;
    proto::Query {
        kind: Some(match query {
            Query::Select(value) => K::Select(query_select_to_proto(value)),
            Query::Insert(value) => K::Insert(query_insert_to_proto(value)),
            Query::InsertValues(value) => K::InsertValues(query_insert_values_to_proto(value)),
            Query::Update(value) => K::Update(query_update_to_proto(value)),
            Query::Delete(value) => K::Delete(query_delete_to_proto(value)),
        }),
    }
}

pub fn query_from_proto(query: proto::Query) -> Result<Query, QueryServiceError> {
    use proto::query::Kind as K;
    Ok(match query.kind.ok_or_else(|| invalid("Query.kind"))? {
        K::Select(value) => Query::Select(query_select_from_proto(value)?),
        K::Insert(value) => Query::Insert(query_insert_from_proto(value)?),
        K::InsertValues(value) => Query::InsertValues(query_insert_values_from_proto(value)?),
        K::Update(value) => Query::Update(query_update_from_proto(value)?),
        K::Delete(value) => Query::Delete(query_delete_from_proto(value)?),
    })
}

fn query_insert_to_proto(value: QueryInsert) -> proto::QueryInsert {
    proto::QueryInsert {
        table: value.table,
        row: Some(proto::Row {
            values: value.row.values.into_iter().map(value_to_proto).collect(),
        }),
        returning: value
            .returning
            .map(|columns| proto::ReturningStrings { columns }),
    }
}
fn query_insert_from_proto(value: proto::QueryInsert) -> Result<QueryInsert, QueryServiceError> {
    let row = value.row.ok_or_else(|| invalid("QueryInsert.row"))?;
    Ok(QueryInsert {
        table: value.table,
        row: value::Row {
            values: row
                .values
                .into_iter()
                .map(value_from_proto)
                .collect::<Result<_, _>>()?,
        },
        returning: value.returning.map(|r| r.columns),
    })
}
fn query_insert_value_to_proto(value: QueryInsertValue) -> proto::QueryInsertValue {
    use proto::query_insert_value::Kind as K;
    proto::QueryInsertValue {
        kind: Some(match value {
            QueryInsertValue::Value(v) => K::Value(value_to_proto(v)),
            QueryInsertValue::Default => K::Default(()),
        }),
    }
}
fn query_insert_value_from_proto(
    value: proto::QueryInsertValue,
) -> Result<QueryInsertValue, QueryServiceError> {
    use proto::query_insert_value::Kind as K;
    Ok(
        match value.kind.ok_or_else(|| invalid("QueryInsertValue.kind"))? {
            K::Value(v) => QueryInsertValue::Value(value_from_proto(v)?),
            K::Default(()) => QueryInsertValue::Default,
        },
    )
}
fn query_insert_values_to_proto(value: QueryInsertValues) -> proto::QueryInsertValues {
    proto::QueryInsertValues {
        table: value.table,
        columns: value.columns,
        rows: value
            .rows
            .into_iter()
            .map(|values| proto::QueryInsertRow {
                values: values
                    .into_iter()
                    .map(query_insert_value_to_proto)
                    .collect(),
            })
            .collect(),
        returning: value
            .returning
            .map(|columns| proto::ReturningStrings { columns }),
        on_conflict_do_nothing: value
            .on_conflict_do_nothing
            .map(|values| proto::StringList { values }),
        on_conflict_do_update: value.on_conflict_do_update.map(|(columns, assignments)| {
            proto::ConflictUpdate {
                columns,
                assignments: assignments
                    .into_iter()
                    .map(query_update_assignment_to_proto)
                    .collect(),
            }
        }),
    }
}
fn query_insert_values_from_proto(
    value: proto::QueryInsertValues,
) -> Result<QueryInsertValues, QueryServiceError> {
    Ok(QueryInsertValues {
        table: value.table,
        columns: value.columns,
        rows: value
            .rows
            .into_iter()
            .map(|row| {
                row.values
                    .into_iter()
                    .map(query_insert_value_from_proto)
                    .collect()
            })
            .collect::<Result<_, _>>()?,
        returning: value.returning.map(|r| r.columns),
        on_conflict_do_nothing: value.on_conflict_do_nothing.map(|v| v.values),
        on_conflict_do_update: value
            .on_conflict_do_update
            .map(|v| {
                Ok((
                    v.columns,
                    v.assignments
                        .into_iter()
                        .map(query_update_assignment_from_proto)
                        .collect::<Result<_, _>>()?,
                ))
            })
            .transpose()?,
    })
}
fn query_update_to_proto(value: QueryUpdate) -> proto::QueryUpdate {
    proto::QueryUpdate {
        from: Some(query_from_to_proto(value.from)),
        assignments: value
            .assignments
            .into_iter()
            .map(query_update_assignment_to_proto)
            .collect(),
        predicate: value.predicate.map(query_expr_to_proto),
        returning: value.returning.map(|columns| proto::ReturningColumns {
            columns: columns.into_iter().map(query_column_to_proto).collect(),
        }),
    }
}
fn query_update_from_proto(value: proto::QueryUpdate) -> Result<QueryUpdate, QueryServiceError> {
    Ok(QueryUpdate {
        from: query_from_from_proto(value.from.ok_or_else(|| invalid("QueryUpdate.from"))?)?,
        assignments: value
            .assignments
            .into_iter()
            .map(query_update_assignment_from_proto)
            .collect::<Result<_, _>>()?,
        predicate: value.predicate.map(query_expr_from_proto).transpose()?,
        returning: value
            .returning
            .map(|v| v.columns.into_iter().map(query_column_from_proto).collect()),
    })
}
fn query_delete_to_proto(value: QueryDelete) -> proto::QueryDelete {
    proto::QueryDelete {
        from: Some(query_from_to_proto(value.from)),
        predicate: value.predicate.map(query_expr_to_proto),
        returning: value.returning.map(|columns| proto::ReturningColumns {
            columns: columns.into_iter().map(query_column_to_proto).collect(),
        }),
    }
}
fn query_delete_from_proto(value: proto::QueryDelete) -> Result<QueryDelete, QueryServiceError> {
    Ok(QueryDelete {
        from: query_from_from_proto(value.from.ok_or_else(|| invalid("QueryDelete.from"))?)?,
        predicate: value.predicate.map(query_expr_from_proto).transpose()?,
        returning: value
            .returning
            .map(|v| v.columns.into_iter().map(query_column_from_proto).collect()),
    })
}

pub fn statement_to_proto(value: Statement) -> proto::Statement {
    use proto::statement::Kind as K;
    proto::Statement {
        kind: Some(match value {
            Statement::Query(value) => K::Query(query_to_proto(value)),
            Statement::DataDefinition(value) => K::DataDefinition(data_definition_to_proto(value)),
        }),
    }
}
pub fn statement_from_proto(value: proto::Statement) -> Result<Statement, QueryServiceError> {
    use proto::statement::Kind as K;
    Ok(match value.kind.ok_or_else(|| invalid("Statement.kind"))? {
        K::Query(value) => Statement::Query(query_from_proto(value)?),
        K::DataDefinition(value) => Statement::DataDefinition(data_definition_from_proto(value)?),
    })
}
fn data_definition_to_proto(value: DataDefinition) -> proto::DataDefinition {
    use proto::data_definition::Kind as K;
    proto::DataDefinition {
        kind: Some(match value {
            DataDefinition::CreateTable {
                schema,
                if_not_exists,
            } => K::CreateTable(proto::CreateTable {
                schema: Some(table_to_proto(schema)),
                if_not_exists,
            }),
            DataDefinition::CreateTableWithIndexes {
                schema,
                indexes,
                if_not_exists,
            } => K::CreateTableWithIndexes(proto::CreateTableWithIndexes {
                schema: Some(table_to_proto(schema)),
                indexes: indexes.into_iter().map(index_to_proto).collect(),
                if_not_exists,
            }),
            DataDefinition::AlterTable {
                table_name,
                operations,
                if_exists,
            } => K::AlterTable(proto::AlterTable {
                table_name,
                operations: operations
                    .into_iter()
                    .map(alter_table_operation_to_proto)
                    .collect(),
                if_exists,
            }),
            DataDefinition::DropTable {
                table_name,
                if_exists,
            } => K::DropTable(proto::DropTable {
                table_name,
                if_exists,
            }),
            DataDefinition::CreateIndex {
                schema,
                if_not_exists,
            } => K::CreateIndex(proto::CreateIndex {
                schema: Some(index_to_proto(schema)),
                if_not_exists,
            }),
            DataDefinition::CreateIndexUnresolved {
                index_name,
                table_name,
                column_names,
                unique,
                if_not_exists,
            } => K::CreateIndexUnresolved(proto::CreateIndexUnresolved {
                index_name,
                table_name,
                column_names,
                unique,
                if_not_exists,
            }),
            DataDefinition::AlterIndex {
                index_name,
                operation,
                if_exists,
            } => K::AlterIndex(proto::AlterIndex {
                index_name,
                operation: Some(alter_index_operation_to_proto(operation)),
                if_exists,
            }),
            DataDefinition::DropIndex {
                index_name,
                if_exists,
            } => K::DropIndex(proto::DropIndex {
                index_name,
                if_exists,
            }),
        }),
    }
}
fn data_definition_from_proto(
    value: proto::DataDefinition,
) -> Result<DataDefinition, QueryServiceError> {
    use proto::data_definition::Kind as K;
    Ok(
        match value.kind.ok_or_else(|| invalid("DataDefinition.kind"))? {
            K::CreateTable(v) => DataDefinition::CreateTable {
                schema: table_from_proto(v.schema.ok_or_else(|| invalid("CreateTable.schema"))?)?,
                if_not_exists: v.if_not_exists,
            },
            K::CreateTableWithIndexes(v) => DataDefinition::CreateTableWithIndexes {
                schema: table_from_proto(
                    v.schema
                        .ok_or_else(|| invalid("CreateTableWithIndexes.schema"))?,
                )?,
                indexes: v.indexes.into_iter().map(index_from_proto).collect(),
                if_not_exists: v.if_not_exists,
            },
            K::AlterTable(v) => DataDefinition::AlterTable {
                table_name: v.table_name,
                operations: v
                    .operations
                    .into_iter()
                    .map(alter_table_operation_from_proto)
                    .collect::<Result<_, _>>()?,
                if_exists: v.if_exists,
            },
            K::DropTable(v) => DataDefinition::DropTable {
                table_name: v.table_name,
                if_exists: v.if_exists,
            },
            K::CreateIndex(v) => DataDefinition::CreateIndex {
                schema: index_from_proto(v.schema.ok_or_else(|| invalid("CreateIndex.schema"))?),
                if_not_exists: v.if_not_exists,
            },
            K::CreateIndexUnresolved(v) => DataDefinition::CreateIndexUnresolved {
                index_name: v.index_name,
                table_name: v.table_name,
                column_names: v.column_names,
                unique: v.unique,
                if_not_exists: v.if_not_exists,
            },
            K::AlterIndex(v) => DataDefinition::AlterIndex {
                index_name: v.index_name,
                operation: alter_index_operation_from_proto(
                    v.operation.ok_or_else(|| invalid("AlterIndex.operation"))?,
                )?,
                if_exists: v.if_exists,
            },
            K::DropIndex(v) => DataDefinition::DropIndex {
                index_name: v.index_name,
                if_exists: v.if_exists,
            },
        },
    )
}
fn alter_table_operation_to_proto(value: AlterTableOperation) -> proto::AlterTableOperation {
    use proto::alter_table_operation::Kind as K;
    proto::AlterTableOperation {
        kind: Some(match value {
            AlterTableOperation::AddColumn(v) => K::AddColumn(column_to_proto(v)),
            AlterTableOperation::DropColumn(v) => K::DropColumn(v),
            AlterTableOperation::RenameColumn { old_name, new_name } => {
                K::RenameColumn(proto::RenameColumn { old_name, new_name })
            }
            AlterTableOperation::RenameTable { new_name } => {
                K::RenameTable(proto::RenameTable { new_name })
            }
            AlterTableOperation::AddIndex(v) => K::AddIndex(index_to_proto(v)),
            AlterTableOperation::RenameIndex { old_name, new_name } => {
                K::RenameIndex(proto::RenameIndex { old_name, new_name })
            }
            AlterTableOperation::DropIndex(v) => K::DropIndex(v),
        }),
    }
}
fn alter_table_operation_from_proto(
    value: proto::AlterTableOperation,
) -> Result<AlterTableOperation, QueryServiceError> {
    use proto::alter_table_operation::Kind as K;
    Ok(
        match value
            .kind
            .ok_or_else(|| invalid("AlterTableOperation.kind"))?
        {
            K::AddColumn(v) => AlterTableOperation::AddColumn(column_from_proto(v)?),
            K::DropColumn(v) => AlterTableOperation::DropColumn(v),
            K::RenameColumn(v) => AlterTableOperation::RenameColumn {
                old_name: v.old_name,
                new_name: v.new_name,
            },
            K::RenameTable(v) => AlterTableOperation::RenameTable {
                new_name: v.new_name,
            },
            K::AddIndex(v) => AlterTableOperation::AddIndex(index_from_proto(v)),
            K::RenameIndex(v) => AlterTableOperation::RenameIndex {
                old_name: v.old_name,
                new_name: v.new_name,
            },
            K::DropIndex(v) => AlterTableOperation::DropIndex(v),
        },
    )
}
fn alter_index_operation_to_proto(value: AlterIndexOperation) -> proto::AlterIndexOperation {
    use proto::alter_index_operation::Kind as K;
    match value {
        AlterIndexOperation::Rename { new_name } => proto::AlterIndexOperation {
            kind: Some(K::Rename(proto::RenameIndexOp { new_name })),
        },
    }
}
fn alter_index_operation_from_proto(
    value: proto::AlterIndexOperation,
) -> Result<AlterIndexOperation, QueryServiceError> {
    use proto::alter_index_operation::Kind as K;
    match value
        .kind
        .ok_or_else(|| invalid("AlterIndexOperation.kind"))?
    {
        K::Rename(v) => Ok(AlterIndexOperation::Rename {
            new_name: v.new_name,
        }),
    }
}

fn invalid(value: &str) -> QueryServiceError {
    QueryServiceError::Invalid(alloc::format!("missing or invalid {value}"))
}
