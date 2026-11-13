use query::{
    DataDefinition, Query, QueryAggregate, QueryColumn, QueryCountTarget, QueryExpr,
    QueryExprValue, QueryHavingCountOperator, QueryInsertValue, QueryJoinKind, QuerySelect,
    QuerySortDirection, Statement,
};
use value::{Value, ValueType};

use crate::{QueryError, QueryErrorKind};

/// A PostgreSQL statement compiled from the shared query representation.
#[derive(Debug)]
pub struct PostgresStatement {
    pub sql: String,
    pub params: Vec<Value>,
}

impl PostgresStatement {
    pub fn translate(statement: &Statement) -> Result<Self, QueryError> {
        let mut output = Self {
            sql: String::new(),
            params: Vec::new(),
        };
        output.sql = match statement {
            Statement::Query(query) => output.query(query)?,
            Statement::DataDefinition(ddl) => output.ddl(ddl)?,
        };
        Ok(output)
    }

    fn bind(&mut self, value: &Value) -> Result<String, QueryError> {
        if matches!(value, Value::Type(_)) {
            return Err(unsupported("type values"));
        }
        self.params.push(value.clone());
        Ok(format!("${}", self.params.len()))
    }

    fn value(&mut self, value: &QueryExprValue) -> Result<String, QueryError> {
        match value {
            QueryExprValue::Value(value) => self.bind(value),
            QueryExprValue::Column(column) => Ok(column_sql(column)),
            QueryExprValue::ExcludedColumn(column) => Ok(format!("excluded.{}", ident(column))),
        }
    }

    fn expr(&mut self, expr: &QueryExpr) -> Result<String, QueryError> {
        let (left, op, right) = match expr {
            QueryExpr::Value(value) => return self.value(value),
            QueryExpr::Not(expr) => return Ok(format!("(NOT {})", self.expr(expr)?)),
            QueryExpr::IsNull(expr) => return Ok(format!("({} IS NULL)", self.expr(expr)?)),
            QueryExpr::IsNotNull(expr) => return Ok(format!("({} IS NOT NULL)", self.expr(expr)?)),
            QueryExpr::Exists(_) => return Err(unsupported("EXISTS expression")),
            QueryExpr::InList {
                expr,
                list,
                negated,
            } => {
                let expr = self.expr(expr)?;
                if list.is_empty() {
                    return Ok(if *negated { "TRUE" } else { "FALSE" }.into());
                }
                let list = list
                    .iter()
                    .map(|value| self.expr(value))
                    .collect::<Result<Vec<_>, _>>()?;
                return Ok(format!(
                    "({expr} {}IN ({}))",
                    if *negated { "NOT " } else { "" },
                    list.join(", ")
                ));
            }
            QueryExpr::InSubquery {
                expr,
                subquery,
                negated,
            } => {
                let expr = self.expr(expr)?;
                let select = self.select(subquery)?;
                return Ok(format!(
                    "({expr} {}IN ({select}))",
                    if *negated { "NOT " } else { "" }
                ));
            }
            QueryExpr::Like { expr, pattern } => (expr, "LIKE", pattern),
            QueryExpr::Equals(a, b) => (a, "=", b),
            QueryExpr::NotEquals(a, b) => (a, "<>", b),
            QueryExpr::LessThan(a, b) => (a, "<", b),
            QueryExpr::LessThanOrEquals(a, b) => (a, "<=", b),
            QueryExpr::GreaterThan(a, b) => (a, ">", b),
            QueryExpr::GreaterThanOrEquals(a, b) => (a, ">=", b),
            QueryExpr::And(a, b) => (a, "AND", b),
            QueryExpr::Or(a, b) => (a, "OR", b),
        };
        Ok(format!("({} {op} {})", self.expr(left)?, self.expr(right)?))
    }

    fn predicate(&mut self, predicate: &Option<QueryExpr>) -> Result<String, QueryError> {
        predicate
            .as_ref()
            .map(|p| self.expr(p).map(|sql| format!(" WHERE {sql}")))
            .unwrap_or_else(|| Ok(String::new()))
    }

    fn select(&mut self, select: &QuerySelect) -> Result<String, QueryError> {
        let mut projection = Vec::new();
        for (index, column) in select.projection.iter().enumerate() {
            if let Some(Some(concat)) = select.text_concats.get(index) {
                let literal = self.bind(&Value::Text(concat.literal.clone()))?;
                projection.push(format!(
                    "({} || {literal}) AS {}",
                    column_sql(&concat.column),
                    ident(&concat.alias)
                ));
            } else {
                projection.push(column_sql(column));
            }
        }
        for aggregate in &select.aggregates {
            projection.push(match aggregate {
                QueryAggregate::Count(target) => match target {
                    QueryCountTarget::AllRows => "COUNT(*)".into(),
                    QueryCountTarget::Single(name) => format!("COUNT({})", ident(name)),
                    QueryCountTarget::Distinct(name) => format!("COUNT(DISTINCT {})", ident(name)),
                    QueryCountTarget::DistinctMulti(names) => {
                        format!("COUNT(DISTINCT ({}))", names_sql(names))
                    }
                },
                QueryAggregate::Sum(c) => format!("SUM({})", column_sql(c)),
                QueryAggregate::Avg(c) => format!("AVG({})::DOUBLE PRECISION", column_sql(c)),
                QueryAggregate::Min(c) => format!("MIN({})", column_sql(c)),
                QueryAggregate::Max(c) => format!("MAX({})", column_sql(c)),
            });
        }
        if projection.is_empty() {
            projection.push("*".into());
        }
        let mut sql = format!(
            "SELECT {}{} FROM {}",
            if select.distinct { "DISTINCT " } else { "" },
            projection.join(", "),
            ident(&select.from.table)
        );
        for join in &select.from.joins {
            let kind = match join.kind {
                QueryJoinKind::Inner => "INNER",
                QueryJoinKind::Left => "LEFT",
                QueryJoinKind::Right => "RIGHT",
                QueryJoinKind::Full => "FULL",
            };
            sql.push_str(&format!(
                " {kind} JOIN {} ON {}",
                ident(&join.table),
                self.expr(&join.on)?
            ));
        }
        sql.push_str(&self.predicate(&select.predicate)?);
        if !select.group_by.is_empty() {
            sql.push_str(&format!(" GROUP BY {}", columns_sql(&select.group_by)));
        }
        if let Some(having) = &select.having {
            let op = match having.operator {
                QueryHavingCountOperator::GreaterThan => ">",
                QueryHavingCountOperator::GreaterThanOrEquals => ">=",
            };
            let value = self.bind(&Value::Integer(having.value))?;
            sql.push_str(&format!(" HAVING COUNT(*) {op} {value}"));
        }
        if !select.order_by.is_empty() {
            sql.push_str(&format!(
                " ORDER BY {}",
                select
                    .order_by
                    .iter()
                    .map(|order| format!(
                        "{} {}",
                        column_sql(&order.by),
                        match order.direction {
                            QuerySortDirection::Asc => "ASC",
                            QuerySortDirection::Desc => "DESC",
                        }
                    ))
                    .collect::<Vec<_>>()
                    .join(", ")
            ));
        }
        if let Some(limit) = select.limit {
            sql.push_str(&format!(" LIMIT {limit}"));
        }
        if let Some(offset) = select.offset {
            sql.push_str(&format!(" OFFSET {offset}"));
        }
        Ok(sql)
    }

    fn query(&mut self, query: &Query) -> Result<String, QueryError> {
        match query {
            Query::Select(select) => self.select(select),
            Query::Insert(insert) => {
                let values = insert
                    .row
                    .values
                    .iter()
                    .map(|value| self.bind(value))
                    .collect::<Result<Vec<_>, _>>()?;
                let mut sql = format!(
                    "INSERT INTO {} VALUES ({})",
                    ident(&insert.table),
                    values.join(", ")
                );
                returning_names(&mut sql, &insert.returning);
                Ok(sql)
            }
            Query::InsertValues(insert) => {
                let mut rows = Vec::new();
                for row in &insert.rows {
                    let values = row
                        .iter()
                        .map(|value| match value {
                            QueryInsertValue::Value(value) => self.bind(value),
                            QueryInsertValue::Default => Ok("DEFAULT".into()),
                        })
                        .collect::<Result<Vec<_>, _>>()?;
                    rows.push(format!("({})", values.join(", ")));
                }
                if rows.is_empty() {
                    return Err(unsupported("empty INSERT"));
                }
                let mut sql = format!(
                    "INSERT INTO {}{} VALUES {}",
                    ident(&insert.table),
                    if insert.columns.is_empty() {
                        String::new()
                    } else {
                        format!(" ({})", names_sql(&insert.columns))
                    },
                    rows.join(", ")
                );
                if let Some(target) = &insert.on_conflict_do_nothing {
                    sql.push_str(" ON CONFLICT");
                    if !target.is_empty() {
                        sql.push_str(&format!(" ({})", names_sql(target)));
                    }
                    sql.push_str(" DO NOTHING");
                }
                if let Some((target, assignments)) = &insert.on_conflict_do_update {
                    let updates = assignments
                        .iter()
                        .map(|a| {
                            Ok(format!(
                                "{} = {}",
                                ident(&a.column.column),
                                self.value(&a.value)?
                            ))
                        })
                        .collect::<Result<Vec<_>, QueryError>>()?;
                    sql.push_str(&format!(
                        " ON CONFLICT ({}) DO UPDATE SET {}",
                        names_sql(target),
                        updates.join(", ")
                    ));
                }
                returning_names(&mut sql, &insert.returning);
                Ok(sql)
            }
            Query::Update(update) => {
                if !update.from.joins.is_empty() {
                    return Err(unsupported("UPDATE joins"));
                }
                let assignments = update
                    .assignments
                    .iter()
                    .map(|a| {
                        Ok(format!(
                            "{} = {}",
                            ident(&a.column.column),
                            self.value(&a.value)?
                        ))
                    })
                    .collect::<Result<Vec<_>, QueryError>>()?;
                let mut sql = format!(
                    "UPDATE {} SET {}{}",
                    ident(&update.from.table),
                    assignments.join(", "),
                    self.predicate(&update.predicate)?
                );
                returning_columns(&mut sql, &update.returning);
                Ok(sql)
            }
            Query::Delete(delete) => {
                if !delete.from.joins.is_empty() {
                    return Err(unsupported("DELETE joins"));
                }
                let mut sql = format!(
                    "DELETE FROM {}{}",
                    ident(&delete.from.table),
                    self.predicate(&delete.predicate)?
                );
                returning_columns(&mut sql, &delete.returning);
                Ok(sql)
            }
        }
    }

    fn ddl(&mut self, ddl: &DataDefinition) -> Result<String, QueryError> {
        match ddl {
            DataDefinition::CreateTable {
                schema,
                if_not_exists,
            }
            | DataDefinition::CreateTableWithIndexes {
                schema,
                indexes: _,
                if_not_exists,
            } => {
                if let DataDefinition::CreateTableWithIndexes { indexes, .. } = ddl
                    && !indexes.is_empty()
                {
                    return Err(unsupported("inline secondary indexes"));
                }
                let mut columns = Vec::new();
                let mut keys = Vec::new();
                for column in &schema.columns {
                    let kind = match column.r#type {
                        ValueType::Uuid => "UUID",
                        ValueType::Bool => "BOOLEAN",
                        ValueType::Integer => "BIGINT",
                        ValueType::Float => "DOUBLE PRECISION",
                        ValueType::Text => "TEXT",
                        ValueType::Blob => "BYTEA",
                        ValueType::Json => "JSONB",
                        _ => return Err(unsupported("column type")),
                    };
                    let mut sql = format!("{} {kind}", ident(&column.name));
                    if column.default != Value::Null {
                        sql.push_str(&format!(" DEFAULT {}", default_sql(&column.default)?));
                    }
                    columns.push(sql);
                    if column.primary_key {
                        keys.push(column.name.clone());
                    }
                }
                if !keys.is_empty() {
                    columns.push(format!("PRIMARY KEY ({})", names_sql(&keys)));
                }
                Ok(format!(
                    "CREATE TABLE {}{} ({})",
                    if *if_not_exists { "IF NOT EXISTS " } else { "" },
                    ident(&schema.name),
                    columns.join(", ")
                ))
            }
            DataDefinition::CreateIndexUnresolved {
                index_name,
                table_name,
                column_names,
                unique,
                if_not_exists,
            } => Ok(format!(
                "CREATE {}INDEX {}{} ON {} ({})",
                if *unique { "UNIQUE " } else { "" },
                if *if_not_exists { "IF NOT EXISTS " } else { "" },
                ident(index_name),
                ident(table_name),
                names_sql(column_names)
            )),
            DataDefinition::DropTable {
                table_name,
                if_exists,
            } => Ok(format!(
                "DROP TABLE {}{}",
                if *if_exists { "IF EXISTS " } else { "" },
                ident(table_name)
            )),
            DataDefinition::DropIndex {
                index_name,
                if_exists,
            } => Ok(format!(
                "DROP INDEX {}{}",
                if *if_exists { "IF EXISTS " } else { "" },
                ident(index_name)
            )),
            _ => Err(unsupported("schema operation")),
        }
    }
}

fn ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}
fn column_sql(column: &QueryColumn) -> String {
    let name = if column.column == "*" {
        "*".into()
    } else {
        ident(&column.column)
    };
    if column.table.is_empty() {
        name
    } else {
        format!("{}.{name}", ident(&column.table))
    }
}
fn names_sql(names: &[String]) -> String {
    names
        .iter()
        .map(|name| if name == "*" { "*".into() } else { ident(name) })
        .collect::<Vec<_>>()
        .join(", ")
}
fn columns_sql(columns: &[QueryColumn]) -> String {
    columns
        .iter()
        .map(column_sql)
        .collect::<Vec<_>>()
        .join(", ")
}
fn returning_names(sql: &mut String, columns: &Option<Vec<String>>) {
    if let Some(columns) = columns {
        sql.push_str(&format!(" RETURNING {}", names_sql(columns)));
    }
}
fn returning_columns(sql: &mut String, columns: &Option<Vec<QueryColumn>>) {
    if let Some(columns) = columns {
        sql.push_str(&format!(" RETURNING {}", columns_sql(columns)));
    }
}
fn default_sql(value: &Value) -> Result<String, QueryError> {
    match value {
        Value::Bool(value) => Ok(if *value { "TRUE" } else { "FALSE" }.into()),
        Value::Integer(value) => Ok(value.to_string()),
        Value::Float(value) if value.is_finite() => Ok(value.to_string()),
        Value::Text(value) => Ok(format!("'{}'", value.replace('\'', "''"))),
        _ => Err(unsupported("column default")),
    }
}

pub fn unsupported(operation: &str) -> QueryError {
    QueryError::new(
        QueryErrorKind::Unsupported,
        format!("PostgreSQL does not support ofdb {operation}"),
    )
}
