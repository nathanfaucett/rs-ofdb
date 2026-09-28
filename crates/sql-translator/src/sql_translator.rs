#[cfg(not(feature = "std"))]
use alloc::{
    boxed::Box,
    collections::{BTreeMap, BTreeSet},
    format,
    string::{String, ToString},
    vec,
    vec::Vec,
};
#[cfg(feature = "std")]
use std::collections::{BTreeMap, BTreeSet};

use query::{
    AlterIndexOperation, AlterTableOperation, DataDefinition, Query, QueryAggregate, QueryColumn,
    QueryCountTarget, QueryDelete, QueryExpr, QueryExprValue, QueryFrom, QueryHavingCount,
    QueryHavingCountOperator, QueryInsertValue, QueryInsertValues, QueryJoin, QueryJoinKind,
    QueryOrderBy, QueryParams, QuerySelect, QuerySortDirection, QueryTextConcat, QueryUpdate,
    QueryUpdateAssignment, Statement, TranslateError, TranslateResult, Translator,
};

use schema::TableSchema;
use sqlparser::{
    ast::{
        self, Expr, JoinConstraint, JoinOperator, ObjectName, ObjectNamePart, SelectItem,
        TableFactor, TableObject, TableWithJoins,
    },
    dialect::PostgreSqlDialect,
    parser::Parser,
    tokenizer::{Token, Tokenizer},
};
use uuid::Uuid;
use value::{Value, ValueType};

#[derive(Debug, Clone, Copy)]
pub struct SqlTranslator;

struct ParameterBindings<'a> {
    params: Option<&'a QueryParams>,
    positional: usize,
    positional_used: BTreeSet<usize>,
    named_used: BTreeSet<String>,
    style: Option<bool>,
}

impl<'a> ParameterBindings<'a> {
    fn new(params: Option<&'a QueryParams>) -> Self {
        Self {
            params,
            positional: 0,
            positional_used: BTreeSet::new(),
            named_used: BTreeSet::new(),
            style: None,
        }
    }

    fn resolve(&mut self, placeholder: &str) -> TranslateResult<Value> {
        let indexed = placeholder
            .strip_prefix('$')
            .or_else(|| placeholder.strip_prefix('?'))
            .and_then(|value| value.parse::<usize>().ok());
        let is_named = indexed.is_none() && !placeholder.starts_with('?');
        if self.style.is_some_and(|style| style != is_named) {
            return Err(TranslateError::MixedPlaceholderStyles);
        }
        self.style = Some(is_named);
        match (self.params, is_named) {
            (Some(QueryParams::Positional(values)), false) => {
                let index = if let Some(index) = indexed {
                    let index = index.checked_sub(1).ok_or_else(|| {
                        TranslateError::custom("Invalid positional parameter index")
                    })?;
                    self.positional = self.positional.max(index + 1);
                    index
                } else {
                    let index = self.positional;
                    self.positional += 1;
                    index
                };
                self.positional_used.insert(index);
                values.get(index).cloned().ok_or_else(|| {
                    TranslateError::custom(format!("Missing positional parameter: {}", index + 1))
                })
            }
            (Some(QueryParams::Named(values)), true) => {
                let name = placeholder.trim_start_matches([':', '@', '$']).to_string();
                self.named_used.insert(name.clone());
                values
                    .get(&name)
                    .cloned()
                    .ok_or(TranslateError::MissingNamedParameter(name))
            }
            (None, _) => Err(TranslateError::custom(format!(
                "Missing parameter: {placeholder}"
            ))),
            _ => Err(TranslateError::custom(
                "Parameter style does not match placeholders",
            )),
        }
    }

    fn finish(self) -> TranslateResult<()> {
        match self.params {
            Some(QueryParams::Positional(values)) if self.style == Some(false) => {
                let used = self.positional_used.len();
                if values.len() != used {
                    return Err(TranslateError::custom(format!(
                        "Wrong parameter count: expected {used}, got {}",
                        values.len()
                    )));
                }
            }
            Some(QueryParams::Positional(values)) if !values.is_empty() => {
                return Err(TranslateError::custom(
                    "Parameters supplied but query has no positional placeholders",
                ));
            }
            Some(QueryParams::Named(values)) if self.style == Some(true) => {
                if self.named_used.len() != values.len() {
                    return Err(TranslateError::custom("Wrong named parameter count"));
                }
            }
            Some(QueryParams::Named(values)) if !values.is_empty() => {
                return Err(TranslateError::custom(
                    "Parameters supplied but query has no named placeholders",
                ));
            }
            _ => {}
        }
        Ok(())
    }
}

impl Translator for SqlTranslator {
    async fn translate_with_params(
        &self,
        query: &str,
        params: Option<&QueryParams>,
    ) -> TranslateResult<Vec<Statement>> {
        let dialect = PostgreSqlDialect {};
        let mut tokens = Tokenizer::new(&dialect, query)
            .tokenize_with_location()
            .map_err(TranslateError::custom)?;
        if tokens.iter().any(|token| {
            matches!(
                &token.token,
                Token::Word(word) if word.value.eq_ignore_ascii_case("AUTOINCREMENT")
            )
        }) {
            return Err(TranslateError::custom("AUTOINCREMENT is not supported"));
        }
        for token in &mut tokens {
            if matches!(token.token, Token::Question) {
                token.token = Token::Placeholder("?".into());
            }
        }
        let stmts = Parser::new(&dialect)
            .with_tokens_with_locations(tokens)
            .parse_statements()
            .map_err(TranslateError::custom)?;

        let mut bindings = ParameterBindings::new(params);
        let mut results = Vec::with_capacity(stmts.len());
        for stmt in stmts {
            results.push(translate_stmt(stmt, &mut bindings)?);
        }
        bindings.finish()?;
        Ok(results)
    }
}

fn lower_simple_cte(query: &mut ast::Query) -> TranslateResult<()> {
    let Some(with) = query.with.take() else {
        return Ok(());
    };
    if with.recursive || with.cte_tables.len() != 1 {
        return Err(TranslateError::custom(
            "Only one non-recursive CTE is supported",
        ));
    }
    let cte = with
        .cte_tables
        .into_iter()
        .next()
        .expect("one CTE was checked");
    if !cte.alias.columns.is_empty() {
        return Err(TranslateError::custom(
            "CTE column aliases are not supported",
        ));
    }
    let cte_query = *cte.query;
    if cte_query.with.is_some()
        || cte_query.order_by.is_some()
        || cte_query.limit_clause.is_some()
        || cte_query.fetch.is_some()
    {
        return Err(TranslateError::custom("Unsupported CTE query shape"));
    }
    let ast::SetExpr::Select(mut cte_select) = *cte_query.body else {
        return Err(TranslateError::custom("CTE must contain a SELECT"));
    };
    let [cte_from] = cte_select.from.as_slice() else {
        return Err(TranslateError::custom("CTE requires one source table"));
    };
    if !cte_from.joins.is_empty() {
        return Err(TranslateError::custom("CTE joins are not supported"));
    }
    let projected: Vec<&str> = cte_select
        .projection
        .iter()
        .map(|item| match item {
            SelectItem::UnnamedExpr(Expr::Identifier(identifier)) => Ok(identifier.value.as_str()),
            _ => Err(TranslateError::custom(
                "CTE projection must use plain columns",
            )),
        })
        .collect::<TranslateResult<_>>()?;
    let ast::SetExpr::Select(outer_select) = query.body.as_mut() else {
        return Err(TranslateError::custom("CTE must precede a SELECT"));
    };
    if outer_select.from.len() != 1 {
        return Err(TranslateError::custom("CTE query requires one source"));
    }
    let outer_from = &mut outer_select.from[0];
    if !matches!(&outer_from.relation, TableFactor::Table { name, .. } if object_name_to_string(name)? == cte.alias.name.value)
        || !outer_from.joins.is_empty()
    {
        return Err(TranslateError::custom(
            "CTE source must be its declared name",
        ));
    }
    for item in &outer_select.projection {
        if let SelectItem::UnnamedExpr(Expr::Identifier(identifier)) = item {
            if !projected.contains(&identifier.value.as_str()) {
                return Err(TranslateError::custom("CTE column is not projected"));
            }
        } else {
            return Err(TranslateError::custom(
                "CTE outer projection must use plain columns",
            ));
        }
    }
    outer_from.relation = cte_from.relation.clone();
    outer_select.selection = match (cte_select.selection.take(), outer_select.selection.take()) {
        (Some(left), Some(right)) => Some(Expr::BinaryOp {
            left: Box::new(left),
            op: ast::BinaryOperator::And,
            right: Box::new(right),
        }),
        (selection, None) | (None, selection) => selection,
    };
    Ok(())
}

fn table_name_to_string(name: &TableObject) -> TranslateResult<String> {
    match name {
        TableObject::TableName(object_name) => {
            if object_name.0.len() == 1 {
                object_name_part_to_string(&object_name.0[0])
            } else {
                Err(TranslateError::custom(format!(
                    "Unsupported table name with {} parts: {:?}",
                    object_name.0.len(),
                    object_name
                )))
            }
        }
        _ => Err(TranslateError::custom(format!(
            "Unsupported table object: {:?}",
            name
        ))),
    }
}

fn object_name_to_string(name: &ObjectName) -> TranslateResult<String> {
    if name.0.len() == 1 {
        object_name_part_to_string(&name.0[0])
    } else {
        Err(TranslateError::custom(format!(
            "Unsupported object name with {} parts: {:?}",
            name.0.len(),
            name
        )))
    }
}

fn object_name_part_to_string(part: &ObjectNamePart) -> TranslateResult<String> {
    match part {
        ObjectNamePart::Identifier(ident) => Ok(ident.value.clone()),
        ObjectNamePart::Function(_) => Err(TranslateError::custom(
            "Function names not supported in object names",
        )),
    }
}

fn translate_stmt(
    stmt: ast::Statement,
    bindings: &mut ParameterBindings<'_>,
) -> TranslateResult<Statement> {
    match stmt {
        ast::Statement::Query(query) => translate_query(*query, bindings),
        ast::Statement::Insert(insert) => translate_insert(insert, bindings),
        ast::Statement::Update(update) => translate_update(update, bindings),
        ast::Statement::Delete(delete) => translate_delete(delete, bindings),
        ast::Statement::CreateTable(create_table) => translate_create_table(create_table),
        ast::Statement::Drop {
            object_type,
            if_exists,
            names,
            ..
        } => translate_drop(object_type, if_exists, names),
        ast::Statement::AlterTable(alter_table) => translate_alter_table(alter_table),
        ast::Statement::CreateIndex(create_index) => translate_create_index(create_index),
        ast::Statement::AlterIndex { name, operation } => translate_alter_index(name, operation),
        _ => Err(TranslateError::custom(format!(
            "Unsupported SQL statement: {:?}",
            stmt
        ))),
    }
}

fn translate_query(
    mut query: ast::Query,
    bindings: &mut ParameterBindings<'_>,
) -> TranslateResult<Statement> {
    lower_simple_cte(&mut query)?;
    if !matches!(*query.body, ast::SetExpr::Select(_)) {
        return Err(TranslateError::custom(
            "Only plain SELECT queries supported",
        ));
    }
    if query.fetch.is_some() {
        return Err(TranslateError::custom("FETCH is not supported"));
    }

    let select = match *query.body {
        ast::SetExpr::Select(select) => select,
        _ => unreachable!(),
    };

    let distinct = match select.distinct {
        None | Some(ast::Distinct::All) => false,
        Some(ast::Distinct::Distinct) => true,
        Some(ast::Distinct::On(_)) => {
            return Err(TranslateError::custom("DISTINCT ON is not supported"));
        }
    };
    let group_by = match &select.group_by {
        ast::GroupByExpr::Expressions(expressions, modifiers) if modifiers.is_empty() => {
            expressions
                .iter()
                .map(|expression| match expression {
                    Expr::Identifier(identifier) => {
                        Ok(QueryColumn::new(String::new(), identifier.value.clone()))
                    }
                    _ => Err(TranslateError::custom(
                        "GROUP BY supports simple columns only",
                    )),
                })
                .collect::<TranslateResult<Vec<_>>>()?
        }
        ast::GroupByExpr::Expressions(_, _) => {
            return Err(TranslateError::custom(
                "GROUP BY modifiers are not supported",
            ));
        }
        _ => return Err(TranslateError::custom("GROUP BY ALL is not supported")),
    };
    let (from, aliases) = translate_from(&select.from)?;
    let mut text_concats = Vec::new();
    let (projection, aggregates) = if !group_by.is_empty() {
        if distinct || group_by.len() != 1 || select.projection.len() != 2 {
            return Err(TranslateError::custom(
                "Grouped SELECT supports one group column and one COUNT",
            ));
        }
        let SelectItem::UnnamedExpr(Expr::Function(function)) = &select.projection[1] else {
            return Err(TranslateError::custom(
                "Grouped SELECT requires COUNT as its final projection",
            ));
        };
        let group_projection = translate_projection(&aliases, &select.projection[..1])?;
        if group_projection[0].column != group_by[0].column {
            return Err(TranslateError::custom(
                "Grouped SELECT must project its group column first",
            ));
        }
        (group_projection, vec![translate_count(function)?])
    } else if let [SelectItem::UnnamedExpr(Expr::Function(function))] = select.projection.as_slice()
    {
        (vec![], vec![translate_count(function)?])
    } else {
        let mut projection = Vec::with_capacity(select.projection.len());
        for item in &select.projection {
            match item {
                SelectItem::ExprWithAlias { expr, alias } => {
                    if let Expr::BinaryOp {
                        left,
                        op: ast::BinaryOperator::StringConcat,
                        right,
                    } = expr
                    {
                        let (column, literal) = translate_text_concat(&aliases, left, right)?;
                        projection.push(column.clone());
                        text_concats.push(Some(QueryTextConcat {
                            column,
                            literal,
                            alias: alias.value.clone(),
                        }));
                    } else {
                        projection.push(translate_projection_expr(&aliases, expr)?);
                        text_concats.push(None);
                    }
                }
                _ => {
                    projection.push(match item {
                        SelectItem::UnnamedExpr(expr) => translate_projection_expr(&aliases, expr)?,
                        SelectItem::Wildcard(_) => QueryColumn::new(String::new(), "*".into()),
                        _ => return Err(TranslateError::custom("Unsupported projection item")),
                    });
                    text_concats.push(None);
                }
            }
        }
        (projection, vec![])
    };
    let predicate = select
        .selection
        .map(|e| translate_expr(&aliases, e, bindings))
        .transpose()?;
    let having = select.having.map(translate_having_count).transpose()?;
    if having.is_some() && group_by.is_empty() {
        return Err(TranslateError::custom("HAVING requires GROUP BY"));
    }

    let order_by = query
        .order_by
        .map(|order| match order.kind {
            ast::OrderByKind::Expressions(expressions) => expressions
                .into_iter()
                .map(|expression| {
                    if expression.options.nulls_first.is_some() || expression.with_fill.is_some() {
                        return Err(TranslateError::custom(
                            "ORDER BY NULLS and WITH FILL are not supported",
                        ));
                    }
                    let column = match expression.expr {
                        Expr::Identifier(identifier) => {
                            QueryColumn::new(String::new(), identifier.value)
                        }
                        Expr::CompoundIdentifier(parts) if parts.len() == 2 => {
                            let table = aliases
                                .get(&parts[0].value)
                                .cloned()
                                .unwrap_or_else(|| parts[0].value.clone());
                            QueryColumn::new(table, parts[1].value.clone())
                        }
                        _ => return Err(TranslateError::custom("ORDER BY requires a column")),
                    };
                    Ok(QueryOrderBy {
                        by: column,
                        direction: if expression.options.asc == Some(false) {
                            QuerySortDirection::Desc
                        } else {
                            QuerySortDirection::Asc
                        },
                    })
                })
                .collect::<TranslateResult<Vec<_>>>(),
            _ => Err(TranslateError::custom("ORDER BY ALL is not supported")),
        })
        .transpose()?
        .unwrap_or_default();

    let (limit, offset) = match query.limit_clause {
        None => (None, None),
        Some(ast::LimitClause::LimitOffset {
            limit,
            offset,
            limit_by,
        }) if limit_by.is_empty() => (
            limit.map(parse_nonnegative_integer).transpose()?,
            offset
                .map(|offset| parse_nonnegative_integer(offset.value))
                .transpose()?,
        ),
        Some(ast::LimitClause::OffsetCommaLimit { offset, limit }) => (
            Some(parse_nonnegative_integer(limit)?),
            Some(parse_nonnegative_integer(offset)?),
        ),
        _ => return Err(TranslateError::custom("Unsupported LIMIT clause")),
    };

    Ok(Statement::Query(Query::Select(QuerySelect {
        from,
        projection,
        text_concats,
        distinct,
        predicate,
        aggregates,
        group_by,
        order_by,
        limit,
        offset,
        having,
    })))
}

fn translate_having_count(expr: Expr) -> TranslateResult<QueryHavingCount> {
    let Expr::BinaryOp { left, op, right } = expr else {
        return Err(TranslateError::custom("Unsupported HAVING expression"));
    };
    let operator = match op {
        ast::BinaryOperator::Gt => QueryHavingCountOperator::GreaterThan,
        ast::BinaryOperator::GtEq => QueryHavingCountOperator::GreaterThanOrEquals,
        _ => return Err(TranslateError::custom("Unsupported HAVING operator")),
    };
    let Expr::Function(function) = *left else {
        return Err(TranslateError::custom("HAVING requires COUNT(*)"));
    };
    if translate_count(&function)? != QueryAggregate::Count(QueryCountTarget::AllRows) {
        return Err(TranslateError::custom("HAVING requires COUNT(*)"));
    }
    let Expr::Value(ast::ValueWithSpan {
        value: ast::Value::Number(value, _),
        ..
    }) = *right
    else {
        return Err(TranslateError::custom("HAVING requires an integer literal"));
    };
    let value = value
        .parse::<i64>()
        .map_err(|_| TranslateError::custom("Invalid HAVING count"))?;
    Ok(QueryHavingCount { operator, value })
}

fn translate_count(function: &ast::Function) -> TranslateResult<QueryAggregate> {
    if !function.name.to_string().eq_ignore_ascii_case("count")
        || function.filter.is_some()
        || function.over.is_some()
    {
        return Err(TranslateError::custom(
            "Only COUNT aggregates are supported",
        ));
    }
    let ast::FunctionArguments::List(arguments) = &function.args else {
        return Err(TranslateError::custom("COUNT requires an argument"));
    };
    if arguments.duplicate_treatment.is_some() || !arguments.clauses.is_empty() {
        return Err(TranslateError::custom("COUNT modifiers are not supported"));
    }
    match arguments.args.as_slice() {
        [ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Wildcard)] => {
            Ok(QueryAggregate::Count(QueryCountTarget::AllRows))
        }
        [ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(Expr::Identifier(column)))] => Ok(
            QueryAggregate::Count(QueryCountTarget::Single(column.value.clone())),
        ),
        [
            ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(Expr::CompoundIdentifier(parts))),
        ] if parts.len() == 2 => Ok(QueryAggregate::Count(QueryCountTarget::Single(
            parts[1].value.clone(),
        ))),
        _ => Err(TranslateError::custom(
            "COUNT supports * or one column identifier",
        )),
    }
}

fn parse_nonnegative_integer(expression: Expr) -> TranslateResult<usize> {
    match expression {
        Expr::Value(ast::ValueWithSpan {
            value: ast::Value::Number(value, _),
            ..
        }) => value
            .parse()
            .map_err(|_| TranslateError::custom("LIMIT/OFFSET must be a nonnegative integer")),
        _ => Err(TranslateError::custom(
            "LIMIT/OFFSET must be a nonnegative integer",
        )),
    }
}

fn translate_from(
    from: &[TableWithJoins],
) -> TranslateResult<(QueryFrom, BTreeMap<String, String>)> {
    if from.len() != 1 {
        return Err(TranslateError::custom(
            "Only one table in FROM is supported",
        ));
    }
    let twj = from
        .first()
        .ok_or(TranslateError::custom("Missing FROM clause"))?;
    let (table, alias) = translate_table_factor(
        &twj.relation,
        "Only simple table references supported in FROM",
    )?;
    let mut aliases = BTreeMap::new();
    insert_alias(&mut aliases, alias, &table);

    for join in &twj.joins {
        let (table, alias) =
            translate_table_factor(&join.relation, "Complex table in JOIN not supported")?;
        insert_alias(&mut aliases, alias, &table);
    }

    let joins = twj
        .joins
        .iter()
        .map(|join| translate_join(&aliases, join))
        .collect::<TranslateResult<Vec<_>>>()?;

    Ok((QueryFrom { table, joins }, aliases))
}

fn translate_table_factor(
    factor: &TableFactor,
    error: &'static str,
) -> TranslateResult<(String, Option<String>)> {
    match factor {
        TableFactor::Table { name, alias, .. } => Ok((
            object_name_to_string(name)?,
            alias.as_ref().map(|alias| alias.name.value.clone()),
        )),
        _ => Err(TranslateError::custom(error)),
    }
}

fn insert_alias(aliases: &mut BTreeMap<String, String>, alias: Option<String>, table: &str) {
    if let Some(alias) = alias {
        aliases.insert(alias, table.into());
    }
}

fn translate_join(
    aliases: &BTreeMap<String, String>,
    join: &ast::Join,
) -> TranslateResult<QueryJoin> {
    let (table, _) = translate_table_factor(&join.relation, "Complex table in JOIN not supported")?;
    let (kind, constraint) = match &join.join_operator {
        JoinOperator::Inner(constraint) => (QueryJoinKind::Inner, constraint),
        JoinOperator::Left(constraint) => (QueryJoinKind::Left, constraint),
        JoinOperator::Right(constraint) => (QueryJoinKind::Right, constraint),
        JoinOperator::FullOuter(constraint) => (QueryJoinKind::Full, constraint),
        _ => return Err(TranslateError::custom("Unsupported JOIN type")),
    };

    Ok(QueryJoin {
        kind,
        table,
        on: parse_join_constraint(aliases, constraint)?,
    })
}

fn parse_join_constraint(
    aliases: &BTreeMap<String, String>,
    constraint: &JoinConstraint,
) -> TranslateResult<QueryExpr> {
    match constraint {
        JoinConstraint::On(expr) => {
            translate_expr(aliases, expr.clone(), &mut ParameterBindings::new(None))
        }
        JoinConstraint::Using(_) => Err(TranslateError::custom("USING joins not yet supported")),
        JoinConstraint::Natural => Err(TranslateError::custom("NATURAL joins not yet supported")),
        JoinConstraint::None => Err(TranslateError::custom("JOIN without ON condition")),
    }
}

fn translate_projection(
    aliases: &BTreeMap<String, String>,
    projection: &[SelectItem],
) -> TranslateResult<Vec<QueryColumn>> {
    projection
        .iter()
        .map(|item| match item {
            SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => {
                translate_projection_expr(aliases, expr)
            }
            SelectItem::Wildcard(_) => Ok(QueryColumn::new("".to_string(), "*".to_string())),
            _ => Err(TranslateError::custom("Unsupported projection item")),
        })
        .collect()
}

fn translate_text_concat(
    aliases: &BTreeMap<String, String>,
    left: &Expr,
    right: &Expr,
) -> TranslateResult<(QueryColumn, String)> {
    let (column, literal) = match (left, right) {
        (Expr::Identifier(_) | Expr::CompoundIdentifier(_), Expr::Value(value)) => (left, value),
        (Expr::Value(value), Expr::Identifier(_) | Expr::CompoundIdentifier(_)) => (right, value),
        _ => {
            return Err(TranslateError::custom(
                "Text concatenation requires a text column and literal",
            ));
        }
    };
    let ast::Value::SingleQuotedString(literal) = &literal.value else {
        return Err(TranslateError::custom(
            "Text concatenation requires a text literal",
        ));
    };
    Ok((translate_projection_expr(aliases, column)?, literal.clone()))
}

fn translate_projection_expr(
    aliases: &BTreeMap<String, String>,
    expr: &Expr,
) -> TranslateResult<QueryColumn> {
    match expr {
        Expr::Identifier(ident) => Ok(QueryColumn::new("".to_string(), ident.value.clone())),
        Expr::CompoundIdentifier(idents) if idents.len() == 2 => Ok(QueryColumn::new(
            aliases
                .get(&idents[0].value)
                .cloned()
                .unwrap_or_else(|| idents[0].value.clone()),
            idents[1].value.clone(),
        )),
        _ => Err(TranslateError::custom(
            "SELECT expressions are not supported",
        )),
    }
}

fn translate_expr(
    aliases: &BTreeMap<String, String>,
    expr: Expr,
    bindings: &mut ParameterBindings<'_>,
) -> TranslateResult<QueryExpr> {
    match expr {
        Expr::BinaryOp { left, op, right } => {
            translate_binary_expr(aliases, *left, op, *right, bindings)
        }
        Expr::Identifier(ident) => Ok(column_expr("".into(), ident.value)),
        Expr::CompoundIdentifier(idents) if idents.len() == 2 => Ok(column_expr(
            aliases
                .get(&idents[0].value)
                .cloned()
                .unwrap_or_else(|| idents[0].value.clone()),
            idents[1].value.clone(),
        )),
        Expr::Nested(inner) => translate_expr(aliases, *inner, bindings),
        Expr::IsNull(inner) => Ok(QueryExpr::IsNull(Box::new(translate_expr(
            aliases, *inner, bindings,
        )?))),
        Expr::IsNotNull(inner) => Ok(QueryExpr::IsNotNull(Box::new(translate_expr(
            aliases, *inner, bindings,
        )?))),
        Expr::InList {
            expr,
            list,
            negated,
        } => Ok(QueryExpr::InList {
            expr: Box::new(translate_expr(aliases, *expr, bindings)?),
            list: list
                .into_iter()
                .map(|item| translate_expr(aliases, item, bindings))
                .collect::<TranslateResult<Vec<_>>>()?,
            negated,
        }),
        Expr::InSubquery {
            expr,
            subquery,
            negated,
        } => {
            let Statement::Query(Query::Select(subquery)) = translate_query(*subquery, bindings)?
            else {
                return Err(TranslateError::custom(
                    "IN requires a plain SELECT subquery",
                ));
            };
            if !subquery.from.joins.is_empty()
                || subquery.projection.len() != 1
                || subquery.distinct
                || !subquery.aggregates.is_empty()
                || !subquery.group_by.is_empty()
                || subquery.having.is_some()
                || !subquery.order_by.is_empty()
                || subquery.limit.is_some()
                || subquery.offset.is_some()
                || !matches!(subquery.projection[0], QueryColumn { ref column, .. } if column != "*")
            {
                return Err(TranslateError::custom("Unsupported IN subquery shape"));
            }
            Ok(QueryExpr::InSubquery {
                expr: Box::new(translate_expr(aliases, *expr, bindings)?),
                subquery: Box::new(subquery),
                negated,
            })
        }
        Expr::Like {
            negated,
            any: false,
            expr,
            pattern,
            escape_char: None,
        } => {
            let like = QueryExpr::Like {
                expr: Box::new(translate_expr(aliases, *expr, bindings)?),
                pattern: Box::new(translate_expr(aliases, *pattern, bindings)?),
            };
            Ok(if negated {
                QueryExpr::Not(Box::new(like))
            } else {
                like
            })
        }
        Expr::Value(ast::ValueWithSpan {
            value: ast::Value::Placeholder(placeholder),
            ..
        }) => Ok(QueryExpr::Value(QueryExprValue::Value(
            bindings.resolve(&placeholder)?,
        ))),
        Expr::Value(ast::ValueWithSpan { value, .. }) => Ok(QueryExpr::Value(
            QueryExprValue::Value(translate_value(value)?),
        )),
        Expr::Cast {
            kind: ast::CastKind::Cast,
            expr,
            data_type: ast::DataType::Uuid,
            array: false,
            format: None,
        } => {
            let QueryExpr::Value(QueryExprValue::Value(Value::Text(value))) =
                translate_expr(aliases, *expr, bindings)?
            else {
                return Err(TranslateError::custom("UUID casts require text values"));
            };
            let uuid = Uuid::parse_str(&value)
                .map_err(|_| TranslateError::custom("Invalid UUID literal"))?;
            Ok(QueryExpr::Value(QueryExprValue::Value(Value::Uuid(uuid))))
        }
        Expr::Cast { .. } => Err(TranslateError::custom("Unsupported cast")),
        _ => Err(TranslateError::custom(format!(
            "Unsupported expression: {:?}",
            expr
        ))),
    }
}

fn column_expr(table: String, column: String) -> QueryExpr {
    QueryExpr::Value(QueryExprValue::Column(QueryColumn::new(table, column)))
}

fn translate_binary_expr(
    aliases: &BTreeMap<String, String>,
    left: Expr,
    op: ast::BinaryOperator,
    right: Expr,
    bindings: &mut ParameterBindings<'_>,
) -> TranslateResult<QueryExpr> {
    let left = Box::new(translate_expr(aliases, left, bindings)?);
    let right = Box::new(translate_expr(aliases, right, bindings)?);

    match op {
        ast::BinaryOperator::Eq => Ok(QueryExpr::Equals(left, right)),
        ast::BinaryOperator::NotEq => Ok(QueryExpr::NotEquals(left, right)),
        ast::BinaryOperator::Lt => Ok(QueryExpr::LessThan(left, right)),
        ast::BinaryOperator::LtEq => Ok(QueryExpr::LessThanOrEquals(left, right)),
        ast::BinaryOperator::Gt => Ok(QueryExpr::GreaterThan(left, right)),
        ast::BinaryOperator::GtEq => Ok(QueryExpr::GreaterThanOrEquals(left, right)),
        ast::BinaryOperator::And => Ok(QueryExpr::And(left, right)),
        ast::BinaryOperator::Or => Ok(QueryExpr::Or(left, right)),
        _ => Err(TranslateError::custom(format!(
            "Unsupported binary operator: {:?}",
            op
        ))),
    }
}

fn translate_value(value: ast::Value) -> TranslateResult<Value> {
    match value {
        ast::Value::Number(number, _) if number.contains('.') => number
            .parse()
            .map(Value::Float)
            .map_err(|_| TranslateError::custom(format!("Invalid float literal: {}", number))),
        ast::Value::Number(number, _) => number
            .parse()
            .map(Value::Integer)
            .map_err(|_| TranslateError::custom(format!("Invalid integer literal: {}", number))),
        ast::Value::SingleQuotedString(value) => Ok(Value::Text(value)),
        ast::Value::HexStringLiteral(value) if value.len() % 2 == 0 => value
            .as_bytes()
            .as_chunks::<2>()
            .0
            .iter()
            .map(|pair| {
                let byte = core::str::from_utf8(pair)
                    .map_err(|_| TranslateError::custom("Invalid BLOB literal"))?;
                u8::from_str_radix(byte, 16)
                    .map_err(|_| TranslateError::custom("Invalid BLOB literal"))
            })
            .collect::<TranslateResult<Vec<_>>>()
            .map(Value::Blob),
        ast::Value::HexStringLiteral(_) => Err(TranslateError::custom("Invalid BLOB literal")),
        ast::Value::Boolean(value) => Ok(Value::Bool(value)),
        ast::Value::Null => Ok(Value::Null),
        value => Ok(Value::Text(format!("{:?}", value))),
    }
}

fn translate_insert(
    insert: ast::Insert,
    bindings: &mut ParameterBindings<'_>,
) -> TranslateResult<Statement> {
    let (on_conflict_do_nothing, on_conflict_do_update) = match insert.on {
        None => (None, None),
        Some(ast::OnInsert::OnConflict(ast::OnConflict {
            conflict_target: Some(ast::ConflictTarget::Columns(columns)),
            action: ast::OnConflictAction::DoNothing,
        })) if !columns.is_empty() => (
            Some(columns.into_iter().map(|column| column.value).collect()),
            None,
        ),
        Some(ast::OnInsert::OnConflict(ast::OnConflict {
            conflict_target: Some(ast::ConflictTarget::Columns(columns)),
            action: ast::OnConflictAction::DoUpdate(update),
        })) if !columns.is_empty() && update.selection.is_none() => {
            let target = columns.into_iter().map(|column| column.value).collect();
            let assignments = update
                .assignments
                .into_iter()
                .map(|assignment| {
                    let ast::AssignmentTarget::ColumnName(column) = assignment.target else {
                        return Err(TranslateError::custom(
                            "ON CONFLICT assignments require one column",
                        ));
                    };
                    let value = match assignment.value {
                        Expr::CompoundIdentifier(parts)
                            if parts.len() == 2
                                && parts[0].value.eq_ignore_ascii_case("excluded") =>
                        {
                            QueryExprValue::ExcludedColumn(parts[1].value.clone())
                        }
                        expr => {
                            let QueryExpr::Value(value) =
                                translate_expr(&BTreeMap::new(), expr, bindings)?
                            else {
                                return Err(TranslateError::custom(
                                    "ON CONFLICT assignments require values",
                                ));
                            };
                            value
                        }
                    };
                    Ok(QueryUpdateAssignment {
                        column: QueryColumn::new(String::new(), object_name_to_string(&column)?),
                        value,
                    })
                })
                .collect::<TranslateResult<Vec<_>>>()?;
            if assignments.is_empty() {
                return Err(TranslateError::custom(
                    "ON CONFLICT DO UPDATE requires an assignment",
                ));
            }
            (None, Some((target, assignments)))
        }
        _ => return Err(TranslateError::custom("Unsupported INSERT conflict clause")),
    };
    Ok(Statement::Query(Query::InsertValues(QueryInsertValues {
        table: table_name_to_string(&insert.table)?,
        columns: insert
            .columns
            .into_iter()
            .map(|column| object_name_to_string(&column))
            .collect::<TranslateResult<Vec<_>>>()?,
        rows: translate_insert_values(insert.source, bindings)?,
        returning: insert.returning.map(translate_returning).transpose()?,
        on_conflict_do_nothing,
        on_conflict_do_update,
    })))
}

fn translate_insert_values(
    source: Option<Box<ast::Query>>,
    bindings: &mut ParameterBindings<'_>,
) -> TranslateResult<Vec<Vec<QueryInsertValue>>> {
    let Some(source) = source else {
        return Ok(vec![]);
    };
    let ast::SetExpr::Values(values) = *source.body else {
        return Ok(vec![]);
    };
    values
        .rows
        .into_iter()
        .map(|row| {
            row.content
                .into_iter()
                .map(
                    |expr| match translate_expr(&BTreeMap::new(), expr, bindings)? {
                        QueryExpr::Value(QueryExprValue::Value(value)) => {
                            Ok(QueryInsertValue::Value(value))
                        }
                        QueryExpr::Value(QueryExprValue::Column(column))
                            if column.column == "DEFAULT" =>
                        {
                            Ok(QueryInsertValue::Default)
                        }
                        _ => Err(TranslateError::custom("Unsupported expression in VALUES")),
                    },
                )
                .collect()
        })
        .collect()
}

fn translate_returning(items: Vec<SelectItem>) -> TranslateResult<Vec<String>> {
    items.into_iter().map(translate_returning_item).collect()
}

fn translate_returning_item(item: SelectItem) -> TranslateResult<String> {
    match item {
        SelectItem::UnnamedExpr(Expr::Identifier(ident)) => Ok(ident.value),
        SelectItem::UnnamedExpr(Expr::CompoundIdentifier(idents)) if idents.len() == 1 => {
            Ok(idents[0].value.clone())
        }
        SelectItem::UnnamedExpr(Expr::CompoundIdentifier(idents)) => Err(TranslateError::custom(
            format!("Unsupported RETURNING identifier: {:?}", idents),
        )),
        SelectItem::UnnamedExpr(expr) => Err(TranslateError::custom(format!(
            "Unsupported RETURNING expression: {:?}",
            expr
        ))),
        SelectItem::Wildcard(_) => Ok("*".to_string()),
        _ => Err(TranslateError::custom("Unsupported RETURNING item")),
    }
}

fn translate_update(
    update: ast::Update,
    bindings: &mut ParameterBindings<'_>,
) -> TranslateResult<Statement> {
    let table = match &update.table.relation {
        TableFactor::Table { name, .. } => object_name_to_string(name)?,
        _ => {
            return Err(TranslateError::custom(
                "Only simple table references supported in UPDATE",
            ));
        }
    };

    let assignments = update
        .assignments
        .into_iter()
        .map(|assignment| {
            let ast::AssignmentTarget::ColumnName(column) = assignment.target else {
                return Err(TranslateError::custom(
                    "UPDATE assignments require one column",
                ));
            };
            let QueryExpr::Value(value) =
                translate_expr(&BTreeMap::new(), assignment.value, bindings)?
            else {
                return Err(TranslateError::custom("UPDATE assignments require a value"));
            };
            Ok(QueryUpdateAssignment {
                column: QueryColumn::new(table.clone(), object_name_to_string(&column)?),
                value,
            })
        })
        .collect::<TranslateResult<Vec<_>>>()?;

    let predicate = update
        .selection
        .map(|e| translate_expr(&BTreeMap::new(), e, bindings))
        .transpose()?;
    let returning = update
        .returning
        .map(translate_returning)
        .transpose()?
        .map(|columns| {
            columns
                .into_iter()
                .map(|column| QueryColumn::new(table.clone(), column))
                .collect()
        });

    Ok(Statement::Query(Query::Update(QueryUpdate {
        from: QueryFrom {
            table,
            joins: vec![], // TODO: handle joins in UPDATE
        },
        assignments,
        predicate,
        returning,
    })))
}

fn translate_delete(
    delete: ast::Delete,
    bindings: &mut ParameterBindings<'_>,
) -> TranslateResult<Statement> {
    let (ast::FromTable::WithFromKeyword(from) | ast::FromTable::WithoutKeyword(from)) =
        delete.from;
    if from.len() != 1 || !delete.tables.is_empty() {
        return Err(TranslateError::custom(
            "Only one table in DELETE is supported",
        ));
    }
    let TableFactor::Table { name, .. } = &from[0].relation else {
        return Err(TranslateError::custom(
            "Only simple table references supported in DELETE",
        ));
    };
    if !from[0].joins.is_empty() {
        return Err(TranslateError::custom("Joins in DELETE are not supported"));
    }
    let table = object_name_to_string(name)?;

    let predicate = delete
        .selection
        .map(|e| translate_expr(&BTreeMap::new(), e, bindings))
        .transpose()?;
    let returning = delete
        .returning
        .map(translate_returning)
        .transpose()?
        .map(|columns| {
            columns
                .into_iter()
                .map(|column| QueryColumn::new(table.clone(), column))
                .collect()
        });

    Ok(Statement::Query(Query::Delete(QueryDelete {
        from: QueryFrom {
            table,
            joins: vec![], // TODO: handle joins in DELETE
        },
        predicate,
        returning,
    })))
}

fn translate_column_data_type(data_type: &ast::DataType) -> TranslateResult<ValueType> {
    match data_type {
        ast::DataType::Char(_) | ast::DataType::Varchar(_) | ast::DataType::Text => {
            Ok(ValueType::Text)
        }
        ast::DataType::Int(_) | ast::DataType::Integer(_) | ast::DataType::BigInt(_) => {
            Ok(ValueType::Integer)
        }
        ast::DataType::Float(_) | ast::DataType::Double(_) => Ok(ValueType::Float),
        ast::DataType::Boolean => Ok(ValueType::Bool),
        ast::DataType::Blob(_) => Ok(ValueType::Blob),
        ast::DataType::Uuid => Ok(ValueType::Uuid),
        ast::DataType::JSON | ast::DataType::JSONB => Ok(ValueType::Json),
        _ => Err(TranslateError::custom(format!(
            "Unsupported column data type: {:?}",
            data_type
        ))),
    }
}

fn translate_column_schema(column: &ast::ColumnDef) -> TranslateResult<schema::ColumnSchema> {
    let default = column
        .options
        .iter()
        .find_map(|option| match &option.option {
            ast::ColumnOption::Default(expr) => Some(expr.clone()),
            _ => None,
        })
        .map(|expr| {
            match translate_expr(&BTreeMap::new(), expr, &mut ParameterBindings::new(None))? {
                QueryExpr::Value(QueryExprValue::Value(value)) => Ok(value),
                _ => Err(TranslateError::custom(
                    "Column default must be a literal value",
                )),
            }
        })
        .transpose()?
        .unwrap_or_default();

    Ok(schema::ColumnSchema {
        name: column.name.value.clone(),
        r#type: translate_column_data_type(&column.data_type)?,
        default,
        primary_key: column
            .options
            .iter()
            .any(|option| matches!(option.option, ast::ColumnOption::PrimaryKey(_))),
    })
}

fn unique_index_name(table: &str, columns: &[String], ordinal: usize) -> String {
    format!("{}_unique_{}_{}", table, ordinal, columns.join("_"))
}

fn unique_columns(columns: &[ast::IndexColumn]) -> TranslateResult<Vec<String>> {
    columns
        .iter()
        .map(|column| match &column.column.expr {
            Expr::Identifier(identifier)
                if column.operator_class.is_none()
                    && column.column.options.asc.is_none()
                    && column.column.options.nulls_first.is_none()
                    && column.column.with_fill.is_none() =>
            {
                Ok(identifier.value.clone())
            }
            _ => Err(TranslateError::custom(
                "UNIQUE constraints only support bare identifier columns",
            )),
        })
        .collect()
}

fn translate_create_table(create_table: ast::CreateTable) -> TranslateResult<Statement> {
    let table_name = object_name_to_string(&create_table.name)?;
    for column in &create_table.columns {
        if column.options.iter().any(|option| {
            matches!(
                option.option,
                ast::ColumnOption::ForeignKey(_) | ast::ColumnOption::Identity(_)
            ) || matches!(
                &option.option,
                ast::ColumnOption::DialectSpecific(tokens)
                    if tokens.iter().any(|token| matches!(
                        token,
                        Token::Word(word) if word.value.eq_ignore_ascii_case("AUTOINCREMENT")
                    ))
            )
        }) {
            return Err(TranslateError::custom(
                "Foreign keys and AUTOINCREMENT are not supported",
            ));
        }
    }
    if create_table
        .constraints
        .iter()
        .any(|constraint| matches!(constraint, ast::TableConstraint::ForeignKey(_)))
    {
        return Err(TranslateError::custom("Foreign keys are not supported"));
    }

    let columns = create_table
        .columns
        .iter()
        .map(translate_column_schema)
        .collect::<TranslateResult<Vec<_>>>()?;

    let schema = TableSchema {
        name: table_name,
        columns,
    };

    let mut indexes = Vec::new();
    for (ordinal, column) in create_table.columns.iter().enumerate() {
        if column
            .options
            .iter()
            .any(|option| matches!(option.option, ast::ColumnOption::Unique { .. }))
        {
            let columns = vec![column.name.value.clone()];
            let name = column.options.iter().find_map(|option| {
                let ast::ColumnOption::Unique(unique) = &option.option else {
                    return None;
                };
                unique
                    .name
                    .as_ref()
                    .or(unique.index_name.as_ref())
                    .map(|name| name.value.clone())
            });
            indexes.push(schema::IndexSchema {
                name: name.unwrap_or_else(|| unique_index_name(&schema.name, &columns, ordinal)),
                table_name: schema.name.clone(),
                column_indices: vec![ordinal as u32],
                unique: true,
            });
        }
    }
    for (ordinal, constraint) in create_table.constraints.iter().enumerate() {
        if let ast::TableConstraint::Unique(unique) = constraint {
            let names = unique_columns(&unique.columns)?;
            indexes.push(schema::IndexSchema {
                name: unique
                    .name
                    .as_ref()
                    .or(unique.index_name.as_ref())
                    .map(|name| name.value.clone())
                    .unwrap_or_else(|| unique_index_name(&schema.name, &names, ordinal)),
                table_name: schema.name.clone(),
                column_indices: names
                    .iter()
                    .map(|name| {
                        schema
                            .columns
                            .iter()
                            .position(|column| column.name == *name)
                            .map(|index| index as u32)
                            .ok_or(TranslateError::custom("UNIQUE column not found"))
                    })
                    .collect::<TranslateResult<Vec<_>>>()?,
                unique: true,
            });
        }
    }

    if indexes.is_empty() {
        Ok(Statement::DataDefinition(DataDefinition::CreateTable {
            schema,
            if_not_exists: create_table.if_not_exists,
        }))
    } else {
        Ok(Statement::DataDefinition(
            DataDefinition::CreateTableWithIndexes {
                schema,
                indexes,
                if_not_exists: create_table.if_not_exists,
            },
        ))
    }
}

fn translate_drop(
    object_type: ast::ObjectType,
    if_exists: bool,
    names: Vec<ast::ObjectName>,
) -> TranslateResult<Statement> {
    if names.is_empty() {
        return Err(TranslateError::custom("No object names in DROP"));
    }
    let name = object_name_to_string(&names[0])?;

    match object_type {
        ast::ObjectType::Table => Ok(Statement::DataDefinition(DataDefinition::DropTable {
            table_name: name,
            if_exists,
        })),
        ast::ObjectType::Index => Ok(Statement::DataDefinition(DataDefinition::DropIndex {
            index_name: name,
            if_exists,
        })),
        _ => Err(TranslateError::custom(format!(
            "DROP {:?} not supported",
            object_type
        ))),
    }
}

fn translate_alter_table(alter_table: ast::AlterTable) -> TranslateResult<Statement> {
    let table_name = object_name_to_string(&alter_table.name)?;
    let operations = alter_table
        .operations
        .iter()
        .map(|operation| match operation {
            ast::AlterTableOperation::AddColumn { column_def, .. } => Ok(
                AlterTableOperation::AddColumn(translate_column_schema(column_def)?),
            ),
            _ => Err(TranslateError::custom(
                "Only ALTER TABLE ADD COLUMN is supported",
            )),
        })
        .collect::<TranslateResult<Vec<_>>>()?;

    Ok(Statement::DataDefinition(DataDefinition::AlterTable {
        table_name,
        operations,
        if_exists: alter_table.if_exists,
    }))
}

fn translate_create_index(create_index: ast::CreateIndex) -> TranslateResult<Statement> {
    let index_name = create_index
        .name
        .as_ref()
        .ok_or(TranslateError::custom(
            "CREATE INDEX requires an index name",
        ))
        .and_then(object_name_to_string)?;
    let table_name = object_name_to_string(&create_index.table_name)?;
    let column_names = create_index
        .columns
        .iter()
        .map(|column| match &column.column.expr {
            Expr::Identifier(identifier)
                if column.operator_class.is_none()
                    && column.column.options.asc.is_none()
                    && column.column.options.nulls_first.is_none()
                    && column.column.with_fill.is_none() =>
            {
                Ok(identifier.value.clone())
            }
            _ => Err(TranslateError::custom(
                "CREATE INDEX only supports bare identifier columns",
            )),
        })
        .collect::<TranslateResult<Vec<_>>>()?;

    Ok(Statement::DataDefinition(
        DataDefinition::CreateIndexUnresolved {
            index_name,
            table_name,
            column_names,
            unique: create_index.unique,
            if_not_exists: create_index.if_not_exists,
        },
    ))
}

fn translate_alter_index(
    name: ast::ObjectName,
    operation: ast::AlterIndexOperation,
) -> TranslateResult<Statement> {
    let index_name = object_name_to_string(&name)?;

    let op = match operation {
        ast::AlterIndexOperation::RenameIndex { index_name } => AlterIndexOperation::Rename {
            new_name: object_name_to_string(&index_name)?,
        },
    };

    Ok(Statement::DataDefinition(DataDefinition::AlterIndex {
        index_name,
        operation: op,
        if_exists: false,
    }))
}

#[cfg(test)]
mod tests {
    use futures::executor::block_on;

    use super::*;

    #[test]
    fn translates_alter_table_add_column_with_default() {
        block_on(async {
            let statements = SqlTranslator
                .translate_with_params(
                    "ALTER TABLE users ADD COLUMN role TEXT DEFAULT 'member'",
                    None,
                )
                .await
                .unwrap();
            assert_eq!(
                statements,
                vec![Statement::DataDefinition(DataDefinition::AlterTable {
                    table_name: "users".into(),
                    operations: vec![AlterTableOperation::AddColumn(schema::ColumnSchema {
                        name: "role".into(),
                        r#type: ValueType::Text,
                        default: Value::from("member"),
                        primary_key: false,
                    })],
                    if_exists: false,
                })]
            );
        });
    }

    #[test]
    fn translates_create_index() {
        block_on(async {
            let statements = SqlTranslator
                .translate_with_params(
                    "CREATE UNIQUE INDEX IF NOT EXISTS users_email ON users (email)",
                    None,
                )
                .await
                .unwrap();
            assert_eq!(
                statements,
                vec![Statement::DataDefinition(
                    DataDefinition::CreateIndexUnresolved {
                        index_name: "users_email".into(),
                        table_name: "users".into(),
                        column_names: vec!["email".into()],
                        unique: true,
                        if_not_exists: true,
                    }
                )]
            );
        });
    }

    #[test]
    fn rejects_create_index_expressions() {
        block_on(async {
            let error = SqlTranslator
                .translate_with_params("CREATE INDEX users_email ON users (lower(email))", None)
                .await
                .unwrap_err();
            assert!(error.to_string().contains("bare identifier columns"));
        });
    }

    fn translate(sql: &str) -> Vec<Statement> {
        block_on(async {
            SqlTranslator
                .translate_with_params(sql, None)
                .await
                .unwrap()
        })
    }

    #[test]
    fn translates_remaining_statement_types() {
        assert!(matches!(
            translate("INSERT INTO users VALUES (1, 'Ada', TRUE, NULL) RETURNING id, *")[0],
            Statement::Query(Query::InsertValues(_))
        ));
        assert!(matches!(
            translate("UPDATE users SET name = 'Ada' WHERE id <> 1")[0],
            Statement::Query(Query::Update(_))
        ));
        assert!(matches!(
            translate("DELETE FROM users WHERE id <= 1")[..],
            [Statement::Query(Query::Delete(_))]
        ));
        assert!(matches!(
            translate("DROP INDEX IF EXISTS users_name")[0],
            Statement::DataDefinition(DataDefinition::DropIndex { .. })
        ));
        assert!(matches!(
            translate("ALTER INDEX users_name RENAME TO members_name")[0],
            Statement::DataDefinition(DataDefinition::AlterIndex { .. })
        ));
    }

    #[test]
    fn translates_uuid_casts() {
        let uuid = Uuid::parse_str("018f0f8e-7b6d-7c4a-8f12-123456789abc").unwrap();
        let statements = translate(
            "SELECT id FROM users WHERE id = CAST('018f0f8e-7b6d-7c4a-8f12-123456789abc' AS UUID)",
        );
        let Statement::Query(Query::Select(select)) = &statements[0] else {
            panic!("expected SELECT");
        };
        let Some(QueryExpr::Equals(_, right)) = &select.predicate else {
            panic!("expected equality predicate");
        };
        assert_eq!(
            right.as_ref(),
            &QueryExpr::Value(QueryExprValue::Value(Value::Uuid(uuid)))
        );
    }

    #[test]
    fn translates_insert_columns_and_defaults() {
        let statements =
            translate("INSERT INTO users (name, id) VALUES ('Ada', DEFAULT) RETURNING id, name");
        let Statement::Query(Query::InsertValues(insert)) = &statements[0] else {
            panic!("expected INSERT VALUES");
        };
        assert_eq!(insert.columns, vec!["name", "id"]);
        assert_eq!(
            insert.rows,
            vec![vec![
                QueryInsertValue::Value(Value::from("Ada")),
                QueryInsertValue::Default,
            ]]
        );
    }

    #[test]
    fn translates_only_do_nothing_conflicts_with_column_targets() {
        let statements = translate(
            "INSERT INTO users (uuid_primary_key) VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abc' AS UUID)) ON CONFLICT (uuid_primary_key) DO NOTHING",
        );
        let Statement::Query(Query::InsertValues(insert)) = &statements[0] else {
            panic!("expected INSERT VALUES");
        };
        assert_eq!(
            insert.on_conflict_do_nothing,
            Some(vec!["uuid_primary_key".into()])
        );

        let statements =
            translate("INSERT INTO users VALUES (1) ON CONFLICT (id) DO UPDATE SET name = 'Ada'");
        let Statement::Query(Query::InsertValues(insert)) = &statements[0] else {
            panic!("expected INSERT VALUES");
        };
        assert_eq!(insert.on_conflict_do_update.as_ref().unwrap().0, vec!["id"]);
        assert_eq!(insert.on_conflict_do_update.as_ref().unwrap().1.len(), 1);

        for sql in [
            "INSERT INTO users VALUES (1) ON CONFLICT DO NOTHING",
            "INSERT INTO users VALUES (1) ON CONFLICT ON CONSTRAINT users_pkey DO NOTHING",
        ] {
            assert!(
                block_on(async { SqlTranslator.translate_with_params(sql, None).await }).is_err(),
                "accepted unsupported clause: {sql}"
            );
        }
    }

    #[test]
    fn translates_multi_row_insert_values() {
        let statements = translate(
            "INSERT INTO users (id, name) VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abc' AS UUID), 'Ada'), (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abd' AS UUID), 'Lin')",
        );
        let Statement::Query(Query::InsertValues(insert)) = &statements[0] else {
            panic!("expected INSERT VALUES");
        };
        assert_eq!(insert.rows.len(), 2);
        assert_eq!(insert.rows[0].len(), 2);
        assert_eq!(insert.rows[1].len(), 2);
    }

    #[test]
    fn translates_unique_constraints_to_named_indexes() {
        let statements = translate(
            "CREATE TABLE users (id UUID PRIMARY KEY, email TEXT UNIQUE, UNIQUE (id, email))",
        );
        let Statement::DataDefinition(DataDefinition::CreateTableWithIndexes {
            schema,
            indexes,
            ..
        }) = &statements[0]
        else {
            panic!("expected table with indexes");
        };
        assert_eq!(schema.columns.len(), 2);
        assert_eq!(indexes.len(), 2);
        assert_eq!(indexes[0].column_indices, vec![1]);
        assert_eq!(indexes[1].column_indices, vec![0, 1]);
        assert!(indexes.iter().all(|index| index.unique));
    }

    #[test]
    fn rejects_invalid_uuid_casts() {
        let error = block_on(async {
            SqlTranslator
                .translate_with_params(
                    "SELECT id FROM users WHERE id = CAST('not-a-uuid' AS UUID)",
                    None,
                )
                .await
                .unwrap_err()
        });
        assert!(error.to_string().contains("Invalid UUID literal"));
    }

    #[test]
    fn translates_data_types_and_reports_unsupported_types() {
        assert!(matches!(
            translate(
                "CREATE TABLE values (\
                    a CHAR, b VARCHAR, c TEXT, d INT, e INTEGER, f BIGINT, \
                    g FLOAT, h DOUBLE, i BOOLEAN, j BLOB, k UUID, l JSON, m JSONB\
                )"
            )[0],
            Statement::DataDefinition(DataDefinition::CreateTable { .. })
        ));

        let error = block_on(async {
            SqlTranslator
                .translate_with_params("CREATE TABLE values (created DATE)", None)
                .await
                .unwrap_err()
        });
        assert!(error.to_string().contains("Unsupported column data type"));
    }

    #[test]
    fn translates_join_kinds_and_expression_variants() {
        for sql in [
            "SELECT * FROM users u LEFT JOIN posts p ON p.user_id = u.id",
            "SELECT * FROM users u RIGHT JOIN posts p ON p.user_id = u.id",
            "SELECT * FROM users u FULL OUTER JOIN posts p ON p.user_id = u.id",
            "SELECT id, u.id FROM users u WHERE id < 1 OR id <= 2 OR id <> 3 OR id >= 4 AND active = TRUE AND name = 'Ada' AND deleted_at IS NOT NULL",
        ] {
            assert!(matches!(
                translate(sql)[0],
                Statement::Query(Query::Select(_))
            ));
        }
        assert!(block_on(async {
            SqlTranslator
                .translate_with_params("SELECT id + 1 FROM users", None)
                .await
                .is_err()
        }));
    }

    #[test]
    fn test_simple_select() {
        block_on(async {
            let translator = SqlTranslator;
            let sql = "SELECT u.id as user_id, u.name as username, p.created_at FROM users as u INNER JOIN posts as p ON p.user_id = u.id WHERE u.age > 30 AND p.created_at IS NULL";
            let result = translator
                .translate_with_params(sql, None)
                .await
                .expect("Failed to translate SQL");
            assert_eq!(result.len(), 1);

            if let Statement::Query(Query::Select(select)) = &result[0] {
                assert_eq!(select.from.table, "users");

                assert_eq!(select.from.joins.len(), 1);
                let join = &select.from.joins[0];
                assert_eq!(join.kind, QueryJoinKind::Inner);
                assert_eq!(join.table, "posts");
                if let QueryExpr::Equals(left, right) = &join.on {
                    match (left.as_ref(), right.as_ref()) {
                        (
                            QueryExpr::Value(QueryExprValue::Column(left_col)),
                            QueryExpr::Value(QueryExprValue::Column(right_col)),
                        ) => {
                            assert_eq!(left_col.table, "posts");
                            assert_eq!(left_col.column, "user_id");
                            assert_eq!(right_col.table, "users");
                            assert_eq!(right_col.column, "id");
                        }
                        _ => panic!("Expected column equality in JOIN condition"),
                    }
                } else {
                    panic!("Expected equality in JOIN condition");
                }

                assert_eq!(select.projection.len(), 3);
                let user_id_col = &select.projection[0];
                assert_eq!(user_id_col.table, "users");
                assert_eq!(user_id_col.column, "id");
                let username_col = &select.projection[1];
                assert_eq!(username_col.table, "users");
                assert_eq!(username_col.column, "name");
                let created_at_col = &select.projection[2];
                assert_eq!(created_at_col.table, "posts");
                assert_eq!(created_at_col.column, "created_at");

                if let Some(QueryExpr::And(lhs, rhs)) = &select.predicate {
                    match lhs.as_ref() {
                        QueryExpr::GreaterThan(left, right) => {
                            match (left.as_ref(), right.as_ref()) {
                                (
                                    QueryExpr::Value(QueryExprValue::Column(col)),
                                    QueryExpr::Value(QueryExprValue::Value(val)),
                                ) => {
                                    assert_eq!(col.table, "users");
                                    assert_eq!(col.column, "age");
                                    assert_eq!(val, &Value::Integer(30));
                                }
                                _ => panic!("Expected column and value in age > 30 predicate"),
                            }
                        }
                        _ => panic!("Expected age > 30 in predicate"),
                    }
                    match rhs.as_ref() {
                        QueryExpr::IsNull(inner) => match inner.as_ref() {
                            QueryExpr::Value(QueryExprValue::Column(col)) => {
                                assert_eq!(col.table, "posts");
                                assert_eq!(col.column, "created_at");
                            }
                            _ => panic!("Expected column in p.created_at IS NULL predicate"),
                        },
                        _ => panic!("Expected p.created_at IS NULL in predicate"),
                    }
                } else {
                    panic!(
                        "Expected SELECT statement with predicate: {:?}",
                        select.predicate
                    );
                }
            } else {
                panic!("Expected SELECT statement: {:?}", result[0]);
            }
        });
    }
}
