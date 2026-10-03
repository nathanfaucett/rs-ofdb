use std::collections::BTreeMap;

use proto::{JsonNumber, JsonObject, JsonObjectEntry, JsonValue, Value, json_value};
use query::{
    AlterIndexOperation, AlterTableOperation, DataDefinition, Query, QueryAggregate, QueryColumn,
    QueryCountTarget, QueryExpr, QueryExprValue, QueryFrom, QueryHavingCount,
    QueryHavingCountOperator, QueryInsert, QueryInsertValue, QueryInsertValues, QueryJoin,
    QueryJoinKind, QueryOrderBy, QueryResult, QueryResultColumn, QuerySelect, QuerySortDirection,
    QueryUpdateAssignment, Statement,
};
use schema::{ColumnSchema, IndexSchema, TableSchema};
use value::{JsonNumber as DomainJsonNumber, JsonValue as DomainJsonValue, ValueType};

use crate::{
    QueryServiceError, column_from_proto, column_to_proto, query_expr_from_proto,
    query_expr_to_proto, query_expr_value_from_proto, query_expr_value_to_proto, query_from_proto,
    query_result_from_proto, query_result_to_proto, query_to_proto, statement_from_proto,
    statement_to_proto, value_from_proto, value_to_proto,
};

#[test]
fn value_and_nested_json_round_trip() {
    let mut object = BTreeMap::new();
    object.insert(
        "items".into(),
        DomainJsonValue::Array(vec![
            DomainJsonValue::Number(DomainJsonNumber::U64(u64::MAX)),
            DomainJsonValue::Null,
        ]),
    );
    let expected = DomainJsonValue::Object(object);
    let encoded = value_to_proto(value::Value::Json(expected.clone()));

    assert!(matches!(
        value_from_proto(encoded),
        Ok(value::Value::Json(actual)) if actual == expected
    ));
}

#[test]
fn rejects_missing_value_variant() {
    assert_eq!(
        value_from_proto(Value { kind: None }),
        Err(QueryServiceError::Invalid("missing Value kind".into()))
    );
}

#[test]
fn rejects_invalid_uuid_length() {
    assert!(matches!(
        value_from_proto(Value {
            kind: Some(proto::value::Kind::Uuid(vec![1, 2, 3])),
        }),
        Err(QueryServiceError::Invalid(_))
    ));
}

#[test]
fn rejects_duplicate_json_object_keys() {
    let value = Value {
        kind: Some(proto::value::Kind::Json(JsonValue {
            kind: Some(json_value::Kind::Object(JsonObject {
                entries: vec![
                    JsonObjectEntry {
                        key: "same".into(),
                        value: Some(JsonValue {
                            kind: Some(json_value::Kind::Null(())),
                        }),
                    },
                    JsonObjectEntry {
                        key: "same".into(),
                        value: Some(JsonValue {
                            kind: Some(json_value::Kind::Null(())),
                        }),
                    },
                ],
            })),
        })),
    };

    assert!(matches!(
        value_from_proto(value),
        Err(QueryServiceError::Invalid(message)) if message == "duplicate JSON object key"
    ));
}

#[test]
fn rejects_unspecified_json_number() {
    let value = Value {
        kind: Some(proto::value::Kind::Json(JsonValue {
            kind: Some(json_value::Kind::Number(JsonNumber { kind: None })),
        })),
    };

    assert!(matches!(
        value_from_proto(value),
        Err(QueryServiceError::Invalid(message)) if message == "missing JsonNumber kind"
    ));
}

#[test]
fn converts_schema_column_losslessly() {
    for r#type in [
        ValueType::Null,
        ValueType::Type,
        ValueType::Uuid,
        ValueType::Bool,
        ValueType::Integer,
        ValueType::Float,
        ValueType::Text,
        ValueType::Json,
        ValueType::Blob,
    ] {
        let column = ColumnSchema {
            name: "id".into(),
            r#type,
            default: value::Value::Null,
            primary_key: true,
        };
        assert_eq!(
            column_from_proto(column_to_proto(column.clone())),
            Ok(column)
        );
    }
}

#[test]
fn value_types_round_trip_all_variants() {
    for value_type in [
        ValueType::Null,
        ValueType::Type,
        ValueType::Uuid,
        ValueType::Bool,
        ValueType::Integer,
        ValueType::Float,
        ValueType::Text,
        ValueType::Json,
        ValueType::Blob,
    ] {
        let encoded = value_to_proto(value::Value::Type(value_type));
        assert_eq!(
            value_from_proto(encoded),
            Ok(value::Value::Type(value_type))
        );
    }
}

#[test]
fn rejects_unspecified_column_type() {
    let column = proto::ColumnSchema {
        name: "id".into(),
        r#type: 0,
        default: Some(proto::Value {
            kind: Some(proto::value::Kind::Null(())),
        }),
        primary_key: false,
    };

    assert!(matches!(
        column_from_proto(column),
        Err(QueryServiceError::Invalid(message)) if message == "unspecified column ValueType"
    ));
}

#[test]
fn query_expression_values_round_trip_all_variants() {
    for value in [
        QueryExprValue::Column(QueryColumn {
            table: "items".into(),
            column: "id".into(),
        }),
        QueryExprValue::ExcludedColumn("id".into()),
        QueryExprValue::Value(value::Value::Integer(7)),
    ] {
        let encoded = query_expr_value_to_proto(value.clone());
        let decoded = query_expr_value_from_proto(encoded)
            .expect("query expression value conversion is lossless");
        match (value, decoded) {
            (QueryExprValue::Column(expected), QueryExprValue::Column(actual)) => {
                assert_eq!(expected, actual);
            }
            (QueryExprValue::ExcludedColumn(expected), QueryExprValue::ExcludedColumn(actual)) => {
                assert_eq!(expected, actual);
            }
            (
                QueryExprValue::Value(value::Value::Integer(expected)),
                QueryExprValue::Value(value::Value::Integer(actual)),
            ) => {
                assert_eq!(expected, actual);
            }
            _ => panic!("query expression value variant changed"),
        }
    }
}

#[test]
fn rejects_missing_query_expression_value_variant() {
    assert!(matches!(
        query_expr_value_from_proto(proto::QueryExprValue { kind: None }),
        Err(QueryServiceError::Invalid(message)) if message == "missing QueryExprValue kind"
    ));
}

#[test]
fn query_result_round_trips_rows_and_column_sources() {
    let result = QueryResult::new_with_columns(
        vec![value::Row::new(vec![value::Value::Integer(42)])],
        vec![QueryResultColumn {
            name: "answer".into(),
            source_table: Some("items".into()),
            source_column: Some("value".into()),
        }],
    );
    let decoded = query_result_from_proto(query_result_to_proto(result))
        .expect("query result conversion is lossless");

    assert!(matches!(
        decoded.rows[0].values[0],
        value::Value::Integer(42)
    ));
    assert_eq!(decoded.columns[0].name, "answer");
    assert_eq!(decoded.columns[0].source_table.as_deref(), Some("items"));
    assert_eq!(decoded.columns[0].source_column.as_deref(), Some("value"));
}

#[test]
fn rejects_result_row_with_missing_value_kind() {
    let result = proto::QueryResult {
        rows: vec![proto::Row {
            values: vec![proto::Value { kind: None }],
        }],
        columns: Vec::new(),
    };

    assert!(matches!(
        query_result_from_proto(result),
        Err(QueryServiceError::Invalid(message)) if message == "missing Value kind"
    ));
}

#[test]
fn query_expression_round_trips_every_variant() {
    let value = || {
        QueryExpr::Value(QueryExprValue::Column(QueryColumn {
            table: "items".into(),
            column: "id".into(),
        }))
    };
    let expressions = vec![
        value(),
        QueryExpr::Not(Box::new(value())),
        QueryExpr::Exists(Box::new(value())),
        QueryExpr::IsNull(Box::new(value())),
        QueryExpr::IsNotNull(Box::new(value())),
        QueryExpr::Equals(Box::new(value()), Box::new(value())),
        QueryExpr::NotEquals(Box::new(value()), Box::new(value())),
        QueryExpr::LessThan(Box::new(value()), Box::new(value())),
        QueryExpr::LessThanOrEquals(Box::new(value()), Box::new(value())),
        QueryExpr::GreaterThan(Box::new(value()), Box::new(value())),
        QueryExpr::GreaterThanOrEquals(Box::new(value()), Box::new(value())),
        QueryExpr::InList {
            expr: Box::new(value()),
            list: vec![value()],
            negated: true,
        },
        QueryExpr::InSubquery {
            expr: Box::new(value()),
            subquery: Box::new(QuerySelect::default()),
            negated: true,
        },
        QueryExpr::Like {
            expr: Box::new(value()),
            pattern: Box::new(value()),
        },
        QueryExpr::And(Box::new(value()), Box::new(value())),
        QueryExpr::Or(Box::new(value()), Box::new(value())),
    ];
    for expr in expressions {
        let decoded = query_expr_from_proto(query_expr_to_proto(expr.clone()))
            .expect("query expression should round-trip");
        assert_eq!(decoded, expr);
    }
}

#[test]
fn query_select_round_trips_through_query_oneof() {
    let col = |column: &str| QueryColumn {
        table: "items".into(),
        column: column.into(),
    };
    let select = QuerySelect {
        from: QueryFrom {
            table: "items".into(),
            joins: [
                QueryJoinKind::Inner,
                QueryJoinKind::Left,
                QueryJoinKind::Right,
                QueryJoinKind::Full,
            ]
            .into_iter()
            .map(|kind| QueryJoin {
                kind,
                table: "related".into(),
                on: QueryExpr::Value(QueryExprValue::Column(col("id"))),
            })
            .collect(),
        },
        projection: vec![col("id")],
        text_concats: Vec::new(),
        distinct: true,
        predicate: None,
        aggregates: vec![
            QueryAggregate::Count(QueryCountTarget::AllRows),
            QueryAggregate::Count(QueryCountTarget::Single("id".into())),
            QueryAggregate::Count(QueryCountTarget::Distinct("id".into())),
            QueryAggregate::Count(QueryCountTarget::DistinctMulti(vec![
                "id".into(),
                "name".into(),
            ])),
            QueryAggregate::Sum(col("id")),
            QueryAggregate::Avg(col("id")),
            QueryAggregate::Min(col("id")),
            QueryAggregate::Max(col("id")),
        ],
        group_by: vec![col("id")],
        order_by: vec![
            QueryOrderBy {
                by: col("id"),
                direction: QuerySortDirection::Asc,
            },
            QueryOrderBy {
                by: col("name"),
                direction: QuerySortDirection::Desc,
            },
        ],
        limit: Some(10),
        offset: Some(2),
        having: Some(QueryHavingCount {
            operator: QueryHavingCountOperator::GreaterThanOrEquals,
            value: 1,
        }),
    };
    let query = Query::Select(select);
    assert_eq!(
        query_from_proto(query_to_proto(query.clone())).expect("query round-trip"),
        query
    );
}

#[test]
fn statement_ddl_round_trips_every_variant() {
    let column = ColumnSchema {
        name: "id".into(),
        r#type: ValueType::Uuid,
        default: value::Value::Null,
        primary_key: true,
    };
    let table = TableSchema {
        name: "items".into(),
        columns: vec![column.clone()],
    };
    let index = IndexSchema {
        name: "items_id".into(),
        table_name: "items".into(),
        column_indices: vec![0],
        unique: true,
    };
    let definitions = vec![
        DataDefinition::CreateTable {
            schema: table.clone(),
            if_not_exists: true,
        },
        DataDefinition::CreateTableWithIndexes {
            schema: table,
            indexes: vec![index.clone()],
            if_not_exists: true,
        },
        DataDefinition::AlterTable {
            table_name: "items".into(),
            operations: vec![
                AlterTableOperation::AddColumn(column),
                AlterTableOperation::DropColumn("old".into()),
                AlterTableOperation::RenameColumn {
                    old_name: "old".into(),
                    new_name: "new".into(),
                },
                AlterTableOperation::RenameTable {
                    new_name: "products".into(),
                },
                AlterTableOperation::AddIndex(index.clone()),
                AlterTableOperation::RenameIndex {
                    old_name: "old_idx".into(),
                    new_name: "new_idx".into(),
                },
                AlterTableOperation::DropIndex("old_idx".into()),
            ],
            if_exists: true,
        },
        DataDefinition::DropTable {
            table_name: "items".into(),
            if_exists: true,
        },
        DataDefinition::CreateIndex {
            schema: index,
            if_not_exists: true,
        },
        DataDefinition::CreateIndexUnresolved {
            index_name: "items_id".into(),
            table_name: "items".into(),
            column_names: vec!["id".into()],
            unique: true,
            if_not_exists: true,
        },
        DataDefinition::AlterIndex {
            index_name: "items_id".into(),
            operation: AlterIndexOperation::Rename {
                new_name: "products_id".into(),
            },
            if_exists: true,
        },
        DataDefinition::DropIndex {
            index_name: "items_id".into(),
            if_exists: true,
        },
    ];
    for definition in definitions {
        let statement = Statement::DataDefinition(definition);
        assert_eq!(
            statement_from_proto(statement_to_proto(statement.clone()))
                .expect("statement round-trip"),
            statement
        );
    }
}

#[test]
fn insert_values_preserve_conflict_actions() {
    let query = Query::InsertValues(QueryInsertValues {
        table: "items".into(),
        columns: vec!["id".into(), "name".into()],
        rows: vec![vec![
            QueryInsertValue::Default,
            QueryInsertValue::Value(value::Value::Text("item".into())),
        ]],
        returning: Some(vec!["id".into()]),
        on_conflict_do_nothing: Some(vec!["id".into()]),
        on_conflict_do_update: Some((
            vec!["id".into()],
            vec![QueryUpdateAssignment {
                column: QueryColumn {
                    table: "items".into(),
                    column: "name".into(),
                },
                value: QueryExprValue::ExcludedColumn("name".into()),
            }],
        )),
    });
    let statement = Statement::Query(query);
    assert_eq!(
        statement_from_proto(statement_to_proto(statement.clone())).expect("insert round-trip"),
        statement
    );
}

#[test]
fn insert_returning_preserves_absent_and_empty() {
    for returning in [None, Some(Vec::new())] {
        let statement = Statement::Query(Query::Insert(QueryInsert {
            table: "items".into(),
            row: value::Row { values: Vec::new() },
            returning: returning.clone(),
        }));
        let decoded = statement_from_proto(statement_to_proto(statement))
            .expect("insert statement should round-trip");
        assert!(
            matches!(decoded, Statement::Query(Query::Insert(insert)) if insert.returning == returning)
        );
    }
}

#[test]
fn rejects_query_expression_without_kind() {
    assert!(matches!(
        query_expr_from_proto(proto::QueryExpr { kind: None }),
        Err(QueryServiceError::Invalid(message)) if message == "missing or invalid QueryExpr kind"
    ));
}

#[test]
fn converts_each_json_number_kind() {
    for number in [
        DomainJsonNumber::I64(i64::MIN),
        DomainJsonNumber::U64(u64::MAX),
        DomainJsonNumber::F64(1.5),
    ] {
        let expected = DomainJsonValue::Number(number);
        assert!(matches!(
            value_from_proto(value_to_proto(value::Value::Json(expected.clone()))),
            Ok(value::Value::Json(actual)) if actual == expected
        ));
    }
}
