use prost::Message;

use proto::{
    DataDefinition, QueryExpr, QueryExprValue, Statement, Value, data_definition, query_expr,
    query_expr_value, value,
};

#[test]
fn value_integer_v1_wire_fixture() {
    let value = Value {
        kind: Some(value::Kind::Integer(42)),
    };
    let fixture = [0x28, 0x2a];

    assert_eq!(value.encode_to_vec(), fixture);
    assert_eq!(
        Value::decode(fixture.as_slice()).expect("valid Value fixture"),
        value
    );
}

#[test]
fn query_expr_value_v1_wire_fixture() {
    let expr = QueryExpr {
        kind: Some(query_expr::Kind::Value(QueryExprValue {
            kind: Some(query_expr_value::Kind::Value(Value {
                kind: Some(value::Kind::Integer(42)),
            })),
        })),
    };
    let fixture = [0x0a, 0x04, 0x12, 0x02, 0x28, 0x2a];

    assert_eq!(expr.encode_to_vec(), fixture);
    assert_eq!(
        QueryExpr::decode(fixture.as_slice()).expect("valid query fixture"),
        expr
    );
}

#[test]
fn statement_drop_table_v1_wire_fixture() {
    let statement = Statement {
        kind: Some(proto::statement::Kind::DataDefinition(DataDefinition {
            kind: Some(data_definition::Kind::DropTable(proto::DropTable {
                table_name: "t".into(),
                if_exists: true,
            })),
        })),
    };
    let fixture = [0x12, 0x07, 0x22, 0x05, 0x0a, 0x01, 0x74, 0x10, 0x01];

    assert_eq!(statement.encode_to_vec(), fixture);
    assert_eq!(
        Statement::decode(fixture.as_slice()).expect("valid statement fixture"),
        statement
    );
}
