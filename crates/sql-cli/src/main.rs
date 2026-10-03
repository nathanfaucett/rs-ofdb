use std::{
    collections::BTreeMap,
    io::{self, Read, Write},
    path::PathBuf,
    process::ExitCode,
    time::Duration,
};

use base64::{Engine, engine::general_purpose::STANDARD};
use clap::{ArgGroup, Parser};
#[cfg(any(feature = "remote", feature = "in-memory", feature = "redb"))]
use ofdb_sql::QueryResult;
use ofdb_sql::{JsonNumber, JsonValue, QueryParams, Value, ValueType};
#[cfg(any(feature = "remote", feature = "in-memory", feature = "redb", test))]
use serde_json::Map;
use serde_json::Value as Json;

#[derive(Debug, Parser)]
#[command(name = "sql-cli", version, about = "Query an ofdb SQL database")]
#[command(group(ArgGroup::new("target").required(true).multiple(false).args(["database", "memory", "endpoint", "unix_socket"]))) ]
#[command(group(ArgGroup::new("input").required(true).multiple(false).args(["query", "file"]))) ]
struct Args {
    #[arg(long, group = "target")]
    database: Option<PathBuf>,
    #[arg(long, group = "target")]
    memory: bool,
    #[arg(long, group = "target")]
    endpoint: Option<String>,
    #[arg(long, group = "target")]
    unix_socket: Option<PathBuf>,
    #[arg(long, group = "input")]
    query: Option<String>,
    #[arg(long, group = "input")]
    file: Option<PathBuf>,
    #[arg(long)]
    params_file: Option<PathBuf>,
    #[arg(long, value_parser = clap::value_parser!(u64).range(1..))]
    timeout: Option<u64>,
}

#[tokio::main]
async fn main() -> ExitCode {
    let args = Args::parse();
    let _sql = match read_text(args.query.as_deref(), args.file.as_deref()) {
        Ok(sql) => sql,
        Err(error) => return fail(1, error),
    };
    let _params = match args.params_file.as_deref().map(read_params).transpose() {
        Ok(params) => params,
        Err(error) => return fail(2, error),
    };

    let operation = async {
        if let Some(_endpoint) = &args.endpoint {
            #[cfg(feature = "remote")]
            {
                let client = ofdb_sql::Client::connect(_endpoint)
                    .await
                    .map_err(|error| error.to_string())?;
                let result = client
                    .execute_sql(&_sql, _params.as_ref())
                    .await
                    .map_err(|error| error.to_string())?;
                return encode_results(result);
            }
            #[cfg(not(feature = "remote"))]
            return Err("sql-cli was built without the `remote` feature".to_owned());
        }
        if let Some(_path) = &args.unix_socket {
            #[cfg(all(feature = "remote", unix))]
            {
                let client = ofdb_sql::Client::connect_unix(_path)
                    .await
                    .map_err(|error| error.to_string())?;
                let result = client
                    .execute_sql(&_sql, _params.as_ref())
                    .await
                    .map_err(|error| error.to_string())?;
                return encode_results(result);
            }
            #[cfg(not(all(feature = "remote", unix)))]
            return Err("Unix sockets require a Unix build with the `remote` feature".to_owned());
        }
        if args.memory {
            #[cfg(feature = "in-memory")]
            {
                let database = ofdb_sql::Database::in_memory();
                let result = database
                    .execute_sql(&_sql, _params.as_ref())
                    .await
                    .map_err(|error| error.to_string())?;
                return encode_results(result);
            }
            #[cfg(not(feature = "in-memory"))]
            return Err("sql-cli was built without the `in-memory` feature".to_owned());
        }
        if let Some(_path) = &args.database {
            #[cfg(feature = "redb")]
            {
                let database =
                    ofdb_sql::Database::open(_path).map_err(|error| error.to_string())?;
                let result = database
                    .execute_sql(&_sql, _params.as_ref())
                    .await
                    .map_err(|error| error.to_string())?;
                return encode_results(result);
            }
            #[cfg(not(feature = "redb"))]
            return Err("sql-cli was built without the `redb` feature".to_owned());
        }
        Err("select exactly one target".to_owned())
    };
    let result: Result<Vec<u8>, String> = if let Some(seconds) = args.timeout {
        match tokio::time::timeout(Duration::from_secs(seconds), operation).await {
            Ok(result) => result,
            Err(_) => {
                Err("operation timed out; the statement batch may still have committed".to_owned())
            }
        }
    } else {
        operation.await
    };
    match result {
        Ok(bytes) => {
            if let Err(error) = io::stdout().lock().write_all(&bytes) {
                fail(1, error.to_string())
            } else {
                ExitCode::SUCCESS
            }
        }
        Err(error) => fail(1, error),
    }
}

fn read_text(query: Option<&str>, file: Option<&std::path::Path>) -> Result<String, String> {
    if let Some(query) = query {
        return Ok(query.to_owned());
    }
    let path = file.expect("clap requires exactly one SQL input");
    let mut bytes = Vec::new();
    if path == std::path::Path::new("-") {
        io::stdin()
            .read_to_end(&mut bytes)
            .map_err(|error| error.to_string())?;
    } else {
        bytes = std::fs::read(path).map_err(|error| error.to_string())?;
    }
    String::from_utf8(bytes).map_err(|error| format!("SQL input is not UTF-8: {error}"))
}

fn read_params(path: &std::path::Path) -> Result<QueryParams, String> {
    let json: Json =
        serde_json::from_slice(&std::fs::read(path).map_err(|error| error.to_string())?)
            .map_err(|error| format!("invalid parameter JSON: {error}"))?;
    let object = json
        .as_object()
        .ok_or_else(|| "parameter file must be an object".to_owned())?;
    if object.len() != 1 {
        return Err(
            "parameter file must contain exactly one of `positional` or `named`".to_owned(),
        );
    }
    if let Some(values) = object.get("positional") {
        let values = values
            .as_array()
            .ok_or_else(|| "positional parameters must be an array".to_owned())?;
        return values
            .iter()
            .map(parse_value)
            .collect::<Result<Vec<_>, _>>()
            .map(QueryParams::Positional);
    }
    if let Some(values) = object.get("named") {
        let values = values
            .as_object()
            .ok_or_else(|| "named parameters must be an object".to_owned())?;
        let values = values
            .iter()
            .map(|(key, value)| Ok((key.clone(), parse_value(value)?)))
            .collect::<Result<BTreeMap<_, _>, String>>()?;
        return Ok(QueryParams::Named(values));
    }
    Err("parameter file must contain `positional` or `named`".to_owned())
}

fn parse_value(value: &Json) -> Result<Value, String> {
    let object = value
        .as_object()
        .ok_or_else(|| "each SQL value must be a tagged object".to_owned())?;
    let tag = object
        .get("type")
        .and_then(Json::as_str)
        .ok_or_else(|| "SQL value tag must contain string `type`".to_owned())?;
    let payload = object.get("value");
    match tag {
        "null" => Ok(Value::Null),
        "type" => match payload
            .and_then(Json::as_str)
            .ok_or_else(|| "type value requires string payload".to_owned())?
        {
            "Null" => Ok(Value::Type(ValueType::Null)),
            "Type" => Ok(Value::Type(ValueType::Type)),
            "Uuid" => Ok(Value::Type(ValueType::Uuid)),
            "Bool" => Ok(Value::Type(ValueType::Bool)),
            "Integer" => Ok(Value::Type(ValueType::Integer)),
            "Float" => Ok(Value::Type(ValueType::Float)),
            "Text" => Ok(Value::Type(ValueType::Text)),
            "Json" => Ok(Value::Type(ValueType::Json)),
            "Blob" => Ok(Value::Type(ValueType::Blob)),
            other => Err(format!("unknown SQL type tag: {other}")),
        },
        "uuid" => Ok(Value::Uuid(
            payload
                .and_then(Json::as_str)
                .ok_or_else(|| "uuid requires string payload".to_owned())?
                .parse()
                .map_err(|error| format!("invalid UUID: {error}"))?,
        )),
        "bool" => {
            Ok(Value::Bool(payload.and_then(Json::as_bool).ok_or_else(
                || "bool requires boolean payload".to_owned(),
            )?))
        }
        "integer" => Ok(Value::Integer(
            payload
                .and_then(Json::as_str)
                .ok_or_else(|| "integer requires decimal string payload".to_owned())?
                .parse()
                .map_err(|error| format!("invalid integer: {error}"))?,
        )),
        "float" => {
            let bits = payload
                .and_then(Json::as_str)
                .ok_or_else(|| "float requires 16-digit hexadecimal payload".to_owned())?;
            if bits.len() != 16 {
                return Err("float payload must have 16 hexadecimal digits".to_owned());
            }
            Ok(Value::Float(f64::from_bits(
                u64::from_str_radix(bits, 16)
                    .map_err(|error| format!("invalid float bits: {error}"))?,
            )))
        }
        "text" => Ok(Value::Text(
            payload
                .and_then(Json::as_str)
                .ok_or_else(|| "text requires string payload".to_owned())?
                .to_owned(),
        )),
        "blob" => Ok(Value::Blob(
            STANDARD
                .decode(
                    payload
                        .and_then(Json::as_str)
                        .ok_or_else(|| "blob requires base64 string payload".to_owned())?,
                )
                .map_err(|error| format!("invalid base64 blob: {error}"))?,
        )),
        "json" => {
            Ok(Value::Json(parse_json(payload.ok_or_else(|| {
                "json requires value payload".to_owned()
            })?)?))
        }
        other => Err(format!("unknown SQL value tag: {other}")),
    }
}

fn parse_json(value: &Json) -> Result<JsonValue, String> {
    match value {
        Json::Null => Ok(JsonValue::Null),
        Json::Bool(value) => Ok(JsonValue::Bool(*value)),
        Json::String(value) => Ok(JsonValue::String(value.clone())),
        Json::Array(values) => values
            .iter()
            .map(parse_json)
            .collect::<Result<Vec<_>, _>>()
            .map(JsonValue::Array),
        Json::Object(object) if object.len() == 1 && object.contains_key("$number") => {
            let number = object.get("$number").expect("checked one-number object");
            let kind = number
                .get("type")
                .and_then(Json::as_str)
                .ok_or_else(|| "nested number needs string type".to_owned())?;
            let value = number
                .get("value")
                .and_then(Json::as_str)
                .ok_or_else(|| "nested number needs string value".to_owned())?;
            let number = match kind {
                "i64" => JsonNumber::I64(
                    value
                        .parse()
                        .map_err(|error| format!("invalid nested i64: {error}"))?,
                ),
                "u64" => JsonNumber::U64(
                    value
                        .parse()
                        .map_err(|error| format!("invalid nested u64: {error}"))?,
                ),
                "f64" if value.len() == 16 => JsonNumber::F64(f64::from_bits(
                    u64::from_str_radix(value, 16)
                        .map_err(|error| format!("invalid nested float bits: {error}"))?,
                )),
                "f64" => {
                    return Err("nested float payload must have 16 hexadecimal digits".to_owned());
                }
                _ => return Err(format!("unknown nested number type: {kind}")),
            };
            Ok(JsonValue::Number(number))
        }
        Json::Object(object) => object
            .iter()
            .map(|(key, value)| Ok((key.clone(), parse_json(value)?)))
            .collect::<Result<BTreeMap<_, _>, String>>()
            .map(JsonValue::Object),
        Json::Number(number) => {
            if let Some(value) = number.as_i64() {
                Ok(JsonValue::Number(JsonNumber::I64(value)))
            } else if let Some(value) = number.as_u64() {
                Ok(JsonValue::Number(JsonNumber::U64(value)))
            } else {
                Err("JSON numbers must use the tagged `$number` representation".to_owned())
            }
        }
    }
}

#[cfg(any(feature = "remote", feature = "in-memory", feature = "redb"))]
fn encode_results(results: Vec<QueryResult>) -> Result<Vec<u8>, String> {
    let results = results.into_iter().map(|result| {
        let columns = result.columns.into_iter().map(|column| serde_json::json!({
            "name": column.name, "source_table": column.source_table, "source_column": column.source_column
        })).collect::<Vec<_>>();
        let rows = result.rows.into_iter().map(|row| row.values.into_iter().map(encode_value).collect::<Vec<_>>()).collect::<Vec<_>>();
        serde_json::json!({ "columns": columns, "rows": rows })
    }).collect::<Vec<_>>();
    serde_json::to_vec(&results).map_err(|error| error.to_string())
}

#[cfg(any(feature = "remote", feature = "in-memory", feature = "redb", test))]
fn encode_value(value: Value) -> Json {
    let (tag, payload) = match value {
        Value::Null => ("null", Json::Null),
        Value::Type(value) => ("type", Json::String(format!("{value:?}"))),
        Value::Uuid(value) => ("uuid", Json::String(value.to_string())),
        Value::Bool(value) => ("bool", Json::Bool(value)),
        Value::Integer(value) => ("integer", Json::String(value.to_string())),
        Value::Float(value) => ("float", Json::String(format!("{:016x}", value.to_bits()))),
        Value::Text(value) => ("text", Json::String(value)),
        Value::Blob(value) => ("blob", Json::String(STANDARD.encode(value))),
        Value::Json(value) => ("json", encode_json(value)),
    };
    let mut object = Map::new();
    object.insert("type".to_owned(), Json::String(tag.to_owned()));
    if tag != "null" {
        object.insert("value".to_owned(), payload);
    }
    Json::Object(object)
}

#[cfg(any(feature = "remote", feature = "in-memory", feature = "redb", test))]
fn encode_json(value: JsonValue) -> Json {
    match value {
        JsonValue::Null => Json::Null,
        JsonValue::Bool(value) => Json::Bool(value),
        JsonValue::String(value) => Json::String(value),
        JsonValue::Array(values) => Json::Array(values.into_iter().map(encode_json).collect()),
        JsonValue::Object(values) => Json::Object(
            values
                .into_iter()
                .map(|(key, value)| (key, encode_json(value)))
                .collect(),
        ),
        JsonValue::Number(JsonNumber::I64(value)) => {
            serde_json::json!({"$number":{"type":"i64","value":value.to_string()}})
        }
        JsonValue::Number(JsonNumber::U64(value)) => {
            serde_json::json!({"$number":{"type":"u64","value":value.to_string()}})
        }
        JsonValue::Number(JsonNumber::F64(value)) => {
            serde_json::json!({"$number":{"type":"f64","value":format!("{:016x}", value.to_bits())}})
        }
    }
}

fn fail(code: u8, message: String) -> ExitCode {
    let _ = writeln!(io::stderr().lock(), "{message}");
    ExitCode::from(code)
}

#[cfg(test)]
mod tests {
    use super::{Args, encode_json, encode_value, parse_json, parse_value};
    use clap::Parser;
    use ofdb_sql::{JsonNumber, JsonValue, Value};
    use serde_json::json;

    #[test]
    fn cli_requires_one_target_and_one_input() {
        assert!(Args::try_parse_from(["sql-cli", "--memory", "--query", "SELECT 1"]).is_ok());
        assert!(Args::try_parse_from(["sql-cli", "--query", "SELECT 1"]).is_err());
        assert!(
            Args::try_parse_from([
                "sql-cli",
                "--memory",
                "--endpoint",
                "http://localhost",
                "--query",
                "SELECT 1"
            ])
            .is_err()
        );
        assert!(
            Args::try_parse_from(["sql-cli", "--memory", "--query", "SELECT 1", "--file", "-"])
                .is_err()
        );
    }

    #[test]
    fn sql_scalar_values_round_trip_losslessly() {
        let values = [
            Value::Null,
            Value::Json(JsonValue::Null),
            Value::Integer(i64::MIN),
            Value::Float(f64::from_bits(0x8000_0000_0000_0000)),
            Value::Float(f64::from_bits(0x7ff8_0000_0000_0001)),
            Value::Blob(vec![0, 255]),
            Value::Text("value".to_owned()),
        ];
        for value in values {
            assert_eq!(
                parse_value(&encode_value(value.clone())).expect("decode encoded value"),
                value
            );
        }
    }

    #[test]
    fn nested_json_numbers_round_trip_without_precision_loss() {
        let value = JsonValue::Object(
            [(
                "values".to_owned(),
                JsonValue::Array(vec![
                    JsonValue::Number(JsonNumber::U64(u64::MAX)),
                    JsonValue::Number(JsonNumber::F64(f64::from_bits(0x7ff0_0000_0000_0000))),
                ]),
            )]
            .into(),
        );
        assert_eq!(
            parse_json(&encode_json(value.clone())).expect("decode JSON value"),
            value
        );
        assert!(parse_json(&json!(1.5)).is_err());
    }
}
