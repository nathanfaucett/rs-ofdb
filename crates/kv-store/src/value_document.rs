use automerge::{
    AutoCommit, ROOT, ReadDoc, ScalarValue, Value as AmValue, transaction::Transactable,
};
use btree::BTreeError;
use value::Value;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValueState {
    pub value: Option<Value>,
    pub expires_at: Option<i64>,
    pub tombstone: bool,
}

pub fn new_document(value: Value, expires_at: Option<i64>) -> Result<AutoCommit, BTreeError> {
    let mut document = AutoCommit::new();
    document
        .put(ROOT, "format_version", 2_u64)
        .map_err(BTreeError::custom)?;
    document
        .put(ROOT, "tombstone", false)
        .map_err(BTreeError::custom)?;
    write_live(&mut document, value, expires_at)?;
    Ok(document)
}

pub fn read_document(document: &AutoCommit) -> Result<ValueState, BTreeError> {
    let versions = document
        .get_all(ROOT, "format_version")
        .map_err(BTreeError::custom)?;
    if versions.is_empty()
        || versions
            .iter()
            .any(|(value, _)| !matches!(value, AmValue::Scalar(value) if value.as_ref() == &ScalarValue::Uint(2)))
    {
        return Err(BTreeError::InvalidDocument);
    }
    let markers = document
        .get_all(ROOT, "tombstone")
        .map_err(BTreeError::custom)?;
    if markers.is_empty() {
        return Err(BTreeError::InvalidDocument);
    }
    let mut tombstone = false;
    for (value, _) in markers {
        match value {
            AmValue::Scalar(value) => match value.as_ref() {
                ScalarValue::Boolean(deleted) => tombstone |= deleted,
                _ => return Err(BTreeError::InvalidDocument),
            },
            _ => return Err(BTreeError::InvalidDocument),
        }
    }
    // Deletion is permanent for this generation, including concurrent value and expiry edits.
    if tombstone {
        return Ok(ValueState {
            value: None,
            expires_at: None,
            tombstone,
        });
    }
    let expires_at = match scalar(document, "expires_at")? {
        Some(ScalarValue::Int(value)) => Some(value),
        None => None,
        _ => return Err(BTreeError::InvalidDocument),
    };
    if document
        .get(ROOT, "value")
        .map_err(BTreeError::custom)?
        .is_none()
    {
        return Err(BTreeError::InvalidDocument);
    }
    let value = autosurgeon::hydrate_prop(document, ROOT, "value")
        .map_err(|_| BTreeError::InvalidDocument)?;
    Ok(ValueState {
        value: Some(value),
        expires_at,
        tombstone,
    })
}

pub fn write_live(
    document: &mut AutoCommit,
    value: Value,
    expires_at: Option<i64>,
) -> Result<(), BTreeError> {
    document.set_actor(AutoCommit::new().get_actor().clone());
    autosurgeon::reconcile_prop(document, ROOT, "value", value).map_err(BTreeError::custom)?;
    match expires_at {
        Some(timestamp) => {
            if scalar(document, "expires_at")? != Some(ScalarValue::Int(timestamp)) {
                document
                    .put(ROOT, "expires_at", timestamp)
                    .map_err(BTreeError::custom)?;
            }
        }
        None => {
            if document
                .get(ROOT, "expires_at")
                .map_err(BTreeError::custom)?
                .is_some()
            {
                document
                    .delete(ROOT, "expires_at")
                    .map_err(BTreeError::custom)?;
            }
        }
    }
    Ok(())
}

pub fn write_tombstone(document: &mut AutoCommit) -> Result<(), BTreeError> {
    document.set_actor(AutoCommit::new().get_actor().clone());
    document
        .put(ROOT, "tombstone", true)
        .map_err(BTreeError::custom)?;
    Ok(())
}

fn scalar(document: &AutoCommit, key: &str) -> Result<Option<ScalarValue>, BTreeError> {
    match document.get(ROOT, key).map_err(BTreeError::custom)? {
        Some((AmValue::Scalar(value), _)) => Ok(Some(value.as_ref().clone())),
        None => Ok(None),
        _ => Err(BTreeError::InvalidDocument),
    }
}

#[cfg(test)]
mod tests {
    use automerge::{AutoCommit, ROOT, transaction::Transactable};
    use value::{JsonValue, Value};

    use super::{new_document, read_document, write_live, write_tombstone};

    #[test]
    fn shared_values_round_trip_under_property() {
        for value in [
            Value::Null,
            Value::Text("text".into()),
            Value::Integer(i64::MIN),
            Value::Bool(true),
            Value::Blob(vec![0, 255]),
            Value::Uuid(uuid::Uuid::nil()),
            Value::Json(JsonValue::Array(vec![JsonValue::Null])),
        ] {
            let document = new_document(value.clone(), Some(0)).expect("create value document");
            let state = read_document(&document).expect("hydrate property");
            assert_eq!(state.value, Some(value));
            assert_eq!(state.expires_at, Some(0));
        }
    }

    #[test]
    fn concurrent_map_edits_and_delete_merge() {
        let map = |a: bool, b: bool| {
            Value::Json(JsonValue::Object(
                [
                    ("a".into(), JsonValue::Bool(a)),
                    ("b".into(), JsonValue::Bool(b)),
                ]
                .into(),
            ))
        };
        let mut left = new_document(map(false, false), None).expect("create map");
        let mut right = left.fork();
        write_live(&mut left, map(true, false), Some(10)).expect("edit a");
        write_live(&mut right, map(false, true), Some(20)).expect("edit b");
        left.merge(&mut right).expect("merge map");
        assert_eq!(
            read_document(&left).expect("read merged map").value,
            Some(map(true, true))
        );
        let mut deleted = left.fork();
        write_tombstone(&mut deleted).expect("delete generation");
        write_live(&mut left, map(false, true), Some(30)).expect("concurrent update");
        left.merge(&mut deleted).expect("merge deletion");
        let state = read_document(&left).expect("read deleted generation");
        assert!(state.tombstone);
        assert_eq!(state.value, None);
        assert_eq!(state.expires_at, None);
    }

    #[test]
    fn concurrent_type_changes_have_a_valid_deterministic_value() {
        let mut base = new_document(Value::Null, None).expect("create seed");
        let mut left = base.fork();
        let mut right = base.fork();
        write_live(&mut left, Value::Integer(42), None).expect("write integer");
        write_live(&mut right, Value::Text("text".into()), None).expect("write text");
        let mut reverse = right.clone();
        let mut left_copy = left.clone();
        left.merge(&mut right).expect("merge forward");
        reverse.merge(&mut left_copy).expect("merge reverse");
        let forward = read_document(&left).expect("hydrate concurrent variants");
        assert_eq!(
            forward,
            read_document(&reverse).expect("hydrate reverse merge")
        );
        assert!(matches!(
            forward.value,
            Some(Value::Integer(42) | Value::Text(_))
        ));
    }

    #[test]
    fn rejects_old_format_and_invalid_metadata() {
        let mut old = AutoCommit::new();
        old.put(ROOT, "value", vec![1_u8]).expect("write old bytes");
        old.put(ROOT, "tombstone", false)
            .expect("write old metadata");
        assert!(read_document(&old).is_err());
        let mut invalid = new_document(Value::Null, None).expect("create document");
        invalid
            .put(ROOT, "expires_at", "invalid")
            .expect("corrupt timestamp");
        assert!(read_document(&invalid).is_err());
        assert!(read_document(&AutoCommit::new()).is_err());
    }
}
