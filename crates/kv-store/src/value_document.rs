use automerge::{
    AutoCommit, ROOT, ReadDoc, ScalarValue, Value as AmValue, transaction::Transactable,
};
use btree::BTreeError;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValueState {
    pub value: Option<Vec<u8>>,
    pub expires_at: Option<i64>,
    pub tombstone: bool,
}

pub fn new_document(value: Vec<u8>, expires_at: Option<i64>) -> Result<AutoCommit, BTreeError> {
    let mut document = AutoCommit::new();
    write_live(&mut document, value, expires_at)?;
    Ok(document)
}

pub fn read_document(document: &AutoCommit) -> Result<ValueState, BTreeError> {
    let value = match scalar(document, "value")? {
        Some(ScalarValue::Bytes(bytes)) => Some(bytes),
        None => None,
        _ => return Err(BTreeError::InvalidDocument),
    };
    let expires_at = match scalar(document, "expires_at")? {
        Some(ScalarValue::Int(value)) => Some(value),
        None => None,
        _ => return Err(BTreeError::InvalidDocument),
    };
    let tombstone = match scalar(document, "tombstone")? {
        Some(ScalarValue::Boolean(value)) => value,
        _ => return Err(BTreeError::InvalidDocument),
    };
    if tombstone != value.is_none() || (tombstone && expires_at.is_some()) {
        return Err(BTreeError::InvalidDocument);
    }
    Ok(ValueState {
        value,
        expires_at,
        tombstone,
    })
}

pub fn write_live(
    document: &mut AutoCommit,
    value: Vec<u8>,
    expires_at: Option<i64>,
) -> Result<(), BTreeError> {
    document
        .put(ROOT, "value", ScalarValue::Bytes(value))
        .map_err(BTreeError::custom)?;
    match expires_at {
        Some(timestamp) => document
            .put(ROOT, "expires_at", timestamp)
            .map_err(BTreeError::custom)?,
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
    document
        .put(ROOT, "tombstone", false)
        .map_err(BTreeError::custom)?;
    Ok(())
}

pub fn write_tombstone(document: &mut AutoCommit) -> Result<(), BTreeError> {
    if document
        .get(ROOT, "value")
        .map_err(BTreeError::custom)?
        .is_some()
    {
        document.delete(ROOT, "value").map_err(BTreeError::custom)?;
    }
    if document
        .get(ROOT, "expires_at")
        .map_err(BTreeError::custom)?
        .is_some()
    {
        document
            .delete(ROOT, "expires_at")
            .map_err(BTreeError::custom)?;
    }
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
    use super::{new_document, read_document, write_tombstone};
    use automerge::AutoCommit;

    #[test]
    fn empty_value_and_expiration_round_trip() {
        let document = new_document(Vec::new(), Some(0)).unwrap();
        let state = read_document(&document).unwrap();
        assert_eq!(state.value, Some(Vec::new()));
        assert_eq!(state.expires_at, Some(0));
        assert!(!state.tombstone);
    }

    #[test]
    fn absent_expiration_and_tombstone_are_distinct() {
        let mut document = new_document(vec![1], None).unwrap();
        assert_eq!(read_document(&document).unwrap().expires_at, None);
        write_tombstone(&mut document).unwrap();
        assert!(read_document(&document).unwrap().tombstone);
    }

    #[test]
    fn rejects_missing_fields() {
        assert!(read_document(&AutoCommit::new()).is_err());
    }

    #[test]
    fn rejects_wrong_types_and_invalid_tombstones() {
        use automerge::{ROOT, ScalarValue, transaction::Transactable};

        let mut wrong_timestamp = new_document(vec![1], None).unwrap();
        wrong_timestamp.put(ROOT, "expires_at", "invalid").unwrap();
        assert!(read_document(&wrong_timestamp).is_err());

        let mut value_on_tombstone = new_document(vec![1], None).unwrap();
        value_on_tombstone
            .put(ROOT, "tombstone", ScalarValue::Boolean(true))
            .unwrap();
        assert!(read_document(&value_on_tombstone).is_err());
    }
}
