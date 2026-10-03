use btree::BTreeError;
use uuid::Uuid;

pub fn encode_document_id(key: &str, generation: Uuid) -> Vec<u8> {
    let mut id = Vec::with_capacity(key.len() + 18);
    for byte in key.bytes() {
        if byte == 0 {
            id.extend_from_slice(&[0, 255]);
        } else {
            id.push(byte);
        }
    }
    id.extend_from_slice(&[0, 0]);
    id.extend_from_slice(generation.as_bytes());
    id
}

pub fn decode_document_id(id: &[u8]) -> Result<(String, Uuid), BTreeError> {
    let mut key = Vec::new();
    let mut index = 0;
    loop {
        match (id.get(index), id.get(index + 1)) {
            (Some(0), Some(0)) => {
                index += 2;
                break;
            }
            (Some(0), Some(255)) => {
                key.push(0);
                index += 2;
            }
            (Some(0), _) | (None, _) => return Err(BTreeError::InvalidDocument),
            (Some(byte), _) => {
                key.push(*byte);
                index += 1;
            }
        }
    }
    if id.len() != index + 16 {
        return Err(BTreeError::InvalidDocument);
    }
    let key = String::from_utf8(key).map_err(BTreeError::custom)?;
    let generation = Uuid::from_slice(&id[index..]).map_err(BTreeError::custom)?;
    if generation.get_version_num() != 7 {
        return Err(BTreeError::InvalidDocument);
    }
    Ok((key, generation))
}

#[cfg(test)]
mod tests {
    use uuid::Uuid;

    use super::{decode_document_id, encode_document_id};

    fn uuid(value: u128) -> Uuid {
        Uuid::from_u128(value | (7 << 76) | (2 << 62))
    }

    #[test]
    fn round_trips_arbitrary_utf8_keys_and_v7_generations() {
        for key in ["", "a", "ab", "a:b", "a\0b", "雪"] {
            let id = uuid(1);
            assert_eq!(
                decode_document_id(&encode_document_id(key, id)).unwrap(),
                (key.into(), id)
            );
        }
    }

    #[test]
    fn ordering_groups_keys_and_sorts_generations() {
        let a = encode_document_id("a", uuid(1));
        let a_later = encode_document_id("a", uuid(2));
        let ab = encode_document_id("ab", uuid(1));
        let nul = encode_document_id("a\0b", uuid(1));
        assert!(a < a_later);
        assert!(a_later < ab);
        assert!(nul < ab);
    }

    #[test]
    fn rejects_malformed_document_ids() {
        let good = encode_document_id("key", uuid(1));
        assert!(decode_document_id(&good[..good.len() - 1]).is_err());
        assert!(decode_document_id(&[b'k', 0, 4]).is_err());
        assert!(decode_document_id(&[0, 255, 0, 0, 1]).is_err());
        assert!(decode_document_id(&encode_document_id("key", Uuid::nil())).is_err());
        let mut trailing = good;
        trailing.push(0);
        assert!(decode_document_id(&trailing).is_err());
    }
}
