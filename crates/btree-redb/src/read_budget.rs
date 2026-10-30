use std::{marker::PhantomData, sync::Mutex};

use btree::{BTreeError, BTreeResult};

#[derive(Default)]
pub(crate) struct ReadBudget(Option<Mutex<(usize, usize, bool)>>);

impl ReadBudget {
    pub(crate) fn set(&mut self, entries: usize, bytes: usize) -> BTreeResult<()> {
        if self.0.is_some() {
            return Err(BTreeError::UnsupportedOperation);
        }
        self.0 = Some(Mutex::new((entries, bytes, false)));
        Ok(())
    }

    pub(crate) fn exhausted(&self) -> bool {
        self.0
            .as_ref()
            .is_some_and(|budget| budget.lock().expect("native read budget lock").2)
    }

    pub(crate) fn charge(&self, bytes: usize) -> BTreeResult<()> {
        let Some(budget) = self.0.as_ref() else {
            return Ok(());
        };
        let mut budget = budget.lock().expect("native read budget lock");
        let (entries, remaining, exhausted) = &mut *budget;
        if *entries == 0 || bytes > *remaining {
            *exhausted = true;
            *entries = 0;
            *remaining = 0;
            return Err(BTreeError::custom("native read limit exceeded"));
        }
        *entries -= 1;
        *remaining -= bytes;
        Ok(())
    }
}

// Borrow the original encoding so admission precedes owned key/value allocation.
#[derive(Debug)]
pub(crate) struct Borrowed<T>(PhantomData<T>);

impl<T: redb::Value> redb::Value for Borrowed<T> {
    type SelfType<'a>
        = &'a [u8]
    where
        Self: 'a;
    type AsBytes<'a>
        = &'a [u8]
    where
        Self: 'a;

    fn fixed_width() -> Option<usize> {
        T::fixed_width()
    }

    fn from_bytes<'a>(data: &'a [u8]) -> Self::SelfType<'a>
    where
        Self: 'a,
    {
        data
    }

    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> Self::AsBytes<'a>
    where
        Self: 'b,
    {
        value
    }

    fn type_name() -> redb::TypeName {
        T::type_name()
    }
}

impl<T: redb::Key> redb::Key for Borrowed<T> {
    fn compare(left: &[u8], right: &[u8]) -> core::cmp::Ordering {
        T::compare(left, right)
    }
}
