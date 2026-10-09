use std::{
    io,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

use crate::resource::{file_identity, remove_owned_file, retired_error};

#[derive(Clone, Debug)]
pub(crate) struct OwnedFile {
    pub path: PathBuf,
    pub identity: String,
}

#[derive(Clone, Debug, Default)]
pub struct NativeStageTracker(Arc<Mutex<State>>);

#[derive(Debug, Default)]
struct State {
    retired: bool,
    files: Vec<OwnedFile>,
}

impl NativeStageTracker {
    pub fn track(&self, path: &Path) -> io::Result<()> {
        let mut state = self.0.lock().expect("stage tracker lock");
        if state.retired {
            return Err(retired_error());
        }
        state.files.push(OwnedFile {
            path: std::path::absolute(path)?,
            identity: file_identity(path)?,
        });
        Ok(())
    }

    pub fn forget(&self, path: &Path) {
        if let Ok(path) = std::path::absolute(path) {
            self.0
                .lock()
                .expect("stage tracker lock")
                .files
                .retain(|file| file.path != path);
        }
    }

    pub(crate) fn retire(&self) -> Vec<OwnedFile> {
        let mut state = self.0.lock().expect("stage tracker lock");
        state.retired = true;
        state.files.clone()
    }

    pub(crate) fn cleanup(&self) -> io::Result<()> {
        let mut state = self.0.lock().expect("stage tracker lock");
        while let Some(file) = state.files.last() {
            remove_owned_file(file)?;
            state.files.pop();
        }
        Ok(())
    }
}
