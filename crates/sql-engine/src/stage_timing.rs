#[cfg(feature = "std")]
use std::time::Instant;

pub(crate) struct StageTiming {
    #[cfg(feature = "std")]
    started: Option<Instant>,
    label: &'static str,
    pub(crate) count: usize,
}

impl StageTiming {
    pub(crate) fn new(label: &'static str) -> Self {
        Self {
            #[cfg(feature = "std")]
            started: log::log_enabled!(target: "of::idp_stage_timing", log::Level::Info)
                .then(Instant::now),
            label,
            count: 0,
        }
    }
}

impl Drop for StageTiming {
    fn drop(&mut self) {
        #[cfg(feature = "std")]
        if let Some(started) = self.started {
            log::info!(target: "of::idp_stage_timing",
                "{} elapsed_us={} count={}", self.label, started.elapsed().as_micros(), self.count);
        }
        #[cfg(not(feature = "std"))]
        let _ = self.label;
    }
}
