#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
use core::{future::Future, time::Duration};

#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
pub use tokio::time::{sleep, timeout};

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
pub async fn sleep(duration: Duration) {
    // Platform timers use signed 32-bit millisecond delays; chunk longer waits.
    let mut millis = duration.as_millis() + u128::from(duration.subsec_nanos() % 1_000_000 != 0);
    while millis > i32::MAX as u128 {
        gloo_timers::future::TimeoutFuture::new(i32::MAX as u32).await;
        millis -= i32::MAX as u128;
    }
    gloo_timers::future::TimeoutFuture::new(millis as u32).await;
}

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
pub async fn timeout<F: Future>(duration: Duration, future: F) -> Result<F::Output, ()> {
    use futures::{
        future::{Either, select},
        pin_mut,
    };
    let timer = sleep(duration);
    pin_mut!(future, timer);
    match select(future, timer).await {
        Either::Left((output, _)) => Ok(output),
        Either::Right(_) => Err(()),
    }
}
