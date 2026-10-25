#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
use core::{future::Future, time::Duration};

#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
pub use tokio::time::{sleep, timeout};

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
pub fn sleep(duration: Duration) -> impl Future<Output = ()> {
    // Arm before polling the operation: synchronous work consumes its budget too.
    let mut millis = duration.as_millis() + u128::from(duration.subsec_nanos() % 1_000_000 != 0);
    let chunk = millis.min(i32::MAX as u128);
    let mut timer = gloo_timers::future::TimeoutFuture::new(chunk as u32);
    millis -= chunk;
    async move {
        loop {
            timer.await;
            if millis == 0 {
                break;
            }
            let chunk = millis.min(i32::MAX as u128);
            timer = gloo_timers::future::TimeoutFuture::new(chunk as u32);
            millis -= chunk;
        }
    }
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
