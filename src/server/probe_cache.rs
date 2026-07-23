//! Single-flighted, short-TTL existence cache for GET.
//!
//! CI bursts probe the same *missing* keys over and over: one incident burst
//! measured 5,068 GET-404s over 154 distinct keys (~33x duplication) as
//! parallel Nx agents all miss the same cache entries. Each of those was a
//! fresh S3 HeadObject/GetObject - a new connection + `getaddrinfo`, which under
//! load stampedes the VPC resolver and times S3 out at 30s (the "Mode B" 500s).
//!
//! This collapses that fan-out to ~one S3 call per key per TTL:
//! - **Single-flight**: concurrent probes for the same hash share one S3 call
//!   (`tokio::sync::OnceCell::get_or_try_init` runs the closure once; the rest
//!   await it). Handles the simultaneous salvo a plain TTL cache can't.
//! - **TTL cache**: the result (present *or* absent) is reused for `TTL`.
//!   Handles the repeated re-probes across the burst window. Keys are
//!   content-addressed, so a cached "present" can't go stale within seconds;
//!   a cached "absent" self-heals after TTL when the artifact is later written.
//!
//! An S3 error is not cached - `get_or_try_init` leaves the cell uninitialised
//! so the next caller retries.

use std::collections::HashMap;
use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::OnceCell;

/// How long a presence result is trusted. A few seconds is enough to swallow a
/// CI burst while keeping a just-written artifact visible almost immediately.
// ponytail: a const, not an env knob - make it configurable when tuning needs it.
const TTL: Duration = Duration::from_secs(5);

/// Cap on retained slots. A resolved slot lingers until it's re-probed (which
/// replaces it) or a sweep fires, so a long tail of never-again-probed keys
/// would otherwise grow the map unbounded. Past this size we drop everything
/// that's both expired and resolved.
const SWEEP_THRESHOLD: usize = 10_000;

struct Slot {
    present: OnceCell<bool>,
    created: Instant,
}

impl Slot {
    /// Usable while its result is still fresh, or while its probe is still in
    /// flight (unresolved). Keeping an unresolved slot alive past TTL matters
    /// under the exact Mode B condition: if S3 stalls toward the 30s timeout,
    /// every arrival during the stall still coalesces onto the one in-flight
    /// probe instead of starting a fresh one each TTL window.
    fn live(&self) -> bool {
        self.present.get().is_none() || self.created.elapsed() < TTL
    }
}

#[derive(Default)]
pub struct ProbeCache {
    slots: Mutex<HashMap<String, Arc<Slot>>>,
}

impl ProbeCache {
    /// Returns whether `hash` exists, coalescing concurrent callers and caching
    /// the answer for `TTL`. `probe` performs the real (single) existence check
    /// and only runs on a cache miss.
    pub async fn present<F, Fut, E>(&self, hash: &str, probe: F) -> Result<bool, E>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<bool, E>>,
    {
        let slot = self.slot(hash);
        // First caller runs `probe`; concurrent callers await the same future.
        slot.present.get_or_try_init(probe).await.copied()
    }

    fn slot(&self, hash: &str) -> Arc<Slot> {
        let mut slots = self.slots.lock().expect("probe cache mutex poisoned");
        if let Some(existing) = slots.get(hash) {
            if existing.live() {
                return existing.clone();
            }
        }
        if slots.len() > SWEEP_THRESHOLD {
            slots.retain(|_, s| s.live());
        }
        let slot = Arc::new(Slot {
            present: OnceCell::new(),
            created: Instant::now(),
        });
        slots.insert(hash.to_string(), slot.clone());
        slot
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[tokio::test]
    async fn concurrent_probes_for_same_key_run_probe_once() {
        let cache = Arc::new(ProbeCache::default());
        let calls = Arc::new(AtomicUsize::new(0));

        // Fire many concurrent probes for the same missing key - the exact CI
        // burst shape. Exactly one must reach S3.
        let mut handles = Vec::new();
        for _ in 0..50 {
            let cache = cache.clone();
            let calls = calls.clone();
            handles.push(tokio::spawn(async move {
                cache
                    .present("deadbeef", || async {
                        calls.fetch_add(1, Ordering::SeqCst);
                        // Yield so the whole batch piles up on one in-flight call.
                        tokio::task::yield_now().await;
                        Ok::<bool, ()>(false)
                    })
                    .await
            }));
        }
        for h in handles {
            assert_eq!(h.await.unwrap(), Ok(false));
        }
        assert_eq!(calls.load(Ordering::SeqCst), 1, "single-flight must collapse the burst to one probe");
    }

    #[tokio::test]
    async fn result_is_cached_within_ttl() {
        let cache = ProbeCache::default();
        let calls = AtomicUsize::new(0);
        let probe = || async {
            calls.fetch_add(1, Ordering::SeqCst);
            Ok::<bool, ()>(true)
        };
        assert_eq!(cache.present("abc", probe).await, Ok(true));
        assert_eq!(cache.present("abc", probe).await, Ok(true));
        assert_eq!(calls.load(Ordering::SeqCst), 1, "second probe within TTL must hit the cache");
    }

    #[tokio::test]
    async fn error_is_not_cached() {
        let cache = ProbeCache::default();
        let calls = AtomicUsize::new(0);
        let probe = || async {
            calls.fetch_add(1, Ordering::SeqCst);
            Err::<bool, ()>(())
        };
        assert_eq!(cache.present("abc", probe).await, Err(()));
        assert_eq!(cache.present("abc", probe).await, Err(()));
        assert_eq!(calls.load(Ordering::SeqCst), 2, "a failed probe must be retried, not cached");
    }
}
