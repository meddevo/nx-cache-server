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
//! - **TTL cache**: the result is reused for a TTL that differs by outcome.
//!   Keys are content-addressed (immutable), so a `present=true` result is safe
//!   to trust for a long while (`POSITIVE_TTL`) - it only saves a repeat
//!   HeadObject on hot keys. A `present=false` result is the staleness risk (a
//!   key absent now may be written a moment later), so it's kept short
//!   (`NEGATIVE_TTL`); a stale 404 is just a cache miss (Nx recomputes), never
//!   wrong data, and self-heals at TTL.
//! - **Seed on write**: a successful PUT calls [`ProbeCache::mark_present`],
//!   overwriting any cached `false` so same-instance GETs see the new artifact
//!   immediately instead of waiting out the negative TTL. (Only same-instance:
//!   with >1 task, the other task's cache still self-heals via NEGATIVE_TTL.)
//!
//! An S3 error is not cached - `get_or_try_init` leaves the cell uninitialised
//! so the next caller retries.

use std::collections::HashMap;
use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::OnceCell;

/// How long an *absent* result is trusted. Short: a key can be written moments
/// after being probed missing, and this bounds that stale-404 window.
// ponytail: consts, not env knobs - make configurable when tuning needs it.
const NEGATIVE_TTL: Duration = Duration::from_secs(5);

/// How long a *present* result is trusted. Long is safe because keys are
/// content-addressed and immutable; the only way it goes stale is a delete
/// (rare, lifecycle-scale), which degrades harmlessly to a retrieve()->404.
const POSITIVE_TTL: Duration = Duration::from_secs(60);

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
    /// Usable while its result is still fresh (present/absent have different
    /// TTLs, see the consts), or while its probe is still in flight
    /// (unresolved). Keeping an unresolved slot alive past TTL matters under the
    /// exact Mode B condition: if S3 stalls toward the 30s timeout, every
    /// arrival during the stall still coalesces onto the one in-flight probe
    /// instead of starting a fresh one each TTL window.
    fn live(&self, now: Instant) -> bool {
        let age = now.saturating_duration_since(self.created);
        match self.present.get() {
            None => true,
            Some(true) => age < POSITIVE_TTL,
            Some(false) => age < NEGATIVE_TTL,
        }
    }
}

#[derive(Default)]
pub struct ProbeCache {
    slots: Mutex<HashMap<String, Arc<Slot>>>,
}

impl ProbeCache {
    /// Returns whether `hash` exists, coalescing concurrent callers and caching
    /// the answer (per-outcome TTL). `probe` performs the real (single)
    /// existence check and only runs on a cache miss.
    pub async fn present<F, Fut, E>(&self, hash: &str, probe: F) -> Result<bool, E>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<bool, E>>,
    {
        let slot = self.slot(hash);
        // First caller runs `probe`; concurrent callers await the same future.
        slot.present.get_or_try_init(probe).await.copied()
    }

    /// Record that `hash` now exists - called after a successful write so
    /// subsequent GETs on this instance skip the HeadObject (and never see a
    /// stale cached 404 from a pre-write probe). Overwrites any existing slot.
    pub fn mark_present(&self, hash: &str) {
        let now = Instant::now();
        let present = OnceCell::new();
        present.set(true).expect("fresh cell is never initialised");
        let slot = Arc::new(Slot { present, created: now });
        let mut slots = self.slots.lock().expect("probe cache mutex poisoned");
        Self::sweep(&mut slots, now);
        slots.insert(hash.to_string(), slot);
    }

    fn slot(&self, hash: &str) -> Arc<Slot> {
        let now = Instant::now();
        let mut slots = self.slots.lock().expect("probe cache mutex poisoned");
        if let Some(existing) = slots.get(hash) {
            if existing.live(now) {
                return existing.clone();
            }
        }
        Self::sweep(&mut slots, now);
        let slot = Arc::new(Slot {
            present: OnceCell::new(),
            created: now,
        });
        slots.insert(hash.to_string(), slot.clone());
        slot
    }

    /// Drop expired/resolved slots once the map grows past the cap. Called on
    /// both the probe path (`slot`) and the write path (`mark_present`) so
    /// neither can grow the map without bound.
    fn sweep(slots: &mut HashMap<String, Arc<Slot>>, now: Instant) {
        if slots.len() > SWEEP_THRESHOLD {
            slots.retain(|_, s| s.live(now));
        }
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

    #[test]
    fn live_applies_asymmetric_ttl_by_outcome() {
        // Build slots at a fixed `created` and probe `live()` at now = created +
        // age. Add via `+` (never underflows) so this is clock-independent.
        let base = Instant::now();
        let mk = |value: Option<bool>| {
            let present = OnceCell::new();
            if let Some(v) = value {
                present.set(v).unwrap();
            }
            Slot { present, created: base }
        };
        let at = |secs| base + Duration::from_secs(secs);

        // In-flight (unresolved) stays live regardless of age - keeps a stalling
        // probe coalesced.
        assert!(mk(None).live(at(3600)));
        // Absent expires fast (NEGATIVE_TTL = 5s).
        assert!(mk(Some(false)).live(at(2)));
        assert!(!mk(Some(false)).live(at(30)));
        // Present is trusted long (POSITIVE_TTL = 60s) but not forever. The
        // at(30) case is what catches an accidental arm swap: it would fail if
        // present used the 5s negative TTL.
        assert!(mk(Some(true)).live(at(30)));
        assert!(!mk(Some(true)).live(at(120)));
    }

    #[tokio::test]
    async fn mark_present_seeds_a_hit_without_probing() {
        let cache = ProbeCache::default();
        cache.mark_present("abc");
        let calls = AtomicUsize::new(0);
        let present = cache
            .present("abc", || async {
                calls.fetch_add(1, Ordering::SeqCst);
                Ok::<bool, ()>(false)
            })
            .await;
        assert_eq!(present, Ok(true), "seeded key must read present");
        assert_eq!(calls.load(Ordering::SeqCst), 0, "seeded key must not probe S3");
    }

    #[tokio::test]
    async fn mark_present_overrides_a_cached_absent() {
        // The PUT-mid-burst case: a key probed absent, then written. Seeding
        // must clear the stale 404 so the next GET on this instance sees it.
        let cache = ProbeCache::default();
        assert_eq!(cache.present("abc", || async { Ok::<bool, ()>(false) }).await, Ok(false));
        cache.mark_present("abc");
        let present = cache
            .present("abc", || async { Ok::<bool, ()>(false) })
            .await;
        assert_eq!(present, Ok(true), "seed must override the cached absent");
    }
}
