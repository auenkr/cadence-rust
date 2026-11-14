//! Opt-in diagnostics that retain the production System allocator.
use std::{
    alloc::{GlobalAlloc, Layout, System},
    cell::Cell,
    collections::HashMap,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Mutex, OnceLock,
    },
    time::Duration,
};

#[global_allocator]
static ALLOCATOR: ProfiledSystem = ProfiledSystem;
static LIVE: AtomicUsize = AtomicUsize::new(0);
static ENABLED: AtomicUsize = AtomicUsize::new(0);
type Stack = [usize; 32];
type Samples = HashMap<usize, (usize, Stack)>;
static SAMPLES: OnceLock<Mutex<Samples>> = OnceLock::new();
thread_local! {
    static RECORDING: Cell<bool> = const { Cell::new(false) };
    static BYTES: Cell<usize> = const { Cell::new(0) };
}
struct Guard;
impl Guard {
    fn enter() -> Option<Self> {
        RECORDING
            .try_with(|active| {
                if active.replace(true) {
                    None
                } else {
                    Some(Self)
                }
            })
            .ok()
            .flatten()
    }
}
impl Drop for Guard {
    fn drop(&mut self) {
        let _ = RECORDING.try_with(|active| active.set(false));
    }
}
fn forget(pointer: *mut u8) {
    if ENABLED.load(Ordering::Relaxed) == 0 {
        return;
    }
    let Some(_guard) = Guard::enter() else {
        return;
    };
    if let Some(samples) = SAMPLES.get() {
        if let Ok(mut samples) = samples.lock() {
            samples.remove(&(pointer as usize));
        }
    }
}
fn record(pointer: *mut u8, size: usize) {
    if ENABLED.load(Ordering::Relaxed) == 0 {
        return;
    }
    let Some(_guard) = Guard::enter() else {
        return;
    };
    let selected = BYTES
        .try_with(|bytes| {
            let next = bytes.get().saturating_add(size);
            bytes.set(next % 65536);
            next >= 65536
        })
        .unwrap_or(false);
    if !selected {
        return;
    }
    let mut stack = [0; 32];
    let mut index = 0;
    backtrace::trace(|frame| {
        if index == stack.len() {
            return false;
        }
        stack[index] = frame.ip() as usize;
        index += 1;
        true
    });
    if let Some(samples) = SAMPLES.get() {
        if let Ok(mut samples) = samples.lock() {
            samples.insert(pointer as usize, (size, stack));
        }
    }
}
struct ProfiledSystem;
// Each operation delegates to System with the original pointer/layout. Counters
// change only on successful allocation. Recursive profiler allocations are
// counted but excluded from sampling, avoiding allocator/mutex recursion.
unsafe impl GlobalAlloc for ProfiledSystem {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let pointer = System.alloc(layout);
        if !pointer.is_null() {
            LIVE.fetch_add(layout.size(), Ordering::Relaxed);
            record(pointer, layout.size());
        }
        pointer
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let pointer = System.alloc_zeroed(layout);
        if !pointer.is_null() {
            LIVE.fetch_add(layout.size(), Ordering::Relaxed);
            record(pointer, layout.size());
        }
        pointer
    }
    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        forget(pointer);
        LIVE.fetch_sub(layout.size(), Ordering::Relaxed);
        System.dealloc(pointer, layout);
    }
    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        let next = System.realloc(pointer, layout, size);
        if !next.is_null() {
            forget(pointer);
            if size >= layout.size() {
                LIVE.fetch_add(size - layout.size(), Ordering::Relaxed);
            } else {
                LIVE.fetch_sub(layout.size() - size, Ordering::Relaxed);
            }
            record(next, size);
        }
        next
    }
}
pub fn start() {
    let Some(directory) = std::env::var_os("CADENCE_HEAP_PROFILE_DIR") else {
        return;
    };
    SAMPLES.get_or_init(|| Mutex::new(HashMap::new()));
    ENABLED.store(1, Ordering::Relaxed);
    std::thread::spawn(move || {
        let directory = std::path::PathBuf::from(directory);
        let mut previous = 0;
        for seconds in [180, 1200, 2400, 4000] {
            std::thread::sleep(Duration::from_secs(seconds - previous));
            previous = seconds;
            // Snapshot under the recursion guard; symbolization runs after the
            // soak, so debug-info caches cannot contaminate process RSS.
            let Some(_guard) = Guard::enter() else {
                return;
            };
            let live = LIVE.load(Ordering::Relaxed);
            let samples: Vec<_> = SAMPLES
                .get()
                .unwrap()
                .lock()
                .unwrap()
                .values()
                .copied()
                .collect();
            let mut info = std::mem::MaybeUninit::<libc::Dl_info>::uninit();
            let base = unsafe {
                if libc::dladdr(start as *const () as *const libc::c_void, info.as_mut_ptr()) == 0 {
                    0
                } else {
                    info.assume_init().dli_fbase as usize
                }
            };
            let malloc = unsafe { libc::mallinfo2() };
            let stats = format!("seconds={seconds} live_requested={live} sampled_live={} arena={} uordblks={} fordblks={} hblkhd={} keepcost={}\n", samples.len(), malloc.arena, malloc.uordblks, malloc.fordblks, malloc.hblkhd, malloc.keepcost);
            eprint!("{stats}");
            let _ = std::fs::write(directory.join(format!("stats-{seconds:04}.txt")), stats);
            let data = serde_json::json!({"base":base,"samples":samples});
            let _ = std::fs::write(
                directory.join(format!("system-{seconds:04}.json")),
                data.to_string(),
            );
            let _ = std::fs::copy(
                "/proc/self/smaps_rollup",
                directory.join(format!("smaps-{seconds:04}.txt")),
            );
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sampled_system_allocations_are_removed_on_reallocation_and_free() {
        SAMPLES.get_or_init(|| Mutex::new(HashMap::new()));
        ENABLED.store(1, Ordering::Relaxed);
        let layout = Layout::from_size_align(131072, 16).unwrap();
        unsafe {
            let pointer = std::alloc::alloc_zeroed(layout);
            assert!(!pointer.is_null());
            {
                let _guard = Guard::enter().unwrap();
                assert_eq!(
                    SAMPLES
                        .get()
                        .unwrap()
                        .lock()
                        .unwrap()
                        .get(&(pointer as usize))
                        .unwrap()
                        .0,
                    layout.size()
                );
            }
            let next = std::alloc::realloc(pointer, layout, 262144);
            assert!(!next.is_null());
            {
                let _guard = Guard::enter().unwrap();
                let samples = SAMPLES.get().unwrap().lock().unwrap();
                assert_eq!(samples.get(&(next as usize)).unwrap().0, 262144);
                if next != pointer {
                    assert!(!samples.contains_key(&(pointer as usize)));
                }
            }
            std::alloc::dealloc(next, Layout::from_size_align(262144, 16).unwrap());
            let _guard = Guard::enter().unwrap();
            assert!(!SAMPLES
                .get()
                .unwrap()
                .lock()
                .unwrap()
                .contains_key(&(next as usize)));
        }
    }
}
