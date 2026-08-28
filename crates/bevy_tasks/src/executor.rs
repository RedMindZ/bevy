#![allow(unsafe_code)]

use std::collections::{BTreeMap, BinaryHeap};
use std::fmt;
use std::marker::PhantomData;
use std::panic::{RefUnwindSafe, UnwindSafe};
use std::pin::Pin;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, AtomicIsize, AtomicPtr, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, TryLockError};
use std::task::{Context, Poll, Waker};

use async_task::{Builder, Runnable};
use futures_lite::{future, prelude::*};
use slab::Slab;

use async_task::Task;

use crate::TaskPriority;

/// An async executor with task prioritization.
pub struct Executor<'a> {
    /// The executor state.
    state: AtomicPtr<State>,

    /// Makes the `'a` lifetime invariant.
    _marker: PhantomData<std::cell::UnsafeCell<&'a ()>>,
}

// SAFETY: Executor stores no thread local state that can be accessed via other thread.
unsafe impl Send for Executor<'_> {}
// SAFETY: Executor internally synchronizes all of it's operations internally.
unsafe impl Sync for Executor<'_> {}

impl UnwindSafe for Executor<'_> {}
impl RefUnwindSafe for Executor<'_> {}

impl fmt::Debug for Executor<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        debug_executor(self, "Executor", f)
    }
}

impl<'a> Executor<'a> {
    /// Creates a new executor.
    pub const fn new() -> Executor<'a> {
        Executor {
            state: AtomicPtr::new(std::ptr::null_mut()),
            _marker: PhantomData,
        }
    }

    /// Returns `true` if there are no unfinished tasks.
    pub fn is_empty(&self) -> bool {
        self.state().active().is_empty()
    }

    /// Get the target priority for tasks with a dynamic priority.
    ///
    /// Each time a task with dynamic priority is picked for execution,
    /// the task with the closest priority to the target priority will be picked.
    pub fn get_target_dynamic_priority(&self) -> isize {
        self.state().queue.get_target_dynamic_priority()
    }

    /// Set the target priority for tasks with a dynamic priority.
    ///
    /// Each time a task with dynamic priority is picked for execution,
    /// the task with the closest priority to the target priority will be picked.
    pub fn set_target_dynamic_priority(&self, value: isize) {
        self.state().queue.set_target_dynamic_priority(value);
    }

    /// Spawns a task onto the executor.
    pub fn spawn<T: Send + 'a>(
        &self,
        priority: TaskPriority,
        future: impl Future<Output = T> + Send + 'a,
    ) -> Task<T> {
        let mut active = self.state().active();

        // SAFETY: `T` and the future are `Send`.
        unsafe { self.spawn_inner(priority, future, &mut active) }
    }

    // /// Spawns many tasks onto the executor.
    // ///
    // /// As opposed to the [`spawn`] method, this locks the executor's inner task lock once and
    // /// spawns all of the tasks in one go. With large amounts of tasks this can improve
    // /// contention.
    // ///
    // /// For very large numbers of tasks the lock is occasionally dropped and re-acquired to
    // /// prevent runner thread starvation. It is assumed that the iterator provided does not
    // /// block; blocking iterators can lock up the internal mutex and therefore the entire
    // /// executor.
    // ///
    // /// [`spawn`]: Executor::spawn
    // pub fn spawn_many<T: Send + 'a, F: Future<Output = T> + Send + 'a>(
    //     &self,
    //     futures: impl IntoIterator<Item = (isize, F)>,
    //     handles: &mut impl Extend<Task<F::Output>>,
    // ) {
    //     let mut active = Some(self.state().active());

    //     // Convert the futures into tasks.
    //     let tasks = futures
    //         .into_iter()
    //         .enumerate()
    //         .map(move |(i, (priority, future))| {
    //             // SAFETY: `T` and the future are `Send`.
    //             let task = unsafe { self.spawn_inner(priority, future, active.as_mut().unwrap()) };

    //             // Yield the lock every once in a while to ease contention.
    //             if i.wrapping_sub(1) % 500 == 0 {
    //                 drop(active.take());
    //                 active = Some(self.state().active());
    //             }

    //             task
    //         });

    //     // Push the tasks to the user's collection.
    //     handles.extend(tasks);
    // }

    /// Spawn a future while holding the inner lock.
    ///
    /// # Safety
    ///
    /// If this is an `Executor`, `F` and `T` must be `Send`.
    unsafe fn spawn_inner<T: 'a>(
        &self,
        priority: TaskPriority,
        future: impl Future<Output = T> + 'a,
        active: &mut Slab<Waker>,
    ) -> Task<T> {
        // Remove the task from the set of active tasks when the future finishes.
        let entry = active.vacant_entry();
        let index = entry.key();
        let state = self.state_as_arc();
        let future = AsyncCallOnDrop::new(future, move || drop(state.active().try_remove(index)));

        // Create the task and register it in the set of active tasks.
        //
        // SAFETY:
        //
        // If `future` is not `Send`, this must be a `LocalExecutor` as per this
        // function's unsafe precondition. Since `LocalExecutor` is `!Sync`,
        // `try_tick`, `tick` and `run` can only be called from the origin
        // thread of the `LocalExecutor`. Similarly, `spawn` can only  be called
        // from the origin thread, ensuring that `future` and the executor share
        // the same origin thread. The `Runnable` can be scheduled from other
        // threads, but because of the above `Runnable` can only be called or
        // dropped on the origin thread.
        //
        // `future` is not `'static`, but we make sure that the `Runnable` does
        // not outlive `'a`. When the executor is dropped, the `active` field is
        // drained and all of the `Waker`s are woken. Then, the queue inside of
        // the `Executor` is drained of all of its runnables. This ensures that
        // runnables are dropped and this precondition is satisfied.
        //
        // `self.schedule()` is `Send`, `Sync` and `'static`, as checked below.
        // Therefore we do not need to worry about what is done with the
        // `Waker`.
        let (runnable, task) = unsafe {
            Builder::new()
                .propagate_panic(true)
                .spawn_unchecked(|()| future, self.schedule(priority))
        };
        entry.insert(runnable.waker());

        runnable.schedule();
        task
    }

    /// Attempts to run a task if at least one is scheduled.
    ///
    /// Running a scheduled task means simply polling its future once.
    pub fn try_tick(&self) -> bool {
        self.state().try_tick()
    }

    /// Runs a single task.
    ///
    /// Running a task means simply polling its future once.
    ///
    /// If no tasks are scheduled when this method is called, it will wait until one is scheduled.
    pub async fn tick(&self) {
        self.state().tick().await;
    }

    /// Runs the executor until the given future completes.
    pub async fn run<T>(&self, future: impl Future<Output = T>) -> T {
        self.state().run(future).await
    }

    /// Returns a function that schedules a runnable task when it gets woken up.
    fn schedule(&self, priority: TaskPriority) -> impl Fn(Runnable) + Send + Sync + 'static {
        let state = self.state_as_arc();

        // TODO: If possible, push into the current local queue and notify the ticker.
        move |runnable| {
            state.queue.push(priority, runnable);
            state.notify();
        }
    }

    /// Returns a pointer to the inner state.
    #[inline]
    fn state_ptr(&self) -> *const State {
        #[cold]
        fn alloc_state(atomic_ptr: &AtomicPtr<State>) -> *mut State {
            let state = Arc::new(State::new());
            // TODO: Switch this to use cast_mut once the MSRV can be bumped past 1.65
            let ptr = Arc::into_raw(state) as *mut State;
            if let Err(actual) = atomic_ptr.compare_exchange(
                std::ptr::null_mut(),
                ptr,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                // SAFETY: This was just created from Arc::into_raw.
                drop(unsafe { Arc::from_raw(ptr) });
                actual
            } else {
                ptr
            }
        }

        let mut ptr = self.state.load(Ordering::Acquire);
        if ptr.is_null() {
            ptr = alloc_state(&self.state);
        }
        ptr
    }

    /// Returns a reference to the inner state.
    #[inline]
    fn state(&self) -> &State {
        // SAFETY: So long as an Executor lives, it's state pointer will always be valid
        // when accessed through state_ptr.
        unsafe { &*self.state_ptr() }
    }

    // Clones the inner state Arc
    #[inline]
    fn state_as_arc(&self) -> Arc<State> {
        // SAFETY: So long as an Executor lives, it's state pointer will always be a valid
        // Arc when accessed through state_ptr.
        let arc = unsafe { Arc::from_raw(self.state_ptr()) };
        let clone = arc.clone();
        std::mem::forget(arc);
        clone
    }
}

impl Drop for Executor<'_> {
    fn drop(&mut self) {
        let ptr = *self.state.get_mut();
        if ptr.is_null() {
            return;
        }

        // SAFETY: As ptr is not null, it was allocated via Arc::new and converted
        // via Arc::into_raw in state_ptr.
        let state = unsafe { Arc::from_raw(ptr) };

        let mut active = state.active();
        for w in active.drain() {
            w.wake();
        }
        drop(active);

        state.queue.clear();
    }
}

impl<'a> Default for Executor<'a> {
    fn default() -> Executor<'a> {
        Executor::new()
    }
}

/// A thread-local executor.
///
/// The executor can only be run on the thread that created it.
pub struct LocalExecutor<'a> {
    /// The inner executor.
    inner: Executor<'a>,

    /// Makes the type `!Send` and `!Sync`.
    _marker: PhantomData<Rc<()>>,
}

impl UnwindSafe for LocalExecutor<'_> {}
impl RefUnwindSafe for LocalExecutor<'_> {}

impl fmt::Debug for LocalExecutor<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        debug_executor(&self.inner, "LocalExecutor", f)
    }
}

impl<'a> LocalExecutor<'a> {
    /// Creates a single-threaded executor.
    pub const fn new() -> LocalExecutor<'a> {
        LocalExecutor {
            inner: Executor::new(),
            _marker: PhantomData,
        }
    }

    /// Returns `true` if there are no unfinished tasks.
    pub fn is_empty(&self) -> bool {
        self.inner().is_empty()
    }

    /// Spawns a task onto the executor.
    pub fn spawn<T: 'a>(
        &self,
        priority: TaskPriority,
        future: impl Future<Output = T> + 'a,
    ) -> Task<T> {
        let mut active = self.inner().state().active();

        // SAFETY: This executor is not thread safe, so the future and its result
        //         cannot be sent to another thread.
        unsafe { self.inner().spawn_inner(priority, future, &mut active) }
    }

    /// Spawns many tasks onto the executor.
    ///
    /// As opposed to the [`spawn`] method, this locks the executor's inner task lock once and
    /// spawns all of the tasks in one go. With large amounts of tasks this can improve
    /// contention.
    ///
    /// It is assumed that the iterator provided does not block; blocking iterators can lock up
    /// the internal mutex and therefore the entire executor. Unlike [`Executor::spawn`], the
    /// mutex is not released, as there are no other threads that can poll this executor.
    ///
    /// [`spawn`]: LocalExecutor::spawn
    pub fn spawn_many<T: 'a, F: Future<Output = T> + 'a>(
        &self,
        futures: impl IntoIterator<Item = (TaskPriority, F)>,
        handles: &mut impl Extend<Task<F::Output>>,
    ) {
        let mut active = self.inner().state().active();

        // Convert all of the futures to tasks.
        let tasks = futures.into_iter().map(|(priority, future)| {
            // SAFETY: This executor is not thread safe, so the future and its result
            //         cannot be sent to another thread.
            unsafe { self.inner().spawn_inner(priority, future, &mut active) }

            // As only one thread can spawn or poll tasks at a time, there is no need
            // to release lock contention here.
        });

        // Push them to the user's collection.
        handles.extend(tasks);
    }

    /// Attempts to run a task if at least one is scheduled.
    ///
    /// Running a scheduled task means simply polling its future once.
    pub fn try_tick(&self) -> bool {
        self.inner().try_tick()
    }

    /// Runs a single task.
    ///
    /// Running a task means simply polling its future once.
    ///
    /// If no tasks are scheduled when this method is called, it will wait until one is scheduled.
    pub async fn tick(&self) {
        self.inner().tick().await
    }

    /// Runs the executor until the given future completes.
    pub async fn run<T>(&self, future: impl Future<Output = T>) -> T {
        self.inner().run(future).await
    }

    /// Returns a reference to the inner executor.
    fn inner(&self) -> &Executor<'a> {
        &self.inner
    }
}

impl<'a> Default for LocalExecutor<'a> {
    fn default() -> LocalExecutor<'a> {
        LocalExecutor::new()
    }
}

/// A [`Runnable`] that has a priority
struct PriorityRunnable<M = ()> {
    priority: isize,
    runnable: Runnable<M>,
}

impl<M> PriorityRunnable<M> {
    fn new(priority: isize, runnable: Runnable<M>) -> Self {
        Self { priority, runnable }
    }
}

impl<M> PartialEq for PriorityRunnable<M> {
    fn eq(&self, other: &Self) -> bool {
        self.priority == other.priority
    }
}

impl<M> Eq for PriorityRunnable<M> {}

impl<M> PartialOrd for PriorityRunnable<M> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.priority.partial_cmp(&other.priority)
    }
}

impl<M> Ord for PriorityRunnable<M> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.priority.cmp(&other.priority)
    }
}

/// Stores enqueued tasks based on their priority
/// and supports concurrent operations on the queue.
struct TaskQueue<M = ()> {
    high_static_queue: Mutex<BinaryHeap<PriorityRunnable<M>>>,
    low_dynamic_queue: Mutex<BTreeMap<isize, Vec<Runnable<M>>>>,
    target_dynamic_priority: AtomicIsize,
}

impl<M> TaskQueue<M> {
    const fn new() -> Self {
        Self {
            high_static_queue: Mutex::new(BinaryHeap::new()),
            low_dynamic_queue: Mutex::new(BTreeMap::new()),
            target_dynamic_priority: AtomicIsize::new(0),
        }
    }

    /// Gives mutable access to `high_static_queue` by locking the mutex while ignoring poison
    fn high_static_queue<T>(&self, f: impl FnOnce(&mut BinaryHeap<PriorityRunnable<M>>) -> T) -> T {
        f(&mut self
            .high_static_queue
            .lock()
            .unwrap_or_else(|e| e.into_inner()))
    }

    /// Gives mutable access to `low_dynamic_queue` by locking the mutex while ignoring poison
    fn low_dynamic_queue<T>(
        &self,
        f: impl FnOnce(&mut BTreeMap<isize, Vec<Runnable<M>>>) -> T,
    ) -> T {
        f(&mut self
            .low_dynamic_queue
            .lock()
            .unwrap_or_else(|e| e.into_inner()))
    }

    fn push(&self, priority: TaskPriority, runnable: Runnable<M>) {
        match priority {
            TaskPriority::HighStatic(priority) => {
                self.high_static_queue(|q| q.push(PriorityRunnable::new(priority, runnable)));
            }
            TaskPriority::LowDynamic(key) => {
                use std::collections::btree_map::Entry;
                self.low_dynamic_queue(|q| match q.entry(key) {
                    Entry::Vacant(entry) => _ = entry.insert(vec![runnable]),
                    Entry::Occupied(mut entry) => _ = entry.get_mut().push(runnable),
                });
            }
        }
    }

    fn pop(&self) -> Option<Runnable<M>> {
        if let Some(runnable) = self.high_static_queue(|q| q.pop()) {
            return Some(runnable.runnable);
        }

        let target = self.get_target_dynamic_priority();
        if let Some(runnable) = self.low_dynamic_queue(|q| {
            let mut cursor = q.lower_bound_mut(std::ops::Bound::Included(&target));

            let runnable_is_next = match (
                cursor.peek_prev().map(|p| *p.0),
                cursor.peek_next().map(|n| *n.0),
            ) {
                (Some(prev_priority), Some(next_priority)) => {
                    next_priority - target <= target - prev_priority
                }
                (Some(..), None) => false,
                (None, Some(..)) => true,
                (None, None) => return None,
            };

            let runnables = match runnable_is_next {
                true => cursor.peek_next().unwrap().1,
                false => cursor.peek_prev().unwrap().1,
            };

            debug_assert!(!runnables.is_empty());
            let runnable = runnables.pop();
            debug_assert!(runnable.is_some());
            if runnables.is_empty() {
                match runnable_is_next {
                    true => cursor.remove_next(),
                    false => cursor.remove_prev(),
                };
            }

            runnable
        }) {
            return Some(runnable);
        }

        None
    }

    fn get_target_dynamic_priority(&self) -> isize {
        self.target_dynamic_priority.load(Ordering::Relaxed)
    }

    fn set_target_dynamic_priority(&self, value: isize) {
        self.target_dynamic_priority.store(value, Ordering::Relaxed);
    }

    fn clear(&self) {
        self.high_static_queue(|q| q.clear());
        self.low_dynamic_queue(|q| q.clear());
    }

    fn len(&self) -> usize {
        self.high_static_queue(|q| q.len()) + self.low_dynamic_queue(|q| q.len())
    }
}

/// The state of a executor.
struct State {
    /// The task queue.
    queue: TaskQueue,

    /// Set to `true` when a sleeping ticker is notified or no tickers are sleeping.
    notified: AtomicBool,

    /// A list of sleeping tickers.
    sleepers: Mutex<Sleepers>,

    /// Currently active tasks.
    active: Mutex<Slab<Waker>>,
}

impl State {
    /// Creates state for a new executor.
    const fn new() -> State {
        State {
            queue: TaskQueue::new(),
            notified: AtomicBool::new(true),
            sleepers: Mutex::new(Sleepers {
                count: 0,
                wakers: Vec::new(),
                free_ids: Vec::new(),
            }),
            active: Mutex::new(Slab::new()),
        }
    }

    /// Returns a reference to currently active tasks.
    fn active(&self) -> MutexGuard<'_, Slab<Waker>> {
        self.active.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Notifies a sleeping ticker.
    #[inline]
    fn notify(&self) {
        if self
            .notified
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            let waker = self.sleepers.lock().unwrap().notify();
            if let Some(w) = waker {
                w.wake();
            }
        }
    }

    pub(crate) fn try_tick(&self) -> bool {
        match self.queue.pop() {
            None => false,
            Some(runnable) => {
                // Notify another ticker now to pick up where this ticker left off, just in case
                // running the task takes a long time.
                self.notify();

                // Run the task.
                runnable.run();
                true
            }
        }
    }

    pub(crate) async fn tick(&self) {
        let runnable = Ticker::new(self).runnable().await;
        runnable.run();
    }

    pub async fn run<T>(&self, future: impl Future<Output = T>) -> T {
        let mut ticker = Ticker::new(self);

        // A future that runs tasks forever.
        let run_forever = async {
            loop {
                for _ in 0..200 {
                    let runnable = ticker.runnable().await;
                    runnable.run();
                }
                future::yield_now().await;
            }
        };

        // Run `future` and `run_forever` concurrently until `future` completes.
        future.or(run_forever).await
    }
}

/// A list of sleeping tickers.
struct Sleepers {
    /// Number of sleeping tickers (both notified and unnotified).
    count: usize,

    /// IDs and wakers of sleeping unnotified tickers.
    ///
    /// A sleeping ticker is notified when its waker is missing from this list.
    wakers: Vec<(usize, Waker)>,

    /// Reclaimed IDs.
    free_ids: Vec<usize>,
}

impl Sleepers {
    /// Inserts a new sleeping ticker.
    fn insert(&mut self, waker: &Waker) -> usize {
        let id = match self.free_ids.pop() {
            Some(id) => id,
            None => self.count + 1,
        };
        self.count += 1;
        self.wakers.push((id, waker.clone()));
        id
    }

    /// Re-inserts a sleeping ticker's waker if it was notified.
    ///
    /// Returns `true` if the ticker was notified.
    fn update(&mut self, id: usize, waker: &Waker) -> bool {
        for item in &mut self.wakers {
            if item.0 == id {
                item.1.clone_from(waker);
                return false;
            }
        }

        self.wakers.push((id, waker.clone()));
        true
    }

    /// Removes a previously inserted sleeping ticker.
    ///
    /// Returns `true` if the ticker was notified.
    fn remove(&mut self, id: usize) -> bool {
        self.count -= 1;
        self.free_ids.push(id);

        for i in (0..self.wakers.len()).rev() {
            if self.wakers[i].0 == id {
                self.wakers.remove(i);
                return false;
            }
        }
        true
    }

    /// Returns `true` if a sleeping ticker is notified or no tickers are sleeping.
    fn is_notified(&self) -> bool {
        self.count == 0 || self.count > self.wakers.len()
    }

    /// Returns notification waker for a sleeping ticker.
    ///
    /// If a ticker was notified already or there are no tickers, `None` will be returned.
    fn notify(&mut self) -> Option<Waker> {
        if self.wakers.len() == self.count {
            self.wakers.pop().map(|item| item.1)
        } else {
            None
        }
    }
}

/// Runs task one by one.
struct Ticker<'a> {
    /// The executor state.
    state: &'a State,

    /// Set to a non-zero sleeper ID when in sleeping state.
    ///
    /// States a ticker can be in:
    /// 1) Woken.
    ///    2a) Sleeping and unnotified.
    ///    2b) Sleeping and notified.
    sleeping: usize,
}

impl Ticker<'_> {
    /// Creates a ticker.
    fn new(state: &State) -> Ticker<'_> {
        Ticker { state, sleeping: 0 }
    }

    /// Moves the ticker into sleeping and unnotified state.
    ///
    /// Returns `false` if the ticker was already sleeping and unnotified.
    fn sleep(&mut self, waker: &Waker) -> bool {
        let mut sleepers = self.state.sleepers.lock().unwrap();

        match self.sleeping {
            // Move to sleeping state.
            0 => {
                self.sleeping = sleepers.insert(waker);
            }

            // Already sleeping, check if notified.
            id => {
                if !sleepers.update(id, waker) {
                    return false;
                }
            }
        }

        self.state
            .notified
            .store(sleepers.is_notified(), Ordering::Release);

        true
    }

    /// Moves the ticker into woken state.
    fn wake(&mut self) {
        if self.sleeping != 0 {
            let mut sleepers = self.state.sleepers.lock().unwrap();
            sleepers.remove(self.sleeping);

            self.state
                .notified
                .store(sleepers.is_notified(), Ordering::Release);
        }
        self.sleeping = 0;
    }

    /// Waits for the next runnable task to run.
    async fn runnable(&mut self) -> Runnable {
        self.runnable_with(|| self.state.queue.pop()).await
    }

    /// Waits for the next runnable task to run, given a function that searches for a task.
    async fn runnable_with(&mut self, mut search: impl FnMut() -> Option<Runnable>) -> Runnable {
        future::poll_fn(|cx| {
            loop {
                match search() {
                    None => {
                        // Move to sleeping and unnotified state.
                        if !self.sleep(cx.waker()) {
                            // If already sleeping and unnotified, return.
                            return Poll::Pending;
                        }
                    }
                    Some(r) => {
                        // Wake up.
                        self.wake();

                        // Notify another ticker now to pick up where this ticker left off, just in
                        // case running the task takes a long time.
                        self.state.notify();

                        return Poll::Ready(r);
                    }
                }
            }
        })
        .await
    }
}

impl Drop for Ticker<'_> {
    fn drop(&mut self) {
        // If this ticker is in sleeping state, it must be removed from the sleepers list.
        if self.sleeping != 0 {
            let mut sleepers = self.state.sleepers.lock().unwrap();
            let notified = sleepers.remove(self.sleeping);

            self.state
                .notified
                .store(sleepers.is_notified(), Ordering::Release);

            // If this ticker was notified, then notify another ticker.
            if notified {
                drop(sleepers);
                self.state.notify();
            }
        }
    }
}

/// Debug implementation for `Executor` and `LocalExecutor`.
fn debug_executor(executor: &Executor<'_>, name: &str, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    // Get a reference to the state.
    let ptr = executor.state.load(Ordering::Acquire);
    if ptr.is_null() {
        // The executor has not been initialized.
        struct Uninitialized;

        impl fmt::Debug for Uninitialized {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("<uninitialized>")
            }
        }

        return f.debug_tuple(name).field(&Uninitialized).finish();
    }

    // SAFETY: If the state pointer is not null, it must have been
    // allocated properly by Arc::new and converted via Arc::into_raw
    // in state_ptr.
    let state = unsafe { &*ptr };

    debug_state(state, name, f)
}

/// Debug implementation for `Executor` and `LocalExecutor`.
fn debug_state(state: &State, name: &str, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    /// Debug wrapper for the number of active tasks.
    struct ActiveTasks<'a>(&'a Mutex<Slab<Waker>>);

    impl fmt::Debug for ActiveTasks<'_> {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self.0.try_lock() {
                Ok(lock) => fmt::Debug::fmt(&lock.len(), f),
                Err(TryLockError::WouldBlock) => f.write_str("<locked>"),
                Err(TryLockError::Poisoned(err)) => fmt::Debug::fmt(&err.into_inner().len(), f),
            }
        }
    }

    /// Debug wrapper for the sleepers.
    struct SleepCount<'a>(&'a Mutex<Sleepers>);

    impl fmt::Debug for SleepCount<'_> {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self.0.try_lock() {
                Ok(lock) => fmt::Debug::fmt(&lock.count, f),
                Err(TryLockError::WouldBlock) => f.write_str("<locked>"),
                Err(TryLockError::Poisoned(_)) => f.write_str("<poisoned>"),
            }
        }
    }

    f.debug_struct(name)
        .field("active", &ActiveTasks(&state.active))
        .field("tasks", &state.queue.len())
        .field("sleepers", &SleepCount(&state.sleepers))
        .finish()
}

/// Runs a closure when dropped.
struct CallOnDrop<F: FnMut()>(F);

impl<F: FnMut()> Drop for CallOnDrop<F> {
    fn drop(&mut self) {
        (self.0)();
    }
}

pin_project_lite::pin_project! {
    /// A wrapper around a future, running a closure when dropped.
    struct AsyncCallOnDrop<Fut, Cleanup: FnMut()> {
        #[pin]
        future: Fut,
        cleanup: CallOnDrop<Cleanup>,
    }
}

impl<Fut, Cleanup: FnMut()> AsyncCallOnDrop<Fut, Cleanup> {
    fn new(future: Fut, cleanup: Cleanup) -> Self {
        Self {
            future,
            cleanup: CallOnDrop(cleanup),
        }
    }
}

impl<Fut: Future, Cleanup: FnMut()> Future for AsyncCallOnDrop<Fut, Cleanup> {
    type Output = Fut::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.project().future.poll(cx)
    }
}

fn _ensure_send_and_sync() {
    use futures_lite::future::pending;

    fn is_send<T: Send>(_: T) {}
    fn is_sync<T: Sync>(_: T) {}
    fn is_static<T: 'static>(_: T) {}

    is_send::<Executor<'_>>(Executor::new());
    is_sync::<Executor<'_>>(Executor::new());

    let ex = Executor::new();
    is_send(ex.run(pending::<()>()));
    is_sync(ex.run(pending::<()>()));
    is_send(ex.tick());
    is_sync(ex.tick());
    is_send(ex.schedule(TaskPriority::HighStatic(0)));
    is_sync(ex.schedule(TaskPriority::HighStatic(0)));
    is_static(ex.schedule(TaskPriority::HighStatic(0)));

    /// ```compile_fail
    /// use async_executor::LocalExecutor;
    /// use futures_lite::future::pending;
    ///
    /// fn is_send<T: Send>(_: T) {}
    /// fn is_sync<T: Sync>(_: T) {}
    ///
    /// is_send::<LocalExecutor<'_>>(LocalExecutor::new());
    /// is_sync::<LocalExecutor<'_>>(LocalExecutor::new());
    ///
    /// let ex = LocalExecutor::new();
    /// is_send(ex.run(pending::<()>()));
    /// is_sync(ex.run(pending::<()>()));
    /// is_send(ex.tick());
    /// is_sync(ex.tick());
    /// ```
    fn _negative_test() {}
}
