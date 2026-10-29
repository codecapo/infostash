//! Distributed execution: a query plan is cut into stages at shuffle
//! boundaries, and each stage into tasks over disjoint slices of input.
//!
//! Phase 0 runs every task in-process. The model stays the same when tasks
//! move to remote executors, so scaling out changes the runner, not the plan.

use infostash_model::Result;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct StageId(pub u32);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TaskId {
    pub stage: StageId,
    pub partition: u32,
}

/// How a stage's output is handed to the next stage.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Exchange {
    /// Hash rows on these columns into `partitions` buckets.
    Hash {
        columns: Vec<String>,
        partitions: u32,
    },
    /// Everything to one task (final sort, small aggregates).
    Single,
    /// Output is the final result (written to a table or returned).
    Sink,
}

/// One piece of a distributed plan.
pub trait ExecutionStage: Send + Sync {
    fn id(&self) -> StageId;

    /// Stages whose output this stage reads. Empty for scan stages.
    fn depends_on(&self) -> &[StageId];

    /// Number of tasks; for scan stages this comes from file pruning.
    fn task_count(&self) -> u32;

    fn output(&self) -> &Exchange;

    /// Run one task. Must be safe to retry: a task writes only to its own
    /// shuffle or staging location, never to a committed table.
    fn run_task(&self, task: TaskId) -> Result<TaskStats>;
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TaskStats {
    pub rows_in: u64,
    pub rows_out: u64,
    pub bytes_spilled: u64,
}

/// Retry policy for failed tasks. A lost shuffle output means the producing
/// stage reruns.
#[derive(Debug, Clone, Copy)]
pub struct RetryPolicy {
    pub max_task_attempts: u32,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_task_attempts: 3,
        }
    }
}

/// Order stages so every stage runs after the stages it depends on.
pub fn schedule(stages: &[&dyn ExecutionStage]) -> Result<Vec<StageId>> {
    let mut done: Vec<StageId> = Vec::with_capacity(stages.len());
    while done.len() < stages.len() {
        let ready = stages
            .iter()
            .find(|s| !done.contains(&s.id()) && s.depends_on().iter().all(|d| done.contains(d)));
        match ready {
            Some(s) => done.push(s.id()),
            None => {
                return Err(infostash_model::Error::Plan(
                    "stage graph has a cycle or a missing dependency".into(),
                ));
            }
        }
    }
    Ok(done)
}

/// Run every task of every stage in this process, retrying failed tasks.
pub fn run_in_process(stages: &[&dyn ExecutionStage], retry: RetryPolicy) -> Result<TaskStats> {
    let mut total = TaskStats::default();
    for id in schedule(stages)? {
        let stage = stages.iter().find(|s| s.id() == id).ok_or_else(|| {
            infostash_model::Error::Plan(format!("scheduled stage {id:?} not found"))
        })?;
        for partition in 0..stage.task_count() {
            let task = TaskId {
                stage: id,
                partition,
            };
            let mut attempt = 1;
            let stats = loop {
                match stage.run_task(task) {
                    Ok(s) => break s,
                    Err(_) if attempt < retry.max_task_attempts => attempt += 1,
                    Err(e) => return Err(e),
                }
            };
            total.rows_in += stats.rows_in;
            total.rows_out += stats.rows_out;
            total.bytes_spilled += stats.bytes_spilled;
        }
    }
    Ok(total)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};

    struct Fake {
        id: StageId,
        deps: Vec<StageId>,
        tasks: u32,
        fail_first: AtomicU32,
    }

    impl ExecutionStage for Fake {
        fn id(&self) -> StageId {
            self.id
        }
        fn depends_on(&self) -> &[StageId] {
            &self.deps
        }
        fn task_count(&self) -> u32 {
            self.tasks
        }
        fn output(&self) -> &Exchange {
            &Exchange::Single
        }
        fn run_task(&self, _: TaskId) -> Result<TaskStats> {
            if self.fail_first.load(Ordering::SeqCst) > 0 {
                self.fail_first.fetch_sub(1, Ordering::SeqCst);
                return Err(infostash_model::Error::Execution("transient".into()));
            }
            Ok(TaskStats {
                rows_in: 10,
                rows_out: 5,
                bytes_spilled: 0,
            })
        }
    }

    fn stage(id: u32, deps: &[u32], tasks: u32, fail_first: u32) -> Fake {
        Fake {
            id: StageId(id),
            deps: deps.iter().map(|d| StageId(*d)).collect(),
            tasks,
            fail_first: AtomicU32::new(fail_first),
        }
    }

    #[test]
    fn schedules_dependencies_first() {
        let (a, b) = (stage(2, &[1], 1, 0), stage(1, &[], 1, 0));
        assert_eq!(schedule(&[&a, &b]).unwrap(), vec![StageId(1), StageId(2)]);
    }

    #[test]
    fn rejects_cycles() {
        let (a, b) = (stage(1, &[2], 1, 0), stage(2, &[1], 1, 0));
        assert!(schedule(&[&a, &b]).is_err());
    }

    #[test]
    fn retries_transient_task_failures() {
        let scan = stage(1, &[], 4, 2);
        let agg = stage(2, &[1], 1, 0);
        let stats = run_in_process(&[&scan, &agg], RetryPolicy::default()).unwrap();
        assert_eq!(stats.rows_in, 50);
        assert_eq!(scan.output(), &Exchange::Single);
    }

    #[test]
    fn fails_after_last_attempt() {
        let scan = stage(1, &[], 1, 3);
        assert!(run_in_process(&[&scan], RetryPolicy::default()).is_err());
    }
}
