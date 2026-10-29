//! Job queue, scheduler and runners.
//!
//! A [`Runner`] decides where a job executes: in this process (Phase 0 and
//! the CLI), on a Kubernetes pool in the customer's account, or on our
//! hosted fleet. The control plane only ever talks to this trait.

use infostash_model::{Job, JobOutcome, Pool};

pub trait Runner: Send + Sync {
    fn name(&self) -> &str;

    /// Whether this runner can serve a pool right now. A scaled-to-zero
    /// pool still returns true; the runner wakes it.
    fn serves(&self, pool: Pool) -> bool;

    /// Execute one job to completion. The runner owns retries of individual
    /// tasks; a `Failed { retryable: true }` outcome means the whole job
    /// may be requeued.
    fn run(&self, job: &Job) -> JobOutcome;
}

/// Pick the first runner that serves the job's pool.
pub fn route<'a>(runners: &'a [Box<dyn Runner>], job: &Job) -> Option<&'a dyn Runner> {
    runners
        .iter()
        .map(|r| r.as_ref())
        .find(|r| r.serves(job.pool))
}

#[cfg(test)]
mod tests {
    use super::*;
    use infostash_model::{JobId, Layer, ProjectVersion, TableRef};

    struct Only(Pool);

    impl Runner for Only {
        fn name(&self) -> &str {
            "only"
        }
        fn serves(&self, pool: Pool) -> bool {
            pool == self.0
        }
        fn run(&self, _: &Job) -> JobOutcome {
            JobOutcome::NoChange
        }
    }

    fn job(pool: Pool) -> Job {
        Job {
            id: JobId(1),
            project: ProjectVersion("abc".into()),
            target: TableRef::new(Layer::Ready, "daily_revenue"),
            inputs: vec![],
            pool,
        }
    }

    #[test]
    fn routes_by_pool() {
        let runners: Vec<Box<dyn Runner>> =
            vec![Box::new(Only(Pool::Ingest)), Box::new(Only(Pool::Bi))];
        let job = job(Pool::Bi);
        let runner = route(&runners, &job).unwrap();
        assert!(runner.serves(Pool::Bi));
        assert_eq!(runner.name(), "only");
        assert_eq!(runner.run(&job), JobOutcome::NoChange);
    }

    #[test]
    fn no_runner_for_unserved_pool() {
        let runners: Vec<Box<dyn Runner>> = vec![Box::new(Only(Pool::Ingest))];
        assert!(route(&runners, &job(Pool::Agents)).is_none());
    }
}
