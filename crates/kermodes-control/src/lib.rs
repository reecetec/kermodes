//! `kermodes control` (architecture.md §16.2): syncs projects from git, discovers what each commit
//! declares, and runs every resource as a Kubernetes Deployment rolled out by its update policy
//! (§11.3–11.4). Decisions (what to apply, what each object looks like) are pure functions in
//! `plan`; the loop that talks to git, Postgres, and Kubernetes carries them out.

#![forbid(unsafe_code)]

mod git;
mod http;
mod k8s;
mod leader;
pub mod manifest;
pub mod plan;
mod run;
mod state;

pub use run::{Config, run, run_blocking};
