//! A commit's manifest, as `kermodes manifest` prints it (architecture.md §11.1). The controller
//! is a Tolerant Reader of it: it reads the fields it acts on and ignores the rest, which belong
//! to the run (`kermodes run` reads the project itself).

use serde::Deserialize;

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
pub struct Manifest {
    pub project: String,
    pub resources: Vec<Resource>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
pub struct Resource {
    pub name: String,
    pub update: Update,
    pub rollout: Rollout,
}

/// When a synced commit reaches a running resource (§11.4).
#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Update {
    Auto,
    Manual,
    OnStart,
}

/// How a resource's run is replaced (§11.4).
#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Rollout {
    Rolling,
    Recreate,
}

impl Manifest {
    pub fn parse(json: &str) -> Result<Self, String> {
        let manifest: Manifest =
            serde_json::from_str(json).map_err(|e| format!("an unreadable manifest: {e}"))?;
        for resource in &manifest.resources {
            if resource.update == Update::OnStart {
                return Err(format!(
                    "`{}` has update=\"on_start\", which needs schedules; they do not exist yet",
                    resource.name
                ));
            }
        }
        Ok(manifest)
    }
}
