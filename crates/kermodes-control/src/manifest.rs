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
    /// Kubernetes quantities, as both requests and limits; manifests from before sizes have the
    /// defaults.
    #[serde(default = "default_cpu")]
    pub cpu: String,
    #[serde(default = "default_memory")]
    pub memory: String,
}

fn default_cpu() -> String {
    "500m".into()
}

fn default_memory() -> String {
    "512Mi".into()
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
            // Kubernetes would refuse the Deployment; refuse the commit instead, with the reason.
            if !cpu_quantity(&resource.cpu) || !memory_quantity(&resource.memory) {
                return Err(format!(
                    "`{}` asks for cpu {:?} and memory {:?}; cpu is cores (\"0.5\") or millicores \
                     (\"500m\"), memory bytes or a quantity (\"512Mi\", \"2Gi\")",
                    resource.name, resource.cpu, resource.memory
                ));
            }
        }
        Ok(manifest)
    }
}

/// `2`, `0.5`, or `500m`.
fn cpu_quantity(cpu: &str) -> bool {
    match cpu.strip_suffix('m') {
        Some(milli) => !milli.is_empty() && milli.bytes().all(|b| b.is_ascii_digit()),
        None => {
            let (whole, fraction) = cpu.split_once('.').unwrap_or((cpu, "0"));
            [whole, fraction]
                .iter()
                .all(|part| !part.is_empty() && part.bytes().all(|b| b.is_ascii_digit()))
        }
    }
}

/// Bytes, optionally with a binary (`Ki` .. `Ti`) or decimal (`k` .. `T`) suffix.
fn memory_quantity(memory: &str) -> bool {
    let digits = ["Ki", "Mi", "Gi", "Ti", "k", "M", "G", "T"]
        .iter()
        .find_map(|suffix| memory.strip_suffix(suffix))
        .unwrap_or(memory);
    !digits.is_empty() && digits.bytes().all(|b| b.is_ascii_digit())
}

#[cfg(test)]
mod tests {
    use super::Manifest;

    fn manifest(sizes: &str) -> Result<Manifest, String> {
        Manifest::parse(&format!(
            r#"{{"project": "p", "resources": [
                {{"name": "r", "update": "auto", "rollout": "rolling"{sizes}}}]}}"#
        ))
    }

    #[test]
    fn sizes_are_read_with_defaults_and_refused_when_kubernetes_would_refuse_them() {
        let sized = |s: &str| {
            manifest(s).map(|m| (m.resources[0].cpu.clone(), m.resources[0].memory.clone()))
        };
        // A manifest from before sizes runs with the defaults.
        assert_eq!(sized(""), Ok(("500m".into(), "512Mi".into())));
        for ok in [
            r#", "cpu": "2", "memory": "2Gi""#,
            r#", "cpu": "0.25", "memory": "1000000""#,
            r#", "cpu": "250m", "memory": "300M""#,
        ] {
            assert!(sized(ok).is_ok(), "{ok}");
        }
        for bad in [
            r#", "cpu": "fast""#,
            r#", "cpu": "m""#,
            r#", "cpu": "1.""#,
            r#", "memory": "2GB""#,
            r#", "memory": "Gi""#,
            r#", "memory": "-1""#,
        ] {
            assert!(sized(bad).is_err(), "{bad}");
        }
    }
}
