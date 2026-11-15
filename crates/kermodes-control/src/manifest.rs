//! A commit's manifest, as `kermodes manifest` prints it (architecture.md §11.1). The controller
//! is a Tolerant Reader of it: it reads the fields it acts on and ignores the rest, which belong
//! to the run (`kermodes run` reads the project itself).

use serde::Deserialize;

/// Manifests from before apps were resources list them under `apps`, which is ignored: such a
/// commit runs no app until a newer one declares them as resources.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
pub struct Manifest {
    pub project: String,
    pub resources: Vec<Resource>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
pub struct Resource {
    pub name: String,
    /// `deployment`, `ingester`, or `app`, as the run reads it; the controller reports it and
    /// gives an app its frontend and its address.
    #[serde(default = "default_kind")]
    pub kind: String,
    pub update: Update,
    pub rollout: Rollout,
    /// Kubernetes quantities, as both requests and limits; manifests from before sizes have the
    /// defaults.
    #[serde(default = "default_cpu")]
    pub cpu: String,
    #[serde(default = "default_memory")]
    pub memory: String,
    /// Where the project declares it; manifests from before this have none.
    #[serde(default)]
    pub defined_at: Option<DefinedAt>,
    /// An app's frontend: its directory within the project, built per commit.
    #[serde(default)]
    pub path: Option<String>,
}

impl Resource {
    /// The directory of an app's frontend within the project; none for any other kind.
    pub fn app_path(&self) -> Option<&str> {
        if self.kind == "app" {
            self.path.as_deref()
        } else {
            None
        }
    }
}

/// A file within the project's directory, and a line in it.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
pub struct DefinedAt {
    pub file: String,
    pub line: u32,
}

fn default_kind() -> String {
    "deployment".into()
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
            // Without its directory there is nothing to build, and nothing for it to serve.
            if resource.kind == "app" && resource.path.is_none() {
                return Err(format!("the app `{}` names no path", resource.name));
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
    use super::{DefinedAt, Manifest};

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

    #[test]
    fn a_resource_says_where_it_is_declared_when_its_manifest_does() {
        let at = |extra: &str| manifest(extra).map(|m| m.resources[0].defined_at.clone());
        assert_eq!(at(""), Ok(None));
        assert_eq!(
            at(r#", "defined_at": {"file": "src/ticker/app.py", "line": 12}"#),
            Ok(Some(DefinedAt {
                file: "src/ticker/app.py".into(),
                line: 12,
            }))
        );
    }

    #[test]
    fn an_app_is_a_resource_with_a_frontend_and_old_app_lists_are_ignored() {
        let path =
            |extra: &str| manifest(extra).map(|m| m.resources[0].app_path().map(str::to_owned));
        assert_eq!(
            path(r#", "kind": "app", "function": "ticker.app:market", "path": "apps/market""#),
            Ok(Some("apps/market".into()))
        );
        // Only an app has a frontend, and an app always has one.
        assert_eq!(
            path(r#", "kind": "deployment", "path": "apps/market""#),
            Ok(None)
        );
        assert!(path(r#", "kind": "app""#).is_err());
        // A manifest stored before apps were resources still reads.
        let old = Manifest::parse(
            r#"{"project": "p", "resources": [], "apps": [{"name": "market", "path": "apps/m"}]}"#,
        );
        assert_eq!(old.map(|m| m.resources.len()), Ok(0));
    }
}
