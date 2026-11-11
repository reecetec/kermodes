//! Git, from any host (architecture.md §11.7): the `git` command over HTTPS or SSH, so whatever
//! hosts a project needs nothing of its own. Credentials go in an HTTP header, never the URL, and
//! never reach the pods that run the code: they get snapshots.

use std::path::Path;
use std::process::Command;

use base64::Engine;

/// HTTP basic credentials for a project's repository (a username and a token).
#[derive(Clone)]
pub struct Credentials {
    pub username: String,
    pub password: String,
}

/// A project's mainline: where to find it, and which directory is the project.
#[derive(Clone, Debug)]
pub struct Repository {
    pub url: String,
    pub branch: String,
    /// The project's directory within the repository; empty for the root.
    pub path: String,
}

/// The commit at the head of the mainline, without fetching anything.
pub fn head(repo: &Repository, credentials: Option<&Credentials>) -> Result<String, String> {
    let output = git(
        None,
        credentials,
        &["ls-remote", &repo.url, &branch_ref(repo)],
    )?;
    output
        .split_whitespace()
        .next()
        .map(str::to_owned)
        .ok_or_else(|| format!("{} has no branch {}", repo.url, repo.branch))
}

/// A project's directory at one commit.
pub struct Snapshot {
    pub commit: String,
    /// The directory's git tree: two commits with the same tree have the same code.
    pub tree: String,
    /// Its files as a tar, paths relative to the repository's root.
    pub tar: Vec<u8>,
}

/// The project's directory at the mainline's head.
pub fn snapshot(
    repo: &Repository,
    credentials: Option<&Credentials>,
    scratch: &Path,
) -> Result<Snapshot, String> {
    let dir = scratch
        .to_str()
        .ok_or("a scratch directory that is not UTF-8")?;
    git(None, None, &["init", "--quiet", dir])?;
    git(
        Some(scratch),
        credentials,
        &[
            "fetch",
            "--quiet",
            "--depth",
            "1",
            &repo.url,
            &branch_ref(repo),
        ],
    )?;
    let commit = git(Some(scratch), None, &["rev-parse", "FETCH_HEAD"])?
        .trim()
        .to_owned();
    let path = repo.path.trim_matches('/');
    let tree = git(
        Some(scratch),
        None,
        &["rev-parse", &format!("FETCH_HEAD:{path}")],
    )
    .map_err(|e| format!("{} has no directory `{path}` at {commit}: {e}", repo.url))?
    .trim()
    .to_owned();
    let mut args = vec!["archive", "--format=tar", "FETCH_HEAD"];
    if !path.is_empty() {
        args.push(path);
    }
    let tar = run(git_command(Some(scratch), None, &args))?;
    Ok(Snapshot { commit, tree, tar })
}

fn branch_ref(repo: &Repository) -> String {
    format!("refs/heads/{}", repo.branch)
}

fn git(
    dir: Option<&Path>,
    credentials: Option<&Credentials>,
    args: &[&str],
) -> Result<String, String> {
    let output = run(git_command(dir, credentials, args))?;
    String::from_utf8(output).map_err(|_| format!("git {} printed non-UTF-8", args[0]))
}

fn git_command(dir: Option<&Path>, credentials: Option<&Credentials>, args: &[&str]) -> Command {
    let mut command = Command::new("git");
    if let Some(dir) = dir {
        command.current_dir(dir);
    }
    // Never prompt: a missing credential is an error, not a hang.
    command.env("GIT_TERMINAL_PROMPT", "0");
    if let Some(credentials) = credentials {
        let token = base64::engine::general_purpose::STANDARD
            .encode(format!("{}:{}", credentials.username, credentials.password));
        command.args([
            "-c",
            &format!("http.extraHeader=Authorization: Basic {token}"),
        ]);
    }
    command.args(args);
    command
}

fn run(mut command: Command) -> Result<Vec<u8>, String> {
    let output = command
        .output()
        .map_err(|e| format!("cannot run git: {e}"))?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(format!("git failed: {}", stderr.trim()));
    }
    Ok(output.stdout)
}

#[cfg(test)]
mod tests {
    use std::path::{Path, PathBuf};
    use std::process::Command;

    use super::{Repository, head, snapshot};

    fn scratch(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("kermodes-git-{}-{name}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn sh(dir: &Path, args: &[&str]) -> String {
        let output = Command::new("git")
            .current_dir(dir)
            .args(args)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8(output.stdout).unwrap().trim().to_owned()
    }

    fn commit(dir: &Path, path: &str, text: &str) -> String {
        let file = dir.join(path);
        std::fs::create_dir_all(file.parent().unwrap()).unwrap();
        std::fs::write(file, text).unwrap();
        sh(dir, &["add", "."]);
        sh(
            dir,
            &[
                "-c",
                "user.name=t",
                "-c",
                "user.email=t@t",
                "commit",
                "-qm",
                text,
            ],
        );
        sh(dir, &["rev-parse", "HEAD"])
    }

    /// A project in a repository: the mainline's head, and its directory alone as a snapshot.
    #[test]
    fn a_snapshot_is_the_projects_directory_at_the_mainline_head() {
        let origin = scratch("origin");
        sh(&origin, &["init", "-q", "-b", "main"]);
        commit(&origin, "other/x.py", "not the project");
        let first = commit(&origin, "examples/ticker/kermodes.toml", "one");
        let repo = Repository {
            url: format!("file://{}", origin.display()),
            branch: "main".into(),
            path: "examples/ticker".into(),
        };
        assert_eq!(head(&repo, None).unwrap(), first);

        let second = commit(&origin, "examples/ticker/src/app.py", "two");
        assert_eq!(head(&repo, None).unwrap(), second);
        let work = scratch("work");
        let taken = snapshot(&repo, None, &work).unwrap();
        assert_eq!(taken.commit, second);
        let tar = taken.tar;
        let listing = Command::new("tar")
            .args(["-t"])
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .spawn()
            .and_then(|mut child| {
                use std::io::Write;
                child.stdin.take().unwrap().write_all(&tar)?;
                child.wait_with_output()
            })
            .unwrap();
        let mut files: Vec<String> = String::from_utf8(listing.stdout)
            .unwrap()
            .lines()
            .filter(|l| !l.ends_with('/'))
            .map(str::to_owned)
            .collect();
        files.sort();
        assert_eq!(
            files,
            [
                "examples/ticker/kermodes.toml",
                "examples/ticker/src/app.py"
            ]
        );

        // A commit elsewhere in the repository leaves the project's tree as it was; one inside
        // the project changes it.
        commit(&origin, "other/y.py", "still not the project");
        let elsewhere = snapshot(&repo, None, &scratch("elsewhere")).unwrap();
        assert_ne!(elsewhere.commit, taken.commit);
        assert_eq!(elsewhere.tree, taken.tree);
        commit(&origin, "examples/ticker/src/app.py", "three");
        let inside = snapshot(&repo, None, &scratch("inside")).unwrap();
        assert_ne!(inside.tree, taken.tree);
        let gone = Repository {
            path: "examples/nope".into(),
            ..repo.clone()
        };
        assert!(snapshot(&gone, None, &scratch("gone")).is_err());

        let missing = Repository {
            branch: "nope".into(),
            ..repo
        };
        assert!(head(&missing, None).is_err());
        for dir in ["origin", "work", "elsewhere", "inside", "gone"] {
            let _ = std::fs::remove_dir_all(
                std::env::temp_dir().join(format!("kermodes-git-{}-{dir}", std::process::id())),
            );
        }
    }
}
