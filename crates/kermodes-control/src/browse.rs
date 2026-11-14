//! Links to a project's code as its git host shows it: the Deployments tab opens a resource's
//! declaration at the commit it runs (architecture.md §15.2).

/// The page showing `path` (within the repository) at `commit` on the web host of `repository`
/// (its clone URL): a file scrolled to `line`, or a directory when `line` is `None`. GitHub and
/// GitLab have their own forms; any other host gets Gitea's, which Forgejo and Codeberg share.
/// `None` for a repository with no web host (a local path).
pub fn source_url(repository: &str, commit: &str, path: &str, line: Option<u32>) -> Option<String> {
    let (web, host) = web_root(repository)?;
    let path = path.trim_matches('/');
    let path = if path.is_empty() {
        String::new()
    } else {
        format!("/{path}")
    };
    let file = line.is_some();
    let anchor = line.map(|line| format!("#L{line}")).unwrap_or_default();
    let view = match host.as_str() {
        "github.com" if file => "blob",
        "github.com" => "tree",
        h if h == "gitlab.com" || h.starts_with("gitlab.") => {
            if file {
                "-/blob"
            } else {
                "-/tree"
            }
        }
        _ => "src/commit",
    };
    Some(format!("{web}/{view}/{commit}{path}{anchor}"))
}

/// The repository's web page and host from its clone URL: `http(s)://[user@]host[:port]/owner/repo
/// [.git]`, `ssh://[user@]host[:port]/owner/repo[.git]`, or `user@host:owner/repo[.git]`.
fn web_root(url: &str) -> Option<(String, String)> {
    let (scheme, rest) = if let Some(rest) = url.strip_prefix("https://") {
        ("https", rest)
    } else if let Some(rest) = url.strip_prefix("http://") {
        ("http", rest)
    } else if let Some(rest) = url.strip_prefix("ssh://") {
        ("https", rest)
    } else if !url.contains("://") && url.contains('@') && url.contains(':') {
        // scp-like: `git@host:owner/repo.git`
        let (authority, path) = url.split_once(':')?;
        let host = authority
            .rsplit_once('@')
            .map_or(authority, |(_, host)| host);
        return Some((
            format!("https://{host}/{}", repo_path(path)?),
            host.to_owned(),
        ));
    } else {
        return None;
    };
    let (authority, path) = rest.split_once('/')?;
    let authority = authority
        .rsplit_once('@')
        .map_or(authority, |(_, host)| host);
    // An ssh port is not the web's; an http(s) one is.
    let authority = match scheme {
        "https" if url.starts_with("ssh://") => authority.split(':').next()?,
        _ => authority,
    };
    let host = authority.split(':').next()?.to_owned();
    Some((format!("{scheme}://{authority}/{}", repo_path(path)?), host))
}

fn repo_path(path: &str) -> Option<&str> {
    let path = path.trim_matches('/');
    let path = path.strip_suffix(".git").unwrap_or(path);
    (!path.is_empty()).then_some(path)
}

#[cfg(test)]
mod tests {
    use super::source_url;

    #[test]
    fn links_a_file_or_directory_at_a_commit_in_each_hosts_form() {
        let cases: &[(&str, &str, Option<u32>, Option<&str>)] = &[
            // Gitea (and any host that is not GitHub or GitLab), with credentials in the URL.
            (
                "https://bot:token@git.rcolclough.com/rcolclough/kermodes.git",
                "examples/ticker/src/ticker/app.py",
                Some(12),
                Some(
                    "https://git.rcolclough.com/rcolclough/kermodes/src/commit/abc123/examples/ticker/src/ticker/app.py#L12",
                ),
            ),
            (
                "https://git.rcolclough.com/rcolclough/kermodes",
                "",
                None,
                Some("https://git.rcolclough.com/rcolclough/kermodes/src/commit/abc123"),
            ),
            (
                "git@github.com:org/repo.git",
                "src/app.py",
                Some(3),
                Some("https://github.com/org/repo/blob/abc123/src/app.py#L3"),
            ),
            (
                "https://github.com/org/repo",
                "pkg/",
                None,
                Some("https://github.com/org/repo/tree/abc123/pkg"),
            ),
            (
                "ssh://git@gitlab.example.com:2222/team/repo.git",
                "app.py",
                Some(1),
                Some("https://gitlab.example.com/team/repo/-/blob/abc123/app.py#L1"),
            ),
            // A web port is kept; an ssh port is not.
            (
                "http://localhost:3000/me/repo.git",
                "a.py",
                Some(9),
                Some("http://localhost:3000/me/repo/src/commit/abc123/a.py#L9"),
            ),
            // No web host.
            ("/srv/git/repo", "a.py", Some(1), None),
            ("file:///srv/git/repo", "a.py", Some(1), None),
        ];
        for (repository, path, line, expected) in cases {
            assert_eq!(
                source_url(repository, "abc123", path, *line).as_deref(),
                *expected,
                "{repository} {path}"
            );
        }
    }
}
