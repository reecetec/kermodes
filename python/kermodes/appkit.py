"""The platform's setup of an app's page (architecture.md §15.2): the Vite config, the HTML, and
the entry that mounts the app's `App.tsx` in Kermodes' provider and theme. An app holds only its
own code; `kermodes dev` and the cluster's build Job write this kit beside it, into the
project's `.kermodes/apps/<name>/`, and run Vite with it."""

import html
import shutil
from pathlib import Path

import kermodes

KIT = Path(__file__).parent / "app_kit"


def write(dest: Path, app: Path, name: str, sources: list[Path]) -> None:
    """Writes the kit for the app `name` in `app` into `dest`. Tailwind finds the classes the
    page uses in `app` and in `sources` (the packages it builds on)."""
    dest.mkdir(parents=True, exist_ok=True)
    for file in ("vite.config.ts", "main.tsx"):
        shutil.copyfile(KIT / file, dest / file)
    page = (KIT / "index.html").read_text().replace("%TITLE%", html.escape(name))
    (dest / "index.html").write_text(page)
    scanned = "".join(f'@source "{path}";\n' for path in [app, *sources])
    (dest / "app.css").write_text((KIT / "app.css").read_text() + scanned)


def checkout_web() -> Path | None:
    """The `web/` of the Kermodes checkout this package runs from, if it does: its packages'
    sources, for `kermodes dev` to serve apps from."""
    web = Path(kermodes.__file__).resolve().parents[2] / "web"
    return web if (web / "react" / "src" / "index.tsx").exists() else None
