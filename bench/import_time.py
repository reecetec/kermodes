"""The engine.md startup target: `import ursid; us.local()` in a fresh interpreter, < 100 ms.

    uv run --no-sync python bench/import_time.py

Times both steps inside seven fresh interpreters and reports the median.
"""

import statistics
import subprocess
import sys

PROBE = (
    "import time; start = time.perf_counter(); import ursid as us; s = us.local(); "
    "print((time.perf_counter() - start) * 1000)"
)


def main() -> None:
    runs = sorted(
        float(subprocess.check_output([sys.executable, "-c", PROBE])) for _ in range(7)
    )
    print(
        f"import ursid + us.local(): median {statistics.median(runs):.1f} ms "
        f"(runs: {', '.join(f'{r:.1f}' for r in runs)}); target < 100 ms"
    )


if __name__ == "__main__":
    main()
