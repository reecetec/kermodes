"""The memory the OS charges a process, the same way for every engine.

On macOS that is the physical footprint (what Activity Monitor shows and memory pressure acts
on): RSS there still counts pages an allocator has handed back with `madvise(MADV_FREE)` until
the system reclaims them, which mimalloc and jemalloc do and the system allocator mostly doesn't.
Elsewhere it is RSS, which such pages leave at once.
"""

import ctypes
import os
import sys


class _RusageInfoV2(ctypes.Structure):
    # struct rusage_info_v2 from <sys/resource.h>: the kernel writes all of it.
    _fields_ = [("uuid", ctypes.c_uint8 * 16)] + [
        (name, ctypes.c_uint64)
        for name in (
            "user_time",
            "system_time",
            "pkg_idle_wkups",
            "interrupt_wkups",
            "pageins",
            "wired_size",
            "resident_size",
            "phys_footprint",
            "proc_start_abstime",
            "proc_exit_abstime",
            "child_user_time",
            "child_system_time",
            "child_pkg_idle_wkups",
            "child_interrupt_wkups",
            "child_pageins",
            "child_elapsed_abstime",
            "diskio_bytesread",
            "diskio_byteswritten",
        )
    ]


_RUSAGE_INFO_V2 = 2

if sys.platform == "darwin":
    _libc = ctypes.CDLL(None)
    _info = _RusageInfoV2()

    def resident() -> int:
        """Bytes of physical footprint."""
        if (
            _libc.proc_pid_rusage(os.getpid(), _RUSAGE_INFO_V2, ctypes.byref(_info))
            != 0
        ):
            raise OSError(ctypes.get_errno(), "proc_pid_rusage failed")
        return _info.phys_footprint

else:
    import psutil

    _process = psutil.Process()

    def resident() -> int:
        """Bytes of resident memory."""
        return _process.memory_info().rss
