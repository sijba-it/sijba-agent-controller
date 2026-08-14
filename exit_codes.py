"""Exit-code convention between controller.py and plugin processes (see
catalog.py — every plugin, including claude/cursor, is its own repo that
embeds these same two numbers rather than importing this module).

A session process signals *why* it stopped through its own exit code instead
of through a side-channel message — the controller supervises the OS process
directly (`await proc.wait()`), so the exit code is the one signal it is
guaranteed to see no matter how the process died.

Anything other than these two reserved codes (0, 1, killed-by-signal 128+N,
uncaught exception, ...) is treated as an unplanned crash and triggers
crash-loop-backoff restart. Values are picked away from common reserved
ranges (0-2 generic, 126-165 shell/signal convention).
"""

EXIT_ARCHIVED = 90  # session archived itself (/archive) — do not restart
EXIT_PAUSED = 91     # session paused itself (/pause) — do not restart until `controller resume`
