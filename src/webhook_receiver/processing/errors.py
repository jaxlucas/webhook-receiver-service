"""Error classification for handlers."""


class PermanentError(Exception):
    """Retrying cannot help: invalid business data, an unknown referenced entity, a rule violation.

    Raise it only when the same input can never succeed. The event goes straight to ``failed``
    (``failure_reason='permanent_error'``) and the message becomes ``last_error``. Wrap lower-level
    errors with ``raise PermanentError(...) from exc``. Every other exception is treated as transient
    and retried with backoff; that is the safe default.
    """
