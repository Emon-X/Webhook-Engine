
class TransientProcessingError(Exception):
    """Raised when an event fails due to a temporary problem."""
    pass


class PermanentProcessingError(Exception):
    """Raised when an event fails due to a permanent problem."""
    pass
