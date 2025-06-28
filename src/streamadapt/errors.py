"""Exception classes used by streamadapt."""


class StreamModeError(TypeError):
    """Raised when stream mode cannot be determined or mismatched."""


__all__ = ["StreamModeError"]
