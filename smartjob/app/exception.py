class SmartJobException(Exception):
    """Base class for exceptions in this module."""

    pass


class SmartJobTimeoutException(SmartJobException):
    """Exception raised when a job execution times out."""

    pass
