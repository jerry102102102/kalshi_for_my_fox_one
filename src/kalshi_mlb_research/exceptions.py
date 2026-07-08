class KalshiMLBResearchError(Exception):
    """Base project exception."""


class LiveTradingDisabledError(KalshiMLBResearchError):
    """Raised when live trading is attempted while disabled."""


class ExternalServiceError(KalshiMLBResearchError):
    """Raised when an upstream API call fails."""


class DataValidationError(KalshiMLBResearchError):
    """Raised when external data cannot be parsed safely."""

