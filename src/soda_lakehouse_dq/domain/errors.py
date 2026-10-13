"""Domain exceptions."""


class DataQualityError(Exception):
    """Base class for every error raised by this package."""


class ContractNotFoundError(DataQualityError):
    """No contract exists for the requested pipeline / layer."""


class ContractRenderError(DataQualityError):
    """A contract template could not be rendered or parsed."""


class GateBlockedError(DataQualityError):
    """A quality gate blocked promotion. Carries the report for callers that want details."""

    def __init__(self, message: str, report: object) -> None:
        super().__init__(message)
        self.report = report


class DataSourceError(DataQualityError):
    """A data source could not be opened."""


class NotificationError(DataQualityError):
    """A notifier could not deliver a report (for example Slack returned an HTTP error)."""
