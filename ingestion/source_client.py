"""
Source abstraction for World Cup data ingestion.

The rest of the pipeline depends only on this interface, never on a
concrete scraper or API client. This is what makes swapping data sources
(scraper -> paid API, or fixture -> live scraper) a one-line config change
instead of a rewrite.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timezone


@dataclass
class IngestionResult:
    teams: list[dict] = field(default_factory=list)
    stadiums: list[dict] = field(default_factory=list)
    matches: list[dict] = field(default_factory=list)
    fetched_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    source_name: str = "unknown"


class SourceClient(ABC):
    """Every concrete data source (scraper, API client, fixture) implements this."""

    name: str = "base"

    @abstractmethod
    def fetch_teams(self) -> list[dict]: ...

    @abstractmethod
    def fetch_stadiums(self) -> list[dict]: ...

    @abstractmethod
    def fetch_matches(self) -> list[dict]: ...

    def fetch_all(self) -> IngestionResult:
        return IngestionResult(
            teams=self.fetch_teams(),
            stadiums=self.fetch_stadiums(),
            matches=self.fetch_matches(),
            source_name=self.name,
        )
