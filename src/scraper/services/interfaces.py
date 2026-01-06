from abc import ABC, abstractmethod

from scraper.models.query import LegislatureStats, MandateQueryFilter, MandateQueryResult, PersonDTO


class MandateQueryServiceInterface(ABC):
    @abstractmethod
    def search(self, filter: MandateQueryFilter) -> MandateQueryResult:
        """
        Search mandates with evidence-by-default.
        
        Rules:
        - ALWAYS evidence joined (evidence_urls never null, may be empty)
        - Stable sort (person_name ASC, start_date ASC as default)
        - Uses overlap logic: mandate.start_date <= toDate AND coalesce(mandate.end_date, toDate) >= fromDate
        - If only fromDate set: toDate = "now"
        - If only toDate set: fromDate = minimal date or config default
        """
        pass


class LegislatureStatsServiceInterface(ABC):
    @abstractmethod
    def get_legislature_stats(self, legislature_id: str) -> LegislatureStats:
        """
        Get statistics for a legislature.
        
        Returns party seat counts and vote shares (if available).
        Always includes evidence_urls for statistics source.
        """
        pass


class PersonLookupServiceInterface(ABC):
    @abstractmethod
    def find_by_id(self, person_id: str) -> PersonDTO | None:
        """Find person by ID. Returns None if not found."""
        pass

    @abstractmethod
    def search_by_name(self, needle: str, limit: int = 20) -> list[PersonDTO]:
        """
        Search persons by name (case-insensitive contains).
        
        Args:
            needle: Search string (case-insensitive)
            limit: Maximum results (default 20, max 100)
        
        Returns:
            List of PersonDTO sorted by name
        """
        pass

