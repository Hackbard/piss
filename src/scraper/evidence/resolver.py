from typing import List, Optional

from scraper.config import get_settings
from scraper.evidence.backends.file_cache import resolve_from_file_cache
from scraper.evidence.types import ResolvedEvidence

settings = get_settings()


class EvidenceResolver:
    """
    Resolves evidence IDs to ResolvedEvidence objects with canonical URLs and snippets.
    
    Primary backend: file-based cache (offline, deterministic)
    Optional backends: Neo4j, exports (can be added later)
    """
    
    def __init__(self, backend: str = "file_cache"):
        self.backend = backend
    
    def resolve(
        self,
        evidence_ids: List[str],
        with_snippets: bool = False,
        snippet_max_len: int = 500,
    ) -> List[ResolvedEvidence]:
        """
        Resolve list of evidence IDs to ResolvedEvidence objects.
        
        Args:
            evidence_ids: List of evidence IDs to resolve
            with_snippets: Whether to extract snippets from raw cache
            snippet_max_len: Maximum snippet length in characters
        
        Returns:
            List of ResolvedEvidence objects (may be shorter than input if some IDs not found)
        """
        resolved = []
        
        for evidence_id in evidence_ids:
            if self.backend == "file_cache":
                resolved_evidence = resolve_from_file_cache(
                    evidence_id=evidence_id,
                    with_snippet=with_snippets,
                    snippet_max_len=snippet_max_len,
                )
            else:
                # Future: Neo4j, exports backends
                resolved_evidence = None
            
            if resolved_evidence:
                resolved.append(resolved_evidence)
        
        return resolved
    
    def resolve_single(
        self,
        evidence_id: str,
        with_snippet: bool = False,
        snippet_max_len: int = 500,
    ) -> Optional[ResolvedEvidence]:
        """
        Resolve single evidence ID.
        """
        results = self.resolve([evidence_id], with_snippets=with_snippet, snippet_max_len=snippet_max_len)
        return results[0] if results else None

