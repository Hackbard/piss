from typing import List, Optional

from scraper.config import get_settings
from scraper.evidence.backends.file_cache import resolve_from_file_cache
from scraper.evidence.types import ResolvedEvidence
from scraper.models.domain import EvidenceRef

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
        prefer_snippet: str = "table_row",
    ) -> List[ResolvedEvidence]:
        """
        Resolve list of evidence IDs to ResolvedEvidence objects (legacy method).
        
        Uses lead_paragraph fallback since no row-level snippet_ref is available.
        
        Args:
            evidence_ids: List of evidence IDs to resolve
            with_snippets: Whether to extract snippets from raw cache
            snippet_max_len: Maximum snippet length in characters
            prefer_snippet: Preferred snippet type (ignored, uses lead_paragraph fallback)
        
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
                    prefer_snippet="lead_paragraph",  # Legacy: no snippet_ref, use lead_paragraph
                    snippet_ref=None,  # No row-level reference
                    purpose=None,
                )
            else:
                # Future: Neo4j, exports backends
                resolved_evidence = None
            
            if resolved_evidence:
                resolved.append(resolved_evidence)
        
        return resolved
    
    def resolve_refs(
        self,
        evidence_refs: List[EvidenceRef],
        with_snippets: bool = False,
        snippet_max_len: int = 500,
    ) -> List[ResolvedEvidence]:
        """
        Resolve list of EvidenceRef objects to ResolvedEvidence objects (preferred method).
        
        Uses snippet_ref from EvidenceRef if available (table_row), otherwise falls back to lead_paragraph.
        
        Args:
            evidence_refs: List of EvidenceRef objects (entity-level references with row-level snippet_refs)
            with_snippets: Whether to extract snippets from raw cache
            snippet_max_len: Maximum snippet length in characters
        
        Returns:
            List of ResolvedEvidence objects (may be shorter than input if some IDs not found)
        """
        resolved = []
        
        for evidence_ref in evidence_refs:
            if self.backend == "file_cache":
                # Prefer table_row if snippet_ref is available, otherwise lead_paragraph
                prefer_snippet = "table_row" if evidence_ref.snippet_ref and evidence_ref.snippet_ref.get("type") == "table_row" else "lead_paragraph"
                
                resolved_evidence = resolve_from_file_cache(
                    evidence_id=evidence_ref.evidence_id,
                    with_snippet=with_snippets,
                    snippet_max_len=snippet_max_len,
                    prefer_snippet=prefer_snippet,
                    snippet_ref=evidence_ref.snippet_ref,  # Row-level reference from EvidenceRef
                    purpose=evidence_ref.purpose,
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
        prefer_snippet: str = "table_row",
    ) -> Optional[ResolvedEvidence]:
        """
        Resolve single evidence ID (legacy method).
        """
        results = self.resolve([evidence_id], with_snippets=with_snippet, snippet_max_len=snippet_max_len, prefer_snippet=prefer_snippet)
        return results[0] if results else None

