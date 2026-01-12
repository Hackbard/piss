from datetime import date
from uuid import uuid4

from fastapi import FastAPI, HTTPException

from scraper.api.models import (
    ErrorResponse,
    LegislatureStatsRequest,
    LegislatureStatsResponse,
    MandateSearchRequest,
    MandateSearchResponse,
    ParliamentCoverageRequest,
    ParliamentCoverageResponse,
    ParliamentCoverageRowResponse,
    PersonLookupRequest,
    PersonLookupResponse,
)
from scraper.api.utils import (
    compute_result_hash,
    create_tool_meta,
    validate_evidence_strict,
)
from scraper.config import get_settings
from scraper.models.query import MandateQueryFilter, SortDirection, SortField
from scraper.services import (
    Neo4jLegislatureStatsService,
    Neo4jMandateQueryService,
    Neo4jParliamentCoverageService,
    Neo4jPersonLookupService,
)


def create_app() -> FastAPI:
    """Create FastAPI app with tool endpoints."""
    app = FastAPI(
        title="Parliament Data Tool API",
        description="Tool Contracts for LangGraph/LLM integration",
        version="1.0.0",
    )
    settings = get_settings()
    
    @app.post("/api/tools/mandates/search", response_model=MandateSearchResponse)
    async def mandates_search(request: MandateSearchRequest) -> MandateSearchResponse:
        """Search mandates with evidence-by-default."""
        request_id = uuid4()
        tool_meta = create_tool_meta("mandates.search", request_id)
        
        try:
            filter_obj = MandateQueryFilter(
                parliament_id=request.parliament_id,
                legislature_id=request.legislature_id,
                party_code=request.party_code,
                from_date=request.from_date,
                to_date=request.to_date,
                person_id=request.person_id,
                person_name_contains=request.person_name_contains,
                limit=request.limit,
                offset=request.offset,
                sort=SortField(request.sort.value),
                sort_direction=SortDirection(request.sort_dir.value),
            )
            
            service = Neo4jMandateQueryService(settings)
            result = service.search(filter_obj)
            service.close()
            
            rows = []
            for row in result.rows:
                rows.append({
                    "person_id": row.person_id,
                    "person_name": row.person_name,
                    "wikipedia_title": row.wikipedia_title,
                    "mandate_id": row.mandate_id,
                    "parliament_id": row.parliament_id,
                    "legislature_id": row.legislature_id,
                    "legislature": row.legislature_name or row.legislature_id,
                    "start_date": row.start_date,
                    "end_date": row.end_date,
                    "party_code": row.party_code,
                    "evidence_urls": row.evidence_urls,
                })
            
            warnings = validate_evidence_strict(
                result.rows,
                request.strict_evidence,
                "mandates.search",
            )
            tool_meta.warnings = warnings
            
            response_data = {
                "meta": tool_meta.model_dump(mode="json"),
                "applied_filter": filter_obj.model_dump(mode="json"),
                "total": result.total,
                "rows": rows,
            }
            
            tool_meta.result_hash = compute_result_hash(response_data)
            response_data["meta"] = tool_meta.model_dump(mode="json")
            
            return MandateSearchResponse(**response_data)
            
        except ValueError as e:
            error_msg = str(e)
            if "EVIDENCE_MISSING" in error_msg:
                raise HTTPException(
                    status_code=422,
                    detail={
                        "error": error_msg,
                        "error_code": "EVIDENCE_MISSING",
                        "request_id": str(request_id),
                    },
                )
            raise HTTPException(status_code=422, detail={"error": error_msg, "error_code": "VALIDATION_ERROR"})
        except Exception as e:
            raise HTTPException(
                status_code=500,
                detail={"error": str(e), "error_code": "INTERNAL_ERROR", "request_id": str(request_id)},
            )
    
    @app.post("/api/tools/legislatures/stats", response_model=LegislatureStatsResponse)
    async def legislature_stats(request: LegislatureStatsRequest) -> LegislatureStatsResponse:
        """Get statistics for a legislature."""
        request_id = uuid4()
        tool_meta = create_tool_meta("legislature.stats", request_id)
        
        try:
            service = Neo4jLegislatureStatsService(settings)
            stats = service.get_legislature_stats(request.legislature_id)
            service.close()
            
            warnings = validate_evidence_strict(
                [stats],
                request.strict_evidence,
                "legislature.stats",
            )
            tool_meta.warnings = warnings
            
            response_data = {
                "meta": tool_meta.model_dump(mode="json"),
                "legislature_id": stats.legislature_id,
                "legislature_name": stats.legislature_name,
                "parliament_id": None,
                "start_date": None,
                "end_date": None,
                "total_seats": stats.total_seats,
                "party_seats": stats.party_seats,
                "party_vote_share": stats.party_vote_share,
                "evidence_urls": stats.evidence_urls,
            }
            
            tool_meta.result_hash = compute_result_hash(response_data)
            response_data["meta"] = tool_meta.model_dump(mode="json")
            
            return LegislatureStatsResponse(**response_data)
            
        except ValueError as e:
            error_msg = str(e)
            if "EVIDENCE_MISSING" in error_msg:
                raise HTTPException(
                    status_code=422,
                    detail={
                        "error": error_msg,
                        "error_code": "EVIDENCE_MISSING",
                        "request_id": str(request_id),
                    },
                )
            raise HTTPException(status_code=422, detail={"error": error_msg, "error_code": "VALIDATION_ERROR"})
        except Exception as e:
            raise HTTPException(
                status_code=500,
                detail={"error": str(e), "error_code": "INTERNAL_ERROR", "request_id": str(request_id)},
            )
    
    @app.post("/api/tools/persons/lookup", response_model=PersonLookupResponse)
    async def person_lookup(request: PersonLookupRequest) -> PersonLookupResponse:
        """Lookup person by ID or search by name."""
        request_id = uuid4()
        tool_meta = create_tool_meta("person.lookup", request_id)
        
        try:
            service = Neo4jPersonLookupService(settings)
            
            if request.person_id:
                person = service.find_by_id(request.person_id)
                persons = [person] if person else []
            else:
                persons = service.search_by_name(request.name_contains or "", limit=request.limit)
            
            service.close()
            
            persons_data = []
            for p in persons:
                persons_data.append({
                    "person_id": p.person_id,
                    "name": p.name,
                    "wikipedia_title": p.wikipedia_title,
                    "wikipedia_url": p.wikipedia_url,
                    "birth_date": p.birth_date,
                    "death_date": p.death_date,
                    "intro": p.intro,
                    "evidence_urls": p.evidence_urls,
                })
            
            response_data = {
                "meta": tool_meta.model_dump(mode="json"),
                "persons": persons_data,
            }
            
            tool_meta.result_hash = compute_result_hash(response_data)
            response_data["meta"] = tool_meta.model_dump(mode="json")
            
            return PersonLookupResponse(**response_data)
            
        except Exception as e:
            raise HTTPException(
                status_code=500,
                detail={"error": str(e), "error_code": "INTERNAL_ERROR", "request_id": str(request_id)},
            )
    
    @app.post("/api/tools/parliaments/coverage", response_model=ParliamentCoverageResponse)
    async def parliaments_coverage(request: ParliamentCoverageRequest) -> ParliamentCoverageResponse:
        """Get coverage statistics per parliament_id."""
        request_id = uuid4()
        tool_meta = create_tool_meta("parliaments.coverage", request_id)
        
        try:
            parliament_ids = request.parliament_ids if request.parliament_ids else None
            
            service = Neo4jParliamentCoverageService(settings)
            coverage_rows = service.get_coverage(parliament_ids)
            service.close()
            
            rows_data = []
            for row in coverage_rows:
                rows_data.append({
                    "parliament_id": row.parliament_id,
                    "mandates_count": row.mandates_count,
                    "min_start": row.min_start,
                    "max_end": row.max_end,
                    "invalid_start_count": row.invalid_start_count,
                    "invalid_end_count": row.invalid_end_count,
                    "missing_evidence_count": row.missing_evidence_count,
                })
            
            applied_filter = {
                "parliament_ids": parliament_ids if parliament_ids else "all",
            }
            
            response_data = {
                "meta": tool_meta.model_dump(mode="json"),
                "applied_filter": applied_filter,
                "rows": rows_data,
            }
            
            tool_meta.result_hash = compute_result_hash(response_data)
            response_data["meta"] = tool_meta.model_dump(mode="json")
            
            return ParliamentCoverageResponse(**response_data)
            
        except Exception as e:
            raise HTTPException(
                status_code=500,
                detail={"error": str(e), "error_code": "INTERNAL_ERROR", "request_id": str(request_id)},
            )
    
    @app.get("/health")
    async def health():
        """Health check endpoint."""
        return {"status": "ok"}
    
    return app

