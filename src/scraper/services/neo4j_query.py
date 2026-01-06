from datetime import date, datetime
from typing import Optional

from neo4j import GraphDatabase

from scraper.config import Settings
from scraper.models.query import (
    LegislatureStats,
    MandateQueryFilter,
    MandateQueryResult,
    MandateRow,
    PersonDTO,
    SortDirection,
    SortField,
)
from scraper.services.interfaces import (
    LegislatureStatsServiceInterface,
    MandateQueryServiceInterface,
    PersonLookupServiceInterface)


class QueryExecutionException(Exception):
    """Exception raised when a query execution fails."""
    pass


class Neo4jMandateQueryService(MandateQueryServiceInterface):
    def __init__(self, settings: Settings):
        self.settings = settings
        self.driver = GraphDatabase.driver(
            settings.neo4j_uri,
            auth=(settings.neo4j_user, settings.neo4j_password),
        )

    def search(self, filter: MandateQueryFilter) -> MandateQueryResult:
        """
        Search mandates with evidence-by-default.
        
        Implements overlap logic:
        - mandate.start_date <= toDate AND coalesce(mandate.end_date, toDate) >= fromDate
        - If only fromDate: toDate = now
        - If only toDate: fromDate = minimal date (1900-01-01)
        """
        normalized_filter = self._normalize_filter(filter)
        
        try:
            with self.driver.session() as session:
                query, params = self._build_query(normalized_filter)
                result = session.run(query, params)
                
                rows = []
                for record in result:
                    row = self._map_record_to_row(record)
                    rows.append(row)
                
                total = self._get_total_count(session, normalized_filter)
                
                return MandateQueryResult(
                    rows=rows,
                    total=total,
                    applied_filter=normalized_filter,
                )
        except Exception as e:
            raise QueryExecutionException(f"Failed to execute mandate query: {e}") from e

    def _normalize_filter(self, filter: MandateQueryFilter) -> MandateQueryFilter:
        """Normalize filter: handle date defaults, clamp limit."""
        from_date = filter.from_date
        to_date = filter.to_date
        
        if from_date and not to_date:
            to_date = date.today()
        elif to_date and not from_date:
            from_date = date(1900, 1, 1)
        
        limit = min(filter.limit, 1000)
        
        return MandateQueryFilter(
            parliament_id=filter.parliament_id,
            legislature_id=filter.legislature_id,
            party_code=filter.party_code,
            from_date=from_date,
            to_date=to_date,
            person_id=filter.person_id,
            person_name_contains=filter.person_name_contains,
            limit=limit,
            offset=filter.offset,
            sort=filter.sort,
            sort_direction=filter.sort_direction,
        )

    def _build_query(self, filter: MandateQueryFilter) -> tuple[str, dict]:
        """Build Cypher query with evidence join."""
        where_clauses = []
        params: dict = {}
        
        if filter.parliament_id:
            where_clauses.append("m.parliament_id = $parliament_id")
            params["parliament_id"] = filter.parliament_id
        
        if filter.legislature_id:
            where_clauses.append("m.legislature_id = $legislature_id")
            params["legislature_id"] = filter.legislature_id
        
        if filter.party_code:
            where_clauses.append("m.party_code = $party_code")
            params["party_code"] = filter.party_code
        
        if filter.person_id:
            where_clauses.append("m.person_id = $person_id")
            params["person_id"] = filter.person_id
        
        if filter.person_name_contains:
            where_clauses.append("toLower(p.name) CONTAINS toLower($person_name_contains)")
            params["person_name_contains"] = filter.person_name_contains
        
        if filter.from_date or filter.to_date:
            overlap_conditions = []
            if filter.to_date:
                overlap_conditions.append(f"m.start_date <= $to_date")
                params["to_date"] = filter.to_date.isoformat()
            if filter.from_date:
                overlap_conditions.append(
                    f"(m.end_date IS NULL OR m.end_date >= $from_date)"
                )
                params["from_date"] = filter.from_date.isoformat()
            if overlap_conditions:
                where_clauses.append("(" + " AND ".join(overlap_conditions) + ")")
        
        where_clause = " AND ".join(where_clauses) if where_clauses else "1=1"
        
        sort_field_map = {
            SortField.PERSON_NAME: "p.name",
            SortField.START_DATE: "m.start_date",
            SortField.END_DATE: "COALESCE(m.end_date, '9999-12-31')",
            SortField.PARTY_CODE: "COALESCE(m.party_code, '')",
        }
        sort_field = sort_field_map.get(filter.sort, "p.name")
        sort_direction = filter.sort_direction.value
        
        query = f"""
        MATCH (p:Person)-[:HELD]->(m:Mandate)
        OPTIONAL MATCH (m)-[:SUPPORTED_BY]->(e:Evidence)
        OPTIONAL MATCH (m)-[:IN]->(l:Legislature)
        WHERE {where_clause}
        WITH p, m, l, collect(DISTINCT e.source_url) as evidence_urls
        WITH p, m, l, [u IN evidence_urls WHERE u IS NOT NULL] as urls
        ORDER BY {sort_field} {sort_direction}, m.start_date ASC
        SKIP $offset
        LIMIT $limit
        RETURN 
            p.id as person_id,
            p.name as person_name,
            p.wikipedia_title as wikipedia_title,
            m.id as mandate_id,
            m.legislature_id as legislature_id,
            l.name as legislature_name,
            m.parliament_id as parliament_id,
            m.start_date as start_date,
            m.end_date as end_date,
            m.party_code as party_code,
            urls as evidence_urls
        """
        
        params["offset"] = filter.offset
        params["limit"] = filter.limit
        
        return query, params

    def _map_record_to_row(self, record) -> MandateRow:
        """Map Neo4j record to MandateRow DTO."""
        start_date_str = record.get("start_date")
        end_date_str = record.get("end_date")
        
        start_date = date.fromisoformat(start_date_str) if start_date_str else None
        end_date = date.fromisoformat(end_date_str) if end_date_str else None
        
        evidence_urls = record.get("evidence_urls", []) or []
        evidence_urls = sorted(list(set(url for url in evidence_urls if url)))
        
        return MandateRow(
            person_id=record["person_id"],
            person_name=record["person_name"],
            wikipedia_title=record.get("wikipedia_title"),
            mandate_id=record["mandate_id"],
            legislature_id=record["legislature_id"],
            legislature_name=record.get("legislature_name"),
            parliament_id=record["parliament_id"],
            start_date=start_date,
            end_date=end_date,
            party_code=record.get("party_code"),
            evidence_urls=evidence_urls,
        )

    def _get_total_count(self, session, filter: MandateQueryFilter) -> Optional[int]:
        """Get total count with same filters (without evidence join for performance)."""
        try:
            where_clauses = []
            params: dict = {}
            
            if filter.parliament_id:
                where_clauses.append("m.parliament_id = $parliament_id")
                params["parliament_id"] = filter.parliament_id
            
            if filter.legislature_id:
                where_clauses.append("m.legislature_id = $legislature_id")
                params["legislature_id"] = filter.legislature_id
            
            if filter.party_code:
                where_clauses.append("m.party_code = $party_code")
                params["party_code"] = filter.party_code
            
            if filter.person_id:
                where_clauses.append("m.person_id = $person_id")
                params["person_id"] = filter.person_id
            
            if filter.person_name_contains:
                where_clauses.append("toLower(p.name) CONTAINS toLower($person_name_contains)")
                params["person_name_contains"] = filter.person_name_contains
            
            if filter.from_date or filter.to_date:
                overlap_conditions = []
                if filter.to_date:
                    overlap_conditions.append(f"m.start_date <= $to_date")
                    params["to_date"] = filter.to_date.isoformat()
                if filter.from_date:
                    overlap_conditions.append(
                        f"(m.end_date IS NULL OR m.end_date >= $from_date)"
                    )
                    params["from_date"] = filter.from_date.isoformat()
                if overlap_conditions:
                    where_clauses.append("(" + " AND ".join(overlap_conditions) + ")")
            
            where_clause = " AND ".join(where_clauses) if where_clauses else "1=1"
            
            count_query = f"""
            MATCH (p:Person)-[:HELD]->(m:Mandate)
            WHERE {where_clause}
            RETURN count(m) as total
            """
            
            result = session.run(count_query, params)
            record = result.single()
            return record["total"] if record else None
        except Exception:
            return None

    def close(self) -> None:
        self.driver.close()


class Neo4jLegislatureStatsService(LegislatureStatsServiceInterface):
    def __init__(self, settings: Settings):
        self.settings = settings
        self.driver = GraphDatabase.driver(
            settings.neo4j_uri,
            auth=(settings.neo4j_user, settings.neo4j_password),
        )

    def get_legislature_stats(self, legislature_id: str) -> LegislatureStats:
        """Get statistics for a legislature."""
        try:
            with self.driver.session() as session:
                query_legislature = """
                MATCH (l:Legislature {id: $legislature_id})
                RETURN l.id as legislature_id, l.name as legislature_name
                """
                leg_result = session.run(query_legislature, {"legislature_id": legislature_id})
                leg_record = leg_result.single()
                
                if not leg_record:
                    return LegislatureStats(
                        legislature_id=legislature_id,
                        legislature_name="Unknown",
                        total_seats=None,
                        party_seats={},
                        party_vote_share={},
                        evidence_urls=[],
                    )
                
                legislature_name = leg_record.get("legislature_name", "Unknown")
                
                query_stats = """
                MATCH (l:Legislature {id: $legislature_id})
                OPTIONAL MATCH (m:Mandate)-[:IN]->(l)
                OPTIONAL MATCH (m)-[:SUPPORTED_BY]->(e:Evidence)
                WITH l, m, e
                WHERE m IS NOT NULL AND m.party_code IS NOT NULL
                WITH m.party_code as code, count(DISTINCT m) as seats, collect(DISTINCT e.source_url) as evidence_urls
                RETURN code, seats, evidence_urls
                """
                
                result = session.run(query_stats, {"legislature_id": legislature_id})
                
                party_seats = {}
                all_evidence_urls = set()
                total_seats = 0
                
                for record in result:
                    code = record.get("code")
                    seats = record.get("seats", 0)
                    urls = record.get("evidence_urls", []) or []
                    
                    if code:
                        party_seats[code] = seats
                        total_seats += seats
                        all_evidence_urls.update(url for url in urls if url)
                
                if total_seats == 0:
                    query_total = """
                    MATCH (l:Legislature {id: $legislature_id})
                    OPTIONAL MATCH (m:Mandate)-[:IN]->(l)
                    RETURN count(m) as total
                    """
                    total_result = session.run(query_total, {"legislature_id": legislature_id})
                    total_record = total_result.single()
                    total_seats = total_record.get("total") if total_record else None
                
                query_evidence = """
                MATCH (l:Legislature {id: $legislature_id})
                OPTIONAL MATCH (m:Mandate)-[:IN]->(l)
                OPTIONAL MATCH (m)-[:SUPPORTED_BY]->(e:Evidence)
                WITH collect(DISTINCT e.source_url) as all_urls
                RETURN [u IN all_urls WHERE u IS NOT NULL] as evidence_urls
                """
                evidence_result = session.run(query_evidence, {"legislature_id": legislature_id})
                evidence_record = evidence_result.single()
                if evidence_record:
                    all_evidence_urls.update(evidence_record.get("evidence_urls", []) or [])
                
                evidence_urls = sorted(list(all_evidence_urls))
                
                return LegislatureStats(
                    legislature_id=legislature_id,
                    legislature_name=legislature_name,
                    total_seats=total_seats,
                    party_seats=party_seats,
                    party_vote_share={},
                    evidence_urls=evidence_urls,
                )
        except Exception as e:
            raise QueryExecutionException(f"Failed to get legislature stats: {e}") from e

    def close(self) -> None:
        self.driver.close()


class Neo4jPersonLookupService(PersonLookupServiceInterface):
    def __init__(self, settings: Settings):
        self.settings = settings
        self.driver = GraphDatabase.driver(
            settings.neo4j_uri,
            auth=(settings.neo4j_user, settings.neo4j_password),
        )

    def find_by_id(self, person_id: str) -> PersonDTO | None:
        """Find person by ID."""
        try:
            with self.driver.session() as session:
                query = """
                MATCH (p:Person {id: $person_id})
                OPTIONAL MATCH (p)-[:SUPPORTED_BY]->(e:Evidence)
                WITH p, collect(DISTINCT e.source_url) as evidence_urls
                RETURN 
                    p.id as person_id,
                    p.name as name,
                    p.wikipedia_title as wikipedia_title,
                    p.wikipedia_url as wikipedia_url,
                    p.birth_date as birth_date,
                    p.death_date as death_date,
                    p.intro as intro,
                    [u IN evidence_urls WHERE u IS NOT NULL] as evidence_urls
                LIMIT 1
                """
                
                result = session.run(query, {"person_id": person_id})
                record = result.single()
                
                if not record:
                    return None
                
                birth_date_str = record.get("birth_date")
                death_date_str = record.get("death_date")
                
                birth_date = date.fromisoformat(birth_date_str) if birth_date_str else None
                death_date = date.fromisoformat(death_date_str) if death_date_str else None
                
                evidence_urls = record.get("evidence_urls", []) or []
                evidence_urls = sorted(list(set(url for url in evidence_urls if url)))
                
                return PersonDTO(
                    person_id=record["person_id"],
                    name=record["name"],
                    wikipedia_title=record.get("wikipedia_title"),
                    wikipedia_url=record.get("wikipedia_url"),
                    birth_date=birth_date,
                    death_date=death_date,
                    intro=record.get("intro"),
                    evidence_urls=evidence_urls,
                )
        except Exception as e:
            raise QueryExecutionException(f"Failed to find person: {e}") from e

    def search_by_name(self, needle: str, limit: int = 20) -> list[PersonDTO]:
        """Search persons by name (case-insensitive contains)."""
        limit = min(limit, 100)
        
        try:
            with self.driver.session() as session:
                query = """
                MATCH (p:Person)
                WHERE toLower(p.name) CONTAINS toLower($needle)
                OPTIONAL MATCH (p)-[:SUPPORTED_BY]->(e:Evidence)
                WITH p, collect(DISTINCT e.source_url) as evidence_urls
                ORDER BY p.name ASC
                LIMIT $limit
                RETURN 
                    p.id as person_id,
                    p.name as name,
                    p.wikipedia_title as wikipedia_title,
                    p.wikipedia_url as wikipedia_url,
                    p.birth_date as birth_date,
                    p.death_date as death_date,
                    p.intro as intro,
                    [u IN evidence_urls WHERE u IS NOT NULL] as evidence_urls
                """
                
                result = session.run(query, {"needle": needle, "limit": limit})
                
                persons = []
                for record in result:
                    birth_date_str = record.get("birth_date")
                    death_date_str = record.get("death_date")
                    
                    birth_date = date.fromisoformat(birth_date_str) if birth_date_str else None
                    death_date = date.fromisoformat(death_date_str) if death_date_str else None
                    
                    evidence_urls = record.get("evidence_urls", []) or []
                    evidence_urls = sorted(list(set(url for url in evidence_urls if url)))
                    
                    persons.append(
                        PersonDTO(
                            person_id=record["person_id"],
                            name=record["name"],
                            wikipedia_title=record.get("wikipedia_title"),
                            wikipedia_url=record.get("wikipedia_url"),
                            birth_date=birth_date,
                            death_date=death_date,
                            intro=record.get("intro"),
                            evidence_urls=evidence_urls,
                        )
                    )
                
                return persons
        except Exception as e:
            raise QueryExecutionException(f"Failed to search persons: {e}") from e

    def close(self) -> None:
        self.driver.close()

