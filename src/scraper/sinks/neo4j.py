from typing import Any, Dict

from neo4j import GraphDatabase

from scraper.config import Settings


class Neo4jSink:
    def __init__(self, settings: Settings):
        self.settings = settings
        self.driver = GraphDatabase.driver(
            settings.neo4j_uri,
            auth=(settings.neo4j_user, settings.neo4j_password),
        )

    def init(self) -> None:
        with self.driver.session() as session:
            constraints = [
                "CREATE CONSTRAINT person_id IF NOT EXISTS FOR (p:Person) REQUIRE p.id IS UNIQUE",
                "CREATE CONSTRAINT party_id IF NOT EXISTS FOR (p:Party) REQUIRE p.id IS UNIQUE",
                "CREATE CONSTRAINT legislature_id IF NOT EXISTS FOR (l:Legislature) REQUIRE l.id IS UNIQUE",
                "CREATE CONSTRAINT mandate_id IF NOT EXISTS FOR (m:Mandate) REQUIRE m.id IS UNIQUE",
                "CREATE CONSTRAINT evidence_id IF NOT EXISTS FOR (e:Evidence) REQUIRE e.id IS UNIQUE",
                "CREATE CONSTRAINT canonical_person_id IF NOT EXISTS FOR (c:CanonicalPerson) REQUIRE c.id IS UNIQUE",
                "CREATE CONSTRAINT wikipedia_person_record_id IF NOT EXISTS FOR (w:WikipediaPersonRecord) REQUIRE w.id IS UNIQUE",
                "CREATE CONSTRAINT dip_person_record_id IF NOT EXISTS FOR (d:DipPersonRecord) REQUIRE d.id IS UNIQUE",
                "CREATE CONSTRAINT person_link_assertion_id IF NOT EXISTS FOR (a:PersonLinkAssertion) REQUIRE a.id IS UNIQUE",
            ]
            for constraint in constraints:
                try:
                    session.run(constraint)
                except Exception:
                    pass

    def upsert(self, normalized_data: Dict[str, Any]) -> None:
        with self.driver.session() as session:
            for person in normalized_data.get("persons", []):
                # Ensure evidence_ids is derived from evidence_refs if not set
                evidence_ids = person.evidence_ids
                if not evidence_ids and person.evidence_refs:
                    evidence_ids = list(set([ref.evidence_id for ref in person.evidence_refs]))
                
                session.run(
                    """
                    MERGE (p:Person {id: $id})
                    SET p.name = $name,
                        p.wikipedia_title = $wikipedia_title,
                        p.wikipedia_url = $wikipedia_url,
                        p.birth_date = $birth_date,
                        p.birth_date_status = $birth_date_status,
                        p.death_date = $death_date,
                        p.intro = $intro,
                        p.evidence_ids = $evidence_ids,
                        p.data_quality_flags = $data_quality_flags
                    """,
                    id=person.id,
                    name=person.name,
                    wikipedia_title=person.wikipedia_title,
                    wikipedia_url=person.wikipedia_url,
                    birth_date=person.birth_date,
                    birth_date_status=getattr(person, "birth_date_status", "unknown"),
                    death_date=person.death_date,
                    intro=person.intro,
                    evidence_ids=evidence_ids,
                    data_quality_flags=getattr(person, "data_quality_flags", []),
                )
                
                # Create EvidenceRef relationships with snippet_ref as property
                for evidence_ref in person.evidence_refs:
                    import json
                    # Always set snippet_ref_json: empty string if None (for consistency)
                    # This makes the data structure consistent and queries simpler
                    snippet_ref_json = json.dumps(evidence_ref.snippet_ref, sort_keys=True) if evidence_ref.snippet_ref else ""
                    
                    session.run(
                        """
                        MATCH (p:Person {id: $person_id})
                        MERGE (e:Evidence {id: $evidence_id})
                        MERGE (p)-[r:SUPPORTED_BY {
                            purpose: $purpose,
                            snippet_ref_json: $snippet_ref_json
                        }]->(e)
                        """,
                        person_id=person.id,
                        evidence_id=evidence_ref.evidence_id,
                        purpose=evidence_ref.purpose or "",
                        snippet_ref_json=snippet_ref_json,
                    )

            for party in normalized_data.get("parties", []):
                session.run(
                    """
                    MERGE (p:Party {id: $id})
                    SET p.name = $name,
                        p.evidence_ids = $evidence_ids
                    """,
                    id=party.id,
                    name=party.name,
                    evidence_ids=party.evidence_ids,
                )

            for legislature in normalized_data.get("legislatures", []):
                session.run(
                    """
                    MERGE (l:Legislature {id: $id})
                    SET l.parliament = $parliament,
                        l.state = $state,
                        l.number = $number,
                        l.start_date = $start_date,
                        l.end_date = $end_date,
                        l.evidence_ids = $evidence_ids
                    """,
                    id=legislature.id,
                    parliament=legislature.parliament,
                    state=legislature.state,
                    number=legislature.number,
                    start_date=legislature.start_date,
                    end_date=legislature.end_date,
                    evidence_ids=legislature.evidence_ids,
                )

            for mandate in normalized_data.get("mandates", []):
                # Ensure evidence_ids is derived from evidence_refs if not set
                mandate_evidence_ids = mandate.evidence_ids
                if not mandate_evidence_ids and mandate.evidence_refs:
                    mandate_evidence_ids = list(set([ref.evidence_id for ref in mandate.evidence_refs]))
                
                session.run(
                    """
                    MERGE (m:Mandate {id: $id})
                    SET m.person_id = $person_id,
                        m.legislature_id = $legislature_id,
                        m.party_name = $party_name,
                        m.wahlkreis = $wahlkreis,
                        m.start_date = $start_date,
                        m.end_date = $end_date,
                        m.role = $role,
                        m.notes = $notes,
                        m.evidence_ids = $evidence_ids
                    """,
                    id=mandate.id,
                    person_id=mandate.person_id,
                    legislature_id=mandate.legislature_id,
                    party_name=mandate.party_name,
                    wahlkreis=mandate.wahlkreis,
                    start_date=mandate.start_date,
                    end_date=mandate.end_date,
                    role=mandate.role,
                    notes=mandate.notes,
                    evidence_ids=mandate_evidence_ids,
                )
                
                # Create EvidenceRef relationships with snippet_ref as property
                for evidence_ref in mandate.evidence_refs:
                    import json
                    # Always set snippet_ref_json: empty string if None (for consistency)
                    # This makes the data structure consistent and queries simpler
                    snippet_ref_json = json.dumps(evidence_ref.snippet_ref, sort_keys=True) if evidence_ref.snippet_ref else ""
                    
                    session.run(
                        """
                        MATCH (m:Mandate {id: $mandate_id})
                        MERGE (e:Evidence {id: $evidence_id})
                        MERGE (m)-[r:SUPPORTED_BY {
                            purpose: $purpose,
                            snippet_ref_json: $snippet_ref_json
                        }]->(e)
                        """,
                        mandate_id=mandate.id,
                        evidence_id=evidence_ref.evidence_id,
                        purpose=evidence_ref.purpose or "",
                        snippet_ref_json=snippet_ref_json,
                    )

                session.run(
                    """
                    MATCH (p:Person {id: $person_id})
                    MATCH (m:Mandate {id: $mandate_id})
                    MERGE (p)-[:HELD]->(m)
                    """,
                    person_id=mandate.person_id,
                    mandate_id=mandate.id,
                )

                if mandate.legislature_id:
                    session.run(
                        """
                        MATCH (m:Mandate {id: $mandate_id})
                        MATCH (l:Legislature {id: $legislature_id})
                        MERGE (m)-[:IN]->(l)
                        """,
                        mandate_id=mandate.id,
                        legislature_id=mandate.legislature_id,
                    )

                if mandate.party_name:
                    from scraper.utils.ids import generate_party_id

                    party_id = generate_party_id(mandate.party_name)
                    session.run(
                        """
                        MATCH (m:Mandate {id: $mandate_id})
                        MATCH (p:Party {id: $party_id})
                        MERGE (m)-[r:AFFILIATED_WITH]->(p)
                        SET r.start_date = $start_date,
                            r.end_date = $end_date
                        """,
                        mandate_id=mandate.id,
                        party_id=party_id,
                        start_date=mandate.start_date,
                        end_date=mandate.end_date,
                    )

            # Upsert WikipediaPersonRecords
            for wiki_record in normalized_data.get("wikipedia_person_records", []):
                session.run(
                    """
                    MERGE (w:WikipediaPersonRecord {id: $id})
                    SET w.wikipedia_title = $wikipedia_title,
                        w.wikipedia_url = $wikipedia_url,
                        w.page_id = $page_id,
                        w.revision_id = $revision_id,
                        w.name = $name,
                        w.birth_date = $birth_date,
                        w.death_date = $death_date,
                        w.intro = $intro,
                        w.evidence_ids = $evidence_ids
                    """,
                    id=wiki_record.id,
                    wikipedia_title=wiki_record.wikipedia_title,
                    wikipedia_url=wiki_record.wikipedia_url,
                    page_id=wiki_record.page_id,
                    revision_id=wiki_record.revision_id,
                    name=wiki_record.name,
                    birth_date=wiki_record.birth_date,
                    death_date=wiki_record.death_date,
                    intro=wiki_record.intro,
                    evidence_ids=wiki_record.evidence_ids,
                )

            # Upsert DipPersonRecords
            for dip_record in normalized_data.get("dip_person_records", []):
                session.run(
                    """
                    MERGE (d:DipPersonRecord {id: $id})
                    SET d.dip_person_id = $dip_person_id,
                        d.vorname = $vorname,
                        d.nachname = $nachname,
                        d.namenszusatz = $namenszusatz,
                        d.titel = $titel,
                        d.fraktion = $fraktion,
                        d.wahlperiode = $wahlperiode,
                        d.evidence_ids = $evidence_ids
                    """,
                    id=dip_record.id,
                    dip_person_id=dip_record.dip_person_id,
                    vorname=dip_record.vorname,
                    nachname=dip_record.nachname,
                    namenszusatz=dip_record.namenszusatz,
                    titel=dip_record.titel,
                    fraktion=dip_record.fraktion,
                    wahlperiode=dip_record.wahlperiode,
                    evidence_ids=dip_record.evidence_ids,
                )

            # Upsert CanonicalPersons
            for canonical in normalized_data.get("canonical_persons", []):
                import json
                session.run(
                    """
                    MERGE (c:CanonicalPerson {id: $id})
                    SET c.display_name = $display_name,
                        c.wikipedia_title = $wikipedia_title,
                        c.wikipedia_page_id = $wikipedia_page_id,
                        c.dip_person_id = $dip_person_id,
                        c.created_at = $created_at,
                        c.updated_at = $updated_at,
                        c.evidence_ids = $evidence_ids
                    """,
                    id=canonical.id,
                    display_name=canonical.display_name,
                    wikipedia_title=canonical.identifiers.get("wikipedia_title"),
                    wikipedia_page_id=canonical.identifiers.get("wikipedia_page_id"),
                    dip_person_id=canonical.identifiers.get("dip_person_id"),
                    created_at=canonical.created_at,
                    updated_at=canonical.updated_at,
                    evidence_ids=canonical.evidence_ids,
                )

                # Link CanonicalPerson to WikipediaPersonRecord
                wiki_title = canonical.identifiers.get("wikipedia_title")
                if wiki_title:
                    session.run(
                        """
                        MATCH (c:CanonicalPerson {id: $canonical_id})
                        MATCH (w:WikipediaPersonRecord {wikipedia_title: $wikipedia_title})
                        MERGE (c)-[:HAS_SOURCE]->(w)
                        """,
                        canonical_id=canonical.id,
                        wikipedia_title=wiki_title,
                    )

                # Link CanonicalPerson to DipPersonRecord
                dip_id = canonical.identifiers.get("dip_person_id")
                if dip_id:
                    session.run(
                        """
                        MATCH (c:CanonicalPerson {id: $canonical_id})
                        MATCH (d:DipPersonRecord {dip_person_id: $dip_person_id})
                        MERGE (c)-[:HAS_SOURCE]->(d)
                        """,
                        canonical_id=canonical.id,
                        dip_person_id=dip_id,
                    )

            # Upsert PersonLinkAssertions
            for assertion in normalized_data.get("link_assertions", []):
                session.run(
                    """
                    MERGE (a:PersonLinkAssertion {id: $id})
                    SET a.wikipedia_person_ref = $wikipedia_person_ref,
                        a.dip_person_ref = $dip_person_ref,
                        a.ruleset_version = $ruleset_version,
                        a.method = $method,
                        a.score = $score,
                        a.status = $status,
                        a.reason = $reason,
                        a.evidence_ids = $evidence_ids,
                        a.created_at = $created_at
                    """,
                    id=assertion.id,
                    wikipedia_person_ref=assertion.wikipedia_person_ref,
                    dip_person_ref=assertion.dip_person_ref,
                    ruleset_version=assertion.ruleset_version,
                    method=assertion.method,
                    score=assertion.score,
                    status=assertion.status,
                    reason=assertion.reason,
                    evidence_ids=assertion.evidence_ids,
                    created_at=assertion.created_at,
                )

                # Link Assertion to WikipediaPersonRecord
                session.run(
                    """
                    MATCH (a:PersonLinkAssertion {id: $assertion_id})
                    MATCH (w:WikipediaPersonRecord {id: $wikipedia_person_ref})
                    MERGE (a)-[:LINKS]->(w)
                    """,
                    assertion_id=assertion.id,
                    wikipedia_person_ref=assertion.wikipedia_person_ref,
                )

                # Link Assertion to DipPersonRecord
                session.run(
                    """
                    MATCH (a:PersonLinkAssertion {id: $assertion_id})
                    MATCH (d:DipPersonRecord {dip_person_id: $dip_person_ref})
                    MERGE (a)-[:LINKS]->(d)
                    """,
                    assertion_id=assertion.id,
                    dip_person_ref=assertion.dip_person_ref,
                )

            for evidence in normalized_data.get("evidence", []):
                session.run(
                    """
                    MERGE (e:Evidence {id: $id})
                    SET e.endpoint_kind = $endpoint_kind,
                        e.page_title = $page_title,
                        e.page_id = $page_id,
                        e.revision_id = $revision_id,
                        e.source_url = $source_url,
                        e.retrieved_at = $retrieved_at,
                        e.sha256 = $sha256
                    """,
                    id=evidence.id,
                    endpoint_kind=evidence.endpoint_kind,
                    page_title=evidence.page_title,
                    page_id=evidence.page_id,
                    revision_id=evidence.revision_id,
                    source_url=evidence.source_url,
                    retrieved_at=evidence.retrieved_at,
                    sha256=evidence.sha256,
                )

    def close(self) -> None:
        self.driver.close()

