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
            ]
            for constraint in constraints:
                try:
                    session.run(constraint)
                except Exception:
                    pass

    def upsert(self, normalized_data: Dict[str, Any]) -> None:
        with self.driver.session() as session:
            for person in normalized_data.get("persons", []):
                session.run(
                    """
                    MERGE (p:Person {id: $id})
                    SET p.name = $name,
                        p.wikipedia_title = $wikipedia_title,
                        p.wikipedia_url = $wikipedia_url,
                        p.birth_date = $birth_date,
                        p.death_date = $death_date,
                        p.intro = $intro,
                        p.evidence_ids = $evidence_ids
                    """,
                    id=person.id,
                    name=person.name,
                    wikipedia_title=person.wikipedia_title,
                    wikipedia_url=person.wikipedia_url,
                    birth_date=person.birth_date,
                    death_date=person.death_date,
                    intro=person.intro,
                    evidence_ids=person.evidence_ids,
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
                    evidence_ids=mandate.evidence_ids,
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

