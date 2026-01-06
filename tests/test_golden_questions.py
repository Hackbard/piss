from datetime import date

import pytest

from scraper.models.domain import EvidenceRef, Legislature, Mandate, Parliament, Party, Person
from scraper.utils.intervals import filter_mandates_by_overlap
from scraper.validation.validator import DataValidator


class TestGoldenQuestions:
    """Golden Questions - Regression Tests."""
    
    @pytest.fixture
    def parliament_nds(self):
        return Parliament(
            id="parliament-nds",
            name="Niedersächsischer Landtag",
            level="state",
            state_code="NI",
        )
    
    @pytest.fixture
    def legislature_17(self, parliament_nds):
        return Legislature(
            id="legislature-nds-17",
            parliament_id=parliament_nds.id,
            name="17. Landtag Niedersachsen",
            start_date="2013-01-20",
            end_date="2017-11-14",
        )
    
    @pytest.fixture
    def party_spd(self):
        return Party(
            id="party-spd",
            code="SPD",
            name="Sozialdemokratische Partei Deutschlands",
        )
    
    @pytest.fixture
    def party_cdu(self):
        return Party(
            id="party-cdu",
            code="CDU",
            name="Christlich Demokratische Union",
        )
    
    @pytest.fixture
    def person_weil(self):
        return Person(
            id="person-weil",
            name="Stephan Weil",
            wikipedia_title="Stephan_Weil",
            normalized_name="stephan weil",
        )
    
    @pytest.fixture
    def mandates(self, person_weil, legislature_17, parliament_nds, party_spd, party_cdu):
        evidence_ref = EvidenceRef(
            evidence_id="evidence-1",
            purpose="membership_row",
        )
        
        return [
            Mandate(
                id="mandate-1",
                person_id=person_weil.id,
                parliament_id=parliament_nds.id,
                legislature_id=legislature_17.id,
                party_code=party_spd.code,
                start_date="2013-01-20",
                end_date="2017-11-14",
                role="MdL",
                evidence_refs=[evidence_ref],
            ),
            Mandate(
                id="mandate-2",
                person_id=person_weil.id,
                parliament_id=parliament_nds.id,
                legislature_id=legislature_17.id,
                party_code=party_cdu.code,
                start_date="2015-06-01",
                end_date="2016-12-31",
                role="MdL",
                evidence_refs=[evidence_ref],
            ),
        ]
    
    def test_golden_question_spd_nds_2014_2020(self, mandates, party_spd, parliament_nds):
        """Golden Question: SPD im Landtag NDS 2014-2020"""
        filtered = filter_mandates_by_overlap(
            mandates,
            from_date=date(2014, 1, 1),
            to_date=date(2020, 12, 31),
        )
        
        spd_mandates = [m for m in filtered if m.party_code == party_spd.code and m.parliament_id == parliament_nds.id]
        
        assert len(spd_mandates) >= 1
        assert all(m.evidence_refs or m.evidence_ids for m in spd_mandates)
    
    def test_golden_question_evidence_urls(self, mandates):
        """Every mandate row must have evidence_urls (non-empty)"""
        for mandate in mandates:
            assert mandate.evidence_refs or mandate.evidence_ids
            if mandate.evidence_refs:
                assert all(ref.evidence_id for ref in mandate.evidence_refs)
    
    def test_golden_question_open_ended_mandate(self, person_weil, legislature_17, parliament_nds, party_spd):
        """Test mandate with open end_date"""
        evidence_ref = EvidenceRef(
            evidence_id="evidence-2",
            purpose="membership_row",
        )
        
        open_mandate = Mandate(
            id="mandate-open",
            person_id=person_weil.id,
            parliament_id=parliament_nds.id,
            legislature_id=legislature_17.id,
            party_code=party_spd.code,
            start_date="2017-11-15",
            end_date=None,
            role="MdL",
            evidence_refs=[evidence_ref],
        )
        
        assert open_mandate.end_date is None
        assert open_mandate.evidence_refs
    
    def test_golden_question_party_switch(self, mandates, person_weil):
        """Test party switch (two mandate segments)"""
        person_mandates = [m for m in mandates if m.person_id == person_weil.id]
        
        assert len(person_mandates) == 2
        assert person_mandates[0].party_code != person_mandates[1].party_code
    
    def test_golden_question_validator_passes(self, mandates, party_spd, party_cdu):
        """Validator should pass for golden question data"""
        parties = [party_spd, party_cdu]
        validator = DataValidator(strict_mode=False)
        result = validator.validate_all(mandates=mandates, parties=parties)
        
        assert not result.has_errors()
    
    def test_golden_question_legislature_17_nds(self, legislature_17, parliament_nds):
        """Test Legislature '17. Landtag Niedersachsen' exists"""
        assert legislature_17.name == "17. Landtag Niedersachsen"
        assert legislature_17.parliament_id == parliament_nds.id
        assert legislature_17.start_date == "2013-01-20"
        assert legislature_17.end_date == "2017-11-14"

