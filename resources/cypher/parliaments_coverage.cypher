// Coverage-Query für Parlamente: Aggregiert Mandate-Daten pro parliament_id
// Robust gegen invalid dates (regex validation), zählt invalid/missing evidence

MATCH (m:Mandate)
WITH m.parliament_id as parliament_id, m
WHERE parliament_id IS NOT NULL AND parliament_id <> ""
WITH parliament_id,
     count(m) as mandates_count,
     collect(m) as mandates

UNWIND mandates as mandate
WITH parliament_id,
     mandates_count,
     mandate.start_date as start_date,
     mandate.end_date as end_date,
     mandate.id as mandate_id

// Validiere start_date: nur gültige ISO-Date-Strings (YYYY-MM-DD)
WITH parliament_id,
     mandates_count,
     CASE 
       WHEN start_date IS NOT NULL AND start_date =~ '^\\d{4}-\\d{2}-\\d{2}$' 
       THEN start_date 
       ELSE null 
     END as valid_start_date,
     CASE 
       WHEN start_date IS NOT NULL AND NOT (start_date =~ '^\\d{4}-\\d{2}-\\d{2}$') 
       THEN 1 
       ELSE 0 
     END as invalid_start_flag,
     CASE 
       WHEN end_date IS NOT NULL AND end_date =~ '^\\d{4}-\\d{2}-\\d{2}$' 
       THEN end_date 
       ELSE null 
     END as valid_end_date,
     CASE 
       WHEN end_date IS NOT NULL AND NOT (end_date =~ '^\\d{4}-\\d{2}-\\d{2}$') 
       THEN 1 
       ELSE 0 
     END as invalid_end_flag,
     mandate_id

// Aggregiere pro parliament_id
WITH parliament_id,
     mandates_count,
     collect(valid_start_date) as valid_starts,
     collect(valid_end_date) as valid_ends,
     sum(invalid_start_flag) as invalid_start_count,
     sum(invalid_end_flag) as invalid_end_count,
     collect(mandate_id) as mandate_ids

// Berechne min_start und max_end nur aus validen Dates
WITH parliament_id,
     mandates_count,
     [d IN valid_starts WHERE d IS NOT NULL] as filtered_starts,
     [d IN valid_ends WHERE d IS NOT NULL] as filtered_ends,
     invalid_start_count,
     invalid_end_count,
     mandate_ids

WITH parliament_id,
     mandates_count,
     CASE WHEN size(filtered_starts) > 0 THEN min(filtered_starts) ELSE null END as min_start,
     CASE WHEN size(filtered_ends) > 0 THEN max(filtered_ends) ELSE null END as max_end,
     invalid_start_count,
     invalid_end_count,
     mandate_ids

// Prüfe missing evidence: Mandates ohne SUPPORTED_BY Evidence URLs
OPTIONAL MATCH (m2:Mandate)-[:SUPPORTED_BY]->(e:Evidence)
WHERE m2.id IN mandate_ids AND (e.url IS NOT NULL AND e.url <> "" OR e.source_url IS NOT NULL AND e.source_url <> "")
WITH parliament_id,
     mandates_count,
     min_start,
     max_end,
     invalid_start_count,
     invalid_end_count,
     mandate_ids,
     collect(DISTINCT m2.id) as mandates_with_evidence

WITH parliament_id,
     mandates_count,
     min_start,
     max_end,
     invalid_start_count,
     invalid_end_count,
     mandates_count - size(mandates_with_evidence) as missing_evidence_count

RETURN parliament_id,
       mandates_count,
       min_start,
       max_end,
       invalid_start_count,
       invalid_end_count,
       missing_evidence_count
ORDER BY parliament_id

