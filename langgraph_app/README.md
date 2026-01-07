### Minimaler LangGraph-Runner (MVP): `members.list`

Dieses MVP beantwortet Mitglieder-Fragen **ausschließlich auf Basis des Laravel Tool-Gateways** (keine Halluzinationen) und formatiert eine deterministische Antwort inkl. Quellen.

**Features:**
- ✅ Robuste Parameter-Extraktion (Partei, Parlament, Zeitraum)
- ✅ Automatische Pagination mit Merging/Deduplizierung
- ✅ Multiple Output-Formate (text, json, markdown)
- ✅ Konfigurierbare Quellen-Anzeige
- ✅ Unterstützt alle 16 Bundesländer + Bundestag

### Voraussetzungen

- **Tool-Gateway** lokal: `http://localhost:8000/api/tools`
- Python >= 3.12 (Repo-Standard)
- **Ollama** optional: Nur für vollständigen Orchestrator (nicht für MVP)

### Environment

- **`PISS_TOOL_BASE_URL`**: default `http://localhost:8000/api/tools`
- **`OLLAMA_BASE_URL`**: default `http://192.168.178.185:11434/v1` (optional)
- **`OLLAMA_MODEL`**: default `ministral-3:14b` (optional)
- **`PISS_STRICT_EVIDENCE_DEFAULT`**: default `true`

### CLI Usage

**Einmalige Frage:**
```bash
python -m langgraph_app.cli "Alle SPD-Mitglieder im Landtag Niedersachsen zwischen 2014-2020"
```

**Mit Output-Format:**
```bash
# JSON Output
python -m langgraph_app.cli "Liste CDU im Bundestag 2018-2021" --format json

# Markdown Output
python -m langgraph_app.cli "Alle Grünen in Hessen 2020-2025" --format md

# Text Output (default)
python -m langgraph_app.cli "SPD Mitglieder in Bayern 2014-2020" --format text
```

**Quellen-Anzeige konfigurieren:**
```bash
# Keine Quellen
python -m langgraph_app.cli "..." --sources none

# Quellen oben (default)
python -m langgraph_app.cli "..." --sources top

# Quellen pro Person
python -m langgraph_app.cli "..." --sources per-person

# Maximale Anzahl Quellen
python -m langgraph_app.cli "..." --max-sources 50
```

**Interaktiv (startet mit Default-Frage):**
```bash
python -m langgraph_app.cli
```

### Parameter-Extraktion

Der Parser erkennt automatisch:

**Parteien:**
- SPD, CDU, CSU, FDP, LINKE, AFD
- Grüne, GRUENE, GRÜNE → GRUENE
- Case-insensitive

**Parlamente:**
- Alle 16 Bundesländer (Niedersachsen, Hessen, Bayern, etc.)
- Bundestag, Deutscher Bundestag
- Varianten: "hessischer landtag", "landtag niedersachsen", etc.

**Zeiträume:**
- `2014-2020` oder `2014–2020` → 2014-01-01 bis 2020-12-31
- `zwischen 2014 und 2020` → 2014-01-01 bis 2020-12-31
- `ab 2014` → 2014-01-01 bis heute
- `bis 2020` → 0001-01-01 bis 2020-12-31
- Einzeljahre: `2018` → 2018-01-01 bis 2018-12-31

### Pagination

Das MVP ruft automatisch alle Seiten ab:
- Startet mit `limit=200, offset=0`
- Iteriert bis alle Ergebnisse geladen sind
- Merged/Dedupliziert nach `person_id`
- Aggregiert: min `active_first_start_date`, max `active_last_end_date`
- Union der `evidence_urls` mit Deduplizierung

### Output-Formate

**Text (default):**
```text
SPD-Mitglieder im Landtag Niedersachsen (01.01.2014–31.12.2020)
Anzahl: 3

- Vorname Nachname (Wikipedia_Titel) – 2014-01-01 … 2020-12-31
- ...

Quellen:
- https://de.wikipedia.org/wiki/...
```

**Markdown:**
```markdown
# SPD-Mitglieder im Landtag Niedersachsen

**Zeitraum:** 01.01.2014–31.12.2020
**Anzahl:** 3

## Mitglieder

- **Vorname Nachname** (Wikipedia_Titel) – 2014-01-01 … 2020-12-31
```

**JSON:**
```json
{
  "members": [
    {
      "person_name": "Vorname Nachname",
      "wikipedia_title": "Wikipedia_Titel",
      "active_first_start_date": "2014-01-01",
      "active_last_end_date": "2020-12-31",
      "evidence_urls": ["https://..."]
    }
  ],
  "meta": {...}
}
```

### Datumsanzeige

Die Ausgabe verwendet die neuen `active_*` Felder:
- **Hauptanzeige**: `active_first_start_date … active_last_end_date`
- **Fallback**: Falls `active_*` fehlen, werden `first_start_date`/`last_end_date` verwendet
- **Mandat-Hinweis**: Wenn `last_end_date > active_last_end_date`, wird `(Mandat bis <last_end_date>)` angezeigt
- **Offenes Mandat**: Wenn `end_date` fehlt, wird `… (offen)` angezeigt


