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
- **Ollama** erforderlich: MVP benötigt Ollama für Parameter-Extraktion (verpflichtend)

### Environment

- **`PISS_TOOL_BASE_URL`**: default `http://localhost:8000/api/tools`
- **`PISS_OLLAMA_BASE_URL`**: default `http://192.168.178.185:11434/v1` (erforderlich für Parameter-Extraktion)
- **`PISS_OLLAMA_MODEL`**: default `ministral-3:14b` (erforderlich für Parameter-Extraktion)
- **`PISS_STRICT_EVIDENCE_DEFAULT`**: default `true`
- **`PISS_OPENAI_API_KEY`**: default `"ollama"` - API-Key für LLM (bei Ollama meist "ollama")

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

**Healthcheck-Optionen:**
```bash
# Healthcheck deaktivieren (nicht empfohlen)
python -m langgraph_app.cli --no-healthcheck "..."

# Healthcheck-Timeout anpassen (default: 5.0 Sekunden)
python -m langgraph_app.cli --health-timeout 10.0 "..."

# Warmup-Retry deaktivieren (default: aktiviert)
python -m langgraph_app.cli --no-health-warmup "..."
```

### Parameter-Extraktion

**LLM-only Strategie:**
1. Das MVP verwendet **ausschließlich** das LLM (Ollama) für die Parameter-Extraktion
2. Das LLM extrahiert nur Parameter aus der Frage (erfindet keine Fakten)
3. Tool-Aufruf und Formatter bleiben deterministisch (Ground Truth = Tool Gateway)
4. Wenn Ollama nicht erreichbar ist oder ungültiges JSON zurückgibt, wird eine klare Fehlermeldung ausgegeben

**Das LLM extrahiert:**
- `parliament_id`: Erlaubte Codes (NI, BT, HE, BW, BY, BE, BB, HB, HH, MV, NW, RP, SL, SN, ST, SH, TH)
- `party_code`: Uppercase (SPD, CDU, CSU, GRUENE, FDP, AFD, LINKE, ...)
- `from_date` / `to_date`: ISO-Format (YYYY-MM-DD)
- Bei unklaren Angaben: Felder als `null` (löst Clarification-Message aus)

**Fehlerbehandlung:**
Wenn Ollama nicht erreichbar ist oder die Parameter-Extraktion fehlschlägt, wird folgende Fehlermeldung ausgegeben:
```
LLM-Fehler: Parameter konnten nicht extrahiert werden (Ollama nicht erreichbar oder ungültiges JSON). 
Bitte Ollama prüfen oder Frage präzisieren.
```

**Wichtig:** Es gibt keinen deterministischen Fallback mehr. Das System benötigt ein funktionierendes Ollama-Instance.

### Preflight Healthcheck

Das MVP führt **standardmäßig** einen Healthcheck gegen Ollama durch, bevor der Graph gestartet wird (fail-fast). Dies verhindert, dass Fehler erst im Plan-Node erkannt werden.

**Verhalten:**
- **Default**: Healthcheck ist aktiviert
- Führt einen schnellen Request gegen den OpenAI-compat Endpoint aus (`POST /chat/completions`)
- Timeout: 5 Sekunden (konfigurierbar via `--health-timeout`)
- **Warmup-Retry**: Wenn `--health-warmup` aktiviert ist (default), wird bei Timeout ein Retry mit 30s Timeout durchgeführt, um Modell-Loading zu erlauben
- Bei Fehler: Exit-Code 2, klare Fehlermeldung mit Diagnose-Hinweisen

**CLI-Flags:**
- `--no-healthcheck`: Deaktiviert den Healthcheck (nicht empfohlen)
- `--health-timeout <seconds>`: Timeout für den Healthcheck (default: 5.0)
- `--health-warmup` / `--no-health-warmup`: Aktiviert/deaktiviert Warmup-Retry (default: aktiviert)

**Diagnose bei Fehlern:**

1. **Modelle anzeigen:**
   ```bash
   curl -sS http://192.168.178.185:11434/api/tags | jq .
   ```

2. **OpenAI-compat Endpoint testen:**
   ```bash
   curl -sS -X POST http://192.168.178.185:11434/v1/chat/completions \
     -H "Content-Type: application/json" \
     -d '{"model":"ministral-3:14b","messages":[{"role":"user","content":"ping"}],"max_tokens":1,"temperature":0}' | jq .
   ```

3. **ENV-Variablen prüfen:**
   - `PISS_OLLAMA_BASE_URL` (z.B. `http://192.168.178.185:11434/v1`)
   - `PISS_OLLAMA_MODEL` (z.B. `ministral-3:14b`)

**Debug-Ausgabe:**
Wenn `PISS_DEBUG=1` gesetzt ist, wird bei erfolgreichem Healthcheck eine Debug-Ausgabe mit Latenz angezeigt:
```
[DEBUG] Ollama OK: http://192.168.178.185:11434/v1 model=ministral-3:14b latency_ms=234
```

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


