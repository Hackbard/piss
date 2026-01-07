### Minimaler LangGraph-Runner (MVP): `members.list`

Dieses MVP beantwortet Mitglieder-Fragen **ausschließlich auf Basis des Laravel Tool-Gateways** (keine Halluzinationen) und formatiert eine deterministische Antwort inkl. Quellen.

### Voraussetzungen

- **Tool-Gateway** lokal: `http://localhost:8000/api/tools`
- **Ollama** extern: `http://192.168.178.185:11434/v1` (für dieses MVP nicht zwingend genutzt)
- Python >= 3.12 (Repo-Standard)

### Environment

- **`PISS_TOOL_BASE_URL`**: default `http://localhost:8000/api/tools`
- **`OLLAMA_BASE_URL`**: default `http://192.168.178.185:11434/v1`
- **`OLLAMA_MODEL`**: default `ministral-3:14b`
- **`PISS_STRICT_EVIDENCE_DEFAULT`**: default `true`

### Run

Einmalige Frage:

```bash
python -m langgraph_app.cli "Alle SPD-Mitglieder im Landtag Niedersachsen zwischen 2014-2020"
```

Interaktiv (startet mit Default-Frage):

```bash
python -m langgraph_app.cli
```

### Beispieloutput (gekürzt)

```text
SPD-Mitglieder im Landtag Niedersachsen (01.01.2014–31.12.2020)
Anzahl: 3

- Vorname Nachname (Wikipedia_Titel) – 01.01.2014 … 31.12.2020
- ...

Quellen:
- https://de.wikipedia.org/wiki/...
- ...
```


