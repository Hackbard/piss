## Annahmen (PIS)

Dieses Dokument hält Annahmen fest, die (noch) nicht vollständig durch verifizierte Datenquellen oder Implementierung abgesichert sind. Ziel ist, Unsicherheiten transparent zu halten und die Architektur so zu kapseln, dass Quellen/Heuristiken später austauschbar sind.

### Bundesquellen / “Bundes-API”
- **DIP (Deutscher Bundestag)** wird als primäre offizielle Bundesquelle angenommen (OpenAPI, API-Key erforderlich). Referenz: `https://search.dip.bundestag.de/api/v1/swagger-ui/`.
- **Genutzte DIP Endpunkte (PoC)**:
  - `GET /person` (Personenliste; Filter `f.wahlperiode`; Cursor-Pagination)
- Coverage/Qualität für:
  - **Mandate**: DIP liefert Personen/rollenbezogene Daten, aber nicht zwingend alle Landtage/Bundesrat.
  - **Legislaturperioden**: DIP deckt Wahlperioden des Bundestages ab; für Landtage/Bundesrat sind weitere Quellen nötig.
- Weitere “Bundes”-Quellen (z.B. Bundesrat, Bundesregierung) werden als separate Connectoren behandelt. Falls keine stabile API verfügbar ist, wird ein Interface + Stub gebaut und als “official_other” modelliert.

### Deutschland-only Scope
- Alle Entities beziehen sich auf Deutschland; internationale Mandate/Ämter werden ignoriert oder als “out_of_scope” markiert.

### Canonical Identity
- **Wikidata QID** ist der beste globale Schlüssel, aber nicht immer vorhanden (z.B. fehlende Links in offiziellen Quellen).
- Wenn QID fehlt, werden nur **hochpräzise** heuristische Matches automatisch akzeptiert; Ambiguität erzeugt `pending` Dupe-Kandidaten (kein stiller Merge).

### Zeitmodell
- Start/Ende für Mandate und Rollen können offen sein (`end_date = null`).
- Unklare Datumsangaben (Monat/Jahr) werden als `*_raw` + `*_precision` gespeichert; `*_date` wird nur gesetzt, wenn day-precision vorliegt.

