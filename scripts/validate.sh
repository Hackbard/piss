#!/bin/bash
set -euo pipefail

# Get script directory and project root
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "${PROJECT_ROOT}"

# Determine runner (default: Docker, since Neo4j/Meilisearch run in Docker)
if [ "${VALIDATE_VIA_DOCKER:-1}" = "1" ]; then
    RUNNER="docker compose run --rm scraper scraper"
else
    RUNNER="scraper"
fi

# Create artifacts directory
mkdir -p artifacts

# Run integrity validation
# Temporarily disable set -e to capture exit code
set +e
echo "Running integrity validation..." >&2
${RUNNER} validate --json --mode integrity --quiet > artifacts/validate.integrity.json
EXIT_CODE=$?
set -e

# Print summary
python3 - <<'PY'
import json
import sys
try:
    with open("artifacts/validate.integrity.json", "r") as f:
        d = json.load(f)
    error_count = d.get("error_count", 0)
    warning_count = d.get("warning_count", 0)
    print(f"integrity: errors={error_count} warnings={warning_count}", file=sys.stderr)
except Exception as e:
    print(f"Error reading validation results: {e}", file=sys.stderr)
    sys.exit(1)
PY

exit $EXIT_CODE
