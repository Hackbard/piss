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

# Run governance validation
# Temporarily disable set -e to capture exit code
set +e
echo "Running governance validation..." >&2
${RUNNER} validate --json --mode governance --quiet > artifacts/validate.governance.json
EXIT_CODE=$?
set -e

# Print summary
python3 - <<'PY'
import json
import sys
from collections import Counter

try:
    with open("artifacts/validate.governance.json", "r") as f:
        d = json.load(f)
    error_count = d.get("error_count", 0)
    warning_count = d.get("warning_count", 0)
    print(f"governance: errors={error_count} warnings={warning_count}", file=sys.stderr)
    
    errors = d.get("errors", [])
    if errors:
        error_codes = [e.get("code", "UNKNOWN") for e in errors]
        code_counts = Counter(error_codes)
        print("\nTop 10 error codes:", file=sys.stderr)
        for code, count in code_counts.most_common(10):
            print(f"  {code}: {count}", file=sys.stderr)
except Exception as e:
    print(f"Error reading validation results: {e}", file=sys.stderr)
    sys.exit(1)
PY

# Exit with validator code if strict, else always 0
if [ "${STRICT_GOVERNANCE:-0}" = "1" ]; then
    exit $EXIT_CODE
else
    exit 0
fi
