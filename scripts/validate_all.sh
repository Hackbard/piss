#!/bin/bash
set -euo pipefail

# Get script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

# Run integrity (blocking)
echo "=== Running integrity validation ===" >&2
cd "${PROJECT_ROOT}"
"${SCRIPT_DIR}/validate.sh"
INTEGRITY_EXIT=$?

# Run governance (non-blocking unless strict)
# We need to disable set -e temporarily to allow governance to fail without stopping the script
set +e
echo "" >&2
echo "=== Running governance validation ===" >&2
cd "${PROJECT_ROOT}"
"${SCRIPT_DIR}/validate_governance.sh"
GOVERNANCE_EXIT=$?
set -e

# Exit with integrity exit code (governance is non-blocking by default)
exit $INTEGRITY_EXIT
