.PHONY: validate validate-governance validate-all warnings-audit warnings-baseline warnings-check build curation-apply

validate:
	@./scripts/validate.sh

validate-governance:
	@./scripts/validate_governance.sh

validate-all:
	@./scripts/validate_all.sh

warnings-audit:
	@mkdir -p artifacts
	@if [ "${VALIDATE_VIA_DOCKER:-1}" = "1" ]; then \
		docker compose run --rm scraper python -m langgraph_app.cli audit-warnings --mode integrity --output artifacts/warnings.audit.json; \
	else \
		python -m langgraph_app.cli audit-warnings --mode integrity --output artifacts/warnings.audit.json; \
	fi

warnings-baseline:
	@mkdir -p artifacts
	@if [ -f "artifacts/validate.integrity.json" ]; then \
		cp artifacts/validate.integrity.json artifacts/warnings.baseline.json; \
		echo "Baseline created from artifacts/validate.integrity.json" >&2; \
	else \
		echo "Error: artifacts/validate.integrity.json not found. Run 'make validate' first." >&2; \
		exit 1; \
	fi

warnings-check:
	@mkdir -p artifacts
	@if [ ! -f "artifacts/warnings.baseline.json" ]; then \
		echo "Error: artifacts/warnings.baseline.json not found. Run 'make warnings-baseline' first." >&2; \
		exit 1; \
	fi
	@if [ ! -f "artifacts/validate.integrity.json" ]; then \
		echo "Error: artifacts/validate.integrity.json not found. Run 'make validate' first." >&2; \
		exit 1; \
	fi
	@if [ "${VALIDATE_VIA_DOCKER:-1}" = "1" ]; then \
		docker compose run --rm scraper python -m langgraph_app.cli check-warning-budget \
			--baseline artifacts/warnings.baseline.json \
			--current artifacts/validate.integrity.json \
			--print-summary; \
	else \
		python -m langgraph_app.cli check-warning-budget \
			--baseline artifacts/warnings.baseline.json \
			--current artifacts/validate.integrity.json \
			--print-summary; \
	fi

build:
	docker compose build scraper

curation-apply:
	@echo "Applying term start overrides..."
	@docker compose run --rm scraper python -m langgraph_app.cli apply-term-start-overrides --input artifacts/term_starts.queue.yaml || exit 1
	@echo "Propagating legislature starts..."
	@docker compose run --rm scraper python -m langgraph_app.cli propagate-legislature-starts || exit 1
	@echo "Running validation..."
	@VALIDATE_VIA_DOCKER=1 make validate-all || exit 1
	@echo "✓ Curation apply completed"