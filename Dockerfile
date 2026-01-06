FROM python:3.12-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

RUN pip install uv

COPY pyproject.toml README.md ./
COPY src/ ./src/
COPY config/ ./config/
COPY tests/ ./tests/
COPY contracts/ ./contracts/
COPY langgraph_app/ ./langgraph_app/
COPY langgraph_tools/ ./langgraph_tools/
RUN uv pip install --system -e ".[dev]"

CMD ["scraper", "--help"]

