.PHONY: lint format format-check mypy test check

# Run ruff linter
lint:
	uv run --with ruff ruff check .

# Run ruff formatter (fix and format)
format:
	uv run --with ruff ruff check --fix .
	uv run --with ruff ruff format .

# Check formatting without making changes
format-check:
	uv run --with ruff ruff format --check .

# Run mypy type checker
mypy:
	uv run --with mypy mypy .

# Run tests
test:
	uv run --with pytest pytest

# Run all checks
check: lint format-check mypy
