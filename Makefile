# Victoria Metrics Jobs Service Makefile
# Provides convenient targets for common operations

.PHONY: help clean test lint format dev-up dev-down

# Default target
help:
	@echo "Victoria Metrics Jobs Service - Available targets:"
	@echo ""
	@echo "Development:"
	@echo "  install-deps    Install Python dependencies"
	@echo "  dev-up          Bootstrap development environment"
	@echo "  dev-down        Clean up development environment"
	@echo "  test           Run all tests"
	@echo "  test-unit      Run unit tests only"
	@echo "  test-integration Run integration tests only"
	@echo "  test-coverage  Run tests with coverage report"
	@echo "  test-watch     Run tests in watch mode"
	@echo "  lint           Run linting"
	@echo "  format         Format code"
	@echo "  clean          Clean build artifacts"
	@echo ""
	@echo "Service Management:"
	@echo "  start          Start the service"
	@echo "  stop           Stop the service"
	@echo "  restart        Restart the service"
	@echo "  status         Check service status"
	@echo "  logs           View service logs"
	@echo ""
	@echo "Configuration:"
	@echo "  validate-config Validate configuration file"
	@echo ""
	@echo "Manual Job Execution:"
	@echo "  run-extractor   Run extractor job manually"
	@echo "  run-apex        Run apex collector job manually"
	@echo "  list-jobs       List available job configurations"

# Development targets
install-deps:
	poetry install

test:
	poetry run pytest

test-unit:
	poetry run pytest tests/unit -m unit

test-integration:
	poetry run pytest tests/integration -m integration

test-coverage:
	poetry run pytest --cov=scheduler --cov-report=html --cov-report=term

test-watch:
	poetry run pytest-watch

lint:
	poetry run flake8 scheduler/ main.py
	poetry run mypy scheduler/ main.py

format:
	poetry run black scheduler/ main.py
	poetry run isort scheduler/ main.py

clean:
	find . -type f -name "*.pyc" -delete
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".pytest_cache" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name "htmlcov" -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name ".coverage" -delete
	find . -type f -name "coverage.xml" -delete
	rm -rf build/ dist/ *.egg-info/

# Deployment targets (removed - no longer using Ansible)

# Service management targets
start:
	sudo systemctl start victoria_metrics_jobs

stop:
	sudo systemctl stop victoria_metrics_jobs

restart:
	sudo systemctl restart victoria_metrics_jobs

status:
	sudo systemctl status victoria_metrics_jobs

logs:
	sudo journalctl -u victoria_metrics_jobs -f

# Configuration targets
validate-config:
	python -c "import yaml; yaml.safe_load(open('config.yml'))" && echo "Configuration is valid"

# Development environment bootstrap
dev-up: install-deps
	@echo "Bootstrapping development environment..."
	@mkdir -p ~/.config/victoria_metrics_jobs
	@mkdir -p ~/.local/log/scheduler
	@mkdir -p ~/.local/share/scheduler
	@echo "Development environment bootstrap complete!"
	@echo "Configuration files are now in scheduler/ and jobs/ directories"
	@echo "Set ENVIRONMENT=dev to use development configuration"
	@echo "Run 'make dev-run' to start the scheduler in development mode"

# Development environment cleanup
dev-down:
	@echo "Cleaning up development environment..."
	@rm -rf ~/.config/victoria_metrics_jobs
	@rm -rf ~/.local/log/scheduler
	@rm -rf ~/.local/share/scheduler
	@echo "Development environment cleaned up!"

# Development run
dev-run:
	@echo "Starting scheduler in development mode..."
	@ENVIRONMENT=dev python -m victoria_metrics_jobs.scheduler.service --config victoria_metrics_jobs/victoria_metrics_jobs.yml

# Run main script
run:
	@echo "Starting Victoria Metrics Jobs service..."
	@ENVIRONMENT=dev poetry run python -m victoria_metrics_jobs.victoria_metrics_jobs --config victoria_metrics_jobs/victoria_metrics_jobs.yml

# Manual job execution
run-extractor:
	@echo "Running extractor job manually..."
	@ENVIRONMENT=dev TARGET_URL=http://target.com TARGET_TOKEN=test_token SYSTEM_A_SOURCE_URL=http://system-a.com SYSTEM_A_SOURCE_TOKEN=token_a SYSTEM_B_SOURCE_URL=http://system-b.com SYSTEM_B_SOURCE_TOKEN=token_b APEX_SOURCE_URL=http://apex.com APEX_SOURCE_TOKEN=apex_token DB_PASSWORD=testpass poetry run python -m victoria_metrics_jobs.jobs.extractor --config victoria_metrics_jobs/victoria_metrics_jobs.yml --job-id system_a_extractor

run-apex:
	@echo "Running apex collector job manually..."
	@ENVIRONMENT=dev TARGET_URL=http://target.com TARGET_TOKEN=test_token APEX_SOURCE_URL=http://apex.com APEX_SOURCE_TOKEN=apex_token DB_PASSWORD=testpass poetry run python -m victoria_metrics_jobs.jobs.apex_collector --config victoria_metrics_jobs/victoria_metrics_jobs.yml --job-id apex_collector

list-jobs:
	@echo "Available job configurations..."
	@ENVIRONMENT=dev TARGET_URL=http://target.com TARGET_TOKEN=test_token SYSTEM_A_SOURCE_URL=http://system-a.com SYSTEM_A_SOURCE_TOKEN=token_a SYSTEM_B_SOURCE_URL=http://system-b.com SYSTEM_B_SOURCE_TOKEN=token_b APEX_SOURCE_URL=http://apex.com APEX_SOURCE_TOKEN=apex_token DB_PASSWORD=testpass poetry run python -m victoria_metrics_jobs.jobs.extractor --config victoria_metrics_jobs/victoria_metrics_jobs.yml --list-jobs

# Development setup (legacy)
dev-setup: dev-up
	@echo "Development environment setup complete!"
	@echo "Run 'make dev-run' to start the scheduler in development mode"
