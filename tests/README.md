# PyInnoDB Tests

This directory contains the comprehensive test suite for PyInnoDB.

## Test Structure

- `unit/` - Unit tests for individual components
- `integration/` - Integration tests for component interactions
- `functional/` - Functional tests for complete workflows
- `test_basic_parsing.py` - Basic parsing functionality tests
- `test_cli_commands.py` - Command-line interface tests
- `test_constants.py` - Constant and utility function tests
- `test_integration.py` - Full integration tests
- `test_error_handling.py` - Error handling and edge case tests
- `test_performance.py` - Performance and stress tests
- `conftest.py` - Test configuration and fixtures
- `context.py` - Test context and utilities
- `containerOp.py` - Container operations for testing
- `pytest.ini` - Pytest configuration

## Running Tests

### Install test dependencies:
```bash
pip install -r tests/requirements-test.txt
```

### Run all tests:
```bash
pytest
```

### Run specific test categories:
```bash
# Run only unit tests
pytest tests/unit/

# Run only integration tests
pytest tests/integration/

# Run only performance tests
pytest -m performance

# Run tests with coverage
pytest --cov=src/pyinnodb --cov-report=html

# Run tests verbosely
pytest -v
```

### Run tests with specific markers:
```bash
# Skip slow tests
pytest -m "not slow"

# Only run integration tests
pytest -m integration
```

## Test Categories

### Unit Tests
- Individual function/method tests
- Fast execution
- No external dependencies
- Focus on core logic

### Integration Tests
- Multi-component interactions
- Real data files where available
- End-to-end workflow validation

### Functional Tests
- Complete feature validation
- CLI command testing
- Output verification

### Performance Tests
- Speed benchmarks
- Memory usage validation
- Scalability assessment

### Error Handling Tests
- Invalid input handling
- Corrupted file processing
- Edge case management

## Test Data

The test suite uses sample `.ibd` files for MySQL 5.7 and 8.0. These are downloaded automatically when tests requiring them are run for the first time.

## Contributing

When adding new features to PyInnoDB, please include corresponding tests in the appropriate category. All tests should follow the existing patterns and maintain high code coverage.