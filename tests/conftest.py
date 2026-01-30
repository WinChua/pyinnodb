"""Configuration for PyInnoDB tests"""

import pytest
import tempfile
import os
from context import test_mysql8_ibd, test_mysql5_ibd


def pytest_configure(config):
    """Configure pytest settings"""
    config.addinivalue_line(
        "markers", "slow: marks tests as slow (deselect with '-m \"not slow\"')"
    )
    config.addinivalue_line(
        "markers", "integration: marks tests as integration tests"
    )
    config.addinivalue_line(
        "markers", "performance: marks tests as performance tests"
    )


def pytest_collection_modifyitems(config, items):
    """Modify test items during collection"""
    for item in items:
        # Mark tests based on file names
        if "performance" in item.nodeid:
            item.add_marker(pytest.mark.performance)
        if "integration" in item.nodeid:
            item.add_marker(pytest.mark.integration)
        if "error_handling" in item.nodeid:
            item.add_marker(pytest.mark.slow)


@pytest.fixture(scope="session")
def sample_mysql8_file():
    """Provide a sample MySQL 8 file for tests that need it"""
    if test_mysql8_ibd.exists():
        return str(test_mysql8_ibd)
    else:
        pytest.skip("MySQL 8 test file not available")


@pytest.fixture(scope="session")
def sample_mysql5_file():
    """Provide a sample MySQL 5 file for tests that need it"""
    if test_mysql5_ibd.exists():
        return str(test_mysql5_ibd)
    else:
        pytest.skip("MySQL 5 test file not available")


@pytest.fixture
def temp_test_file():
    """Create a temporary test file"""
    with tempfile.NamedTemporaryFile(delete=False) as tmp_file:
        yield tmp_file.name
    # Cleanup after test
    if os.path.exists(tmp_file.name):
        os.unlink(tmp_file.name)


@pytest.fixture(autouse=True)
def setup_logging():
    """Setup logging for tests"""
    import logging
    # Reduce noise from third-party libraries
    for name in ['urllib3', 'requests']:
        logging.getLogger(name).setLevel(logging.WARNING)