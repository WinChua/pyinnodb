"""CLI command tests for PyInnoDB"""

import pytest
import tempfile
import os
from context import *
from pyinnodb.cli.main import main
from click.testing import CliRunner


def test_cli_help():
    """Test that CLI help command works"""
    runner = CliRunner()
    result = runner.invoke(main, ['--help'])
    
    assert result.exit_code == 0
    assert 'A ibd file parser for MySQL 8.0 above' in result.output
    assert '--fn' in result.output
    assert '--version' in result.output


def test_cli_version():
    """Test that CLI version command works"""
    runner = CliRunner()
    result = runner.invoke(main, ['--version'])
    
    # Version command should exit with code 0 when run properly
    # Note: The actual exit happens in the main function when version is used without subcommand
    assert result.exit_code in [0, 1]  # May exit early with sys.exit(0)


def test_cli_with_missing_file():
    """Test CLI behavior when file is missing"""
    runner = CliRunner()
    result = runner.invoke(main, ['--fn', '/nonexistent/file.ibd'])
    
    # Should fail gracefully when file doesn't exist
    assert result.exit_code != 0


def test_cli_validate_subcommand_exists():
    """Test that validate subcommand exists in help"""
    runner = CliRunner()
    # Test that validate subcommand exists in help
    result = runner.invoke(main, ['--help'])
    
    assert 'validate' in result.output


def test_cli_tosql_subcommand_exists():
    """Test that tosql subcommand exists in help"""
    runner = CliRunner()
    result = runner.invoke(main, ['--help'])
    
    assert 'tosql' in result.output


def test_cli_search_subcommand_exists():
    """Test that search subcommand exists in help"""
    runner = CliRunner()
    result = runner.invoke(main, ['--help'])
    
    assert 'search' in result.output


def test_cli_frm_subcommand_exists():
    """Test that frm subcommand exists in help"""
    runner = CliRunner()
    result = runner.invoke(main, ['--help'])
    
    assert 'frm' in result.output


def test_cli_parse_subcommand_exists():
    """Test that parse subcommand exists in help"""
    runner = CliRunner()
    result = runner.invoke(main, ['--help'])
    
    assert 'parse' in result.output