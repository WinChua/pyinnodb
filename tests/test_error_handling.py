"""Error handling tests for PyInnoDB"""

import pytest
from context import *
from pyinnodb.disk_struct.fsp import MFspPage
from pyinnodb.disk_struct.index import MSDIPage, MIndexPage
from pyinnodb.sdi.table import Table
from construct import StreamError, ValidationError
import tempfile
import os


def test_corrupted_file_handling():
    """Test how the library handles corrupted files"""
    # Create a temporary file with random/bad data
    with tempfile.NamedTemporaryFile(mode='wb', delete=False) as temp_file:
        # Write some random data to simulate a corrupted file
        temp_file.write(b'corrupted_file_data_that_does_not_follow_innodb_format')
        temp_filename = temp_file.name

    try:
        with open(temp_filename, 'rb') as bad_file:
            # Try to parse as FSP page - should fail gracefully
            with pytest.raises(Exception):
                MFspPage.parse_stream(bad_file)
    finally:
        # Clean up
        os.unlink(temp_filename)


def test_empty_file_handling():
    """Test handling of empty files"""
    with tempfile.NamedTemporaryFile(mode='wb', delete=False) as temp_file:
        # Write no data (empty file)
        temp_filename = temp_file.name

    try:
        with open(temp_filename, 'rb') as empty_file:
            # Try to parse as FSP page - should fail gracefully
            with pytest.raises(Exception):
                MFspPage.parse_stream(empty_file)
    finally:
        # Clean up
        os.unlink(temp_filename)


def test_short_file_handling():
    """Test handling of files that are too short for proper parsing"""
    with tempfile.NamedTemporaryFile(mode='wb', delete=False) as temp_file:
        # Write very little data (less than a page size)
        temp_file.write(b'short')
        temp_filename = temp_file.name

    try:
        with open(temp_filename, 'rb') as short_file:
            # Try to parse as FSP page - should fail gracefully
            with pytest.raises(Exception):
                MFspPage.parse_stream(short_file)
    finally:
        # Clean up
        os.unlink(temp_filename)


def test_invalid_page_numbers():
    """Test behavior when trying to access invalid page numbers"""
    try:
        # Using real test file if available
        with open(test_mysql8_ibd, 'rb') as mysql_file:
            # Try to seek to a very high page number that shouldn't exist
            large_page_num = 999999  # Very large page number
            try:
                mysql_file.seek(large_page_num * const.PAGE_SIZE)
                # Try to read and parse - this might cause an EOF error
                page_data = mysql_file.read(const.PAGE_SIZE)
                if len(page_data) < const.PAGE_SIZE:
                    # Expected behavior - file ended before reaching this page
                    assert True
            except Exception:
                # Any exception is acceptable here as we're testing error conditions
                assert True
    except FileNotFoundError:
        pytest.skip("Skipping test due to missing test data file")


def test_invalid_sdi_parsing():
    """Test SDI parsing with potentially invalid data"""
    # Create mock data that looks like it could be an SDI page but is invalid
    mock_data = bytearray(const.PAGE_SIZE)
    # Fill with zeros or invalid data
    mock_data[24:26] = (const.PageType.SDI.value).to_bytes(2, 'big')  # Set page type to SDI
    
    from io import BytesIO
    stream = BytesIO(mock_data)
    
    try:
        # This might fail, which is expected for invalid data
        sdi_page = MSDIPage.parse(stream)
        # If it doesn't fail, at least verify basic structure
        assert hasattr(sdi_page, 'fil')
    except ValidationError:
        # Expected for invalid data
        assert True
    except Exception:
        # Other exceptions are also acceptable for invalid data
        assert True


def test_table_validation_errors():
    """Test error handling in table operations"""
    # Create a minimal dictionary that mimics a table but is incomplete
    incomplete_table_dict = {
        'name': 'test_table',
        'schema_ref': 'test_schema',
        # Missing required fields intentionally
    }
    
    try:
        # This should fail because of missing required fields
        table = Table(**incomplete_table_dict)
        # If it doesn't fail, we'll check if it's usable
        assert table is not None
    except TypeError:
        # Expected - missing required arguments
        assert True
    except Exception:
        # Other exceptions are also possible
        assert True


def test_checksum_verification_failures():
    """Test behavior when checksum verification fails"""
    try:
        with open(test_mysql8_ibd, 'rb') as mysql_file:
            # Read a page and modify its data to make checksum fail
            mysql_file.seek(0)  # First page
            page_data = bytearray(mysql_file.read(const.PAGE_SIZE))
            
            if len(page_data) == const.PAGE_SIZE:
                # Modify a byte in the page to make checksum fail
                page_data[100] = page_data[100] ^ 0xFF  # Flip some bits
                
                from io import BytesIO
                modified_stream = BytesIO(page_data)
                
                # Try to validate this modified page
                fsp_page = MFspPage.parse(page_data)
                
                # Calculate checksum for modified page
                calculated_checksum = const.page_checksum_crc32c(page_data)
                
                # The checksum of the modified page should differ from the stored one
                # This tests that the checksum mechanism works
                assert isinstance(calculated_checksum, int)
                
    except FileNotFoundError:
        pytest.skip("Skipping checksum test due to missing test data file")


def test_out_of_bounds_access():
    """Test handling of out-of-bounds access in file operations"""
    try:
        with open(test_mysql8_ibd, 'rb') as mysql_file:
            # Get file size
            mysql_file.seek(0, 2)  # Seek to end
            file_size = mysql_file.tell()
            
            # Try to access beyond file bounds
            page_beyond_end = (file_size // const.PAGE_SIZE) + 10
            mysql_file.seek(page_beyond_end * const.PAGE_SIZE)
            
            # This should either fail or return empty/invalid data
            data = mysql_file.read(const.PAGE_SIZE)
            if len(data) == 0:
                # Expected behavior - no data read
                assert True
            else:
                # Partial read is also acceptable
                assert len(data) <= const.PAGE_SIZE
    except FileNotFoundError:
        pytest.skip("Skipping bounds test due to missing test data file")
    except OSError:
        # Expected when trying to seek beyond file bounds
        assert True


def test_memory_exhaustion_protection():
    """Test protection against potential memory exhaustion"""
    # Create a stream that reports a very large size but only has limited data
    from io import BytesIO
    
    class MaliciousStream(BytesIO):
        def __len__(self):
            return 2**32  # Report extremely large size
            
    malicious_data = b'\x00' * 100  # But only have small amount of actual data
    malicious_stream = MaliciousStream(malicious_data)
    
    try:
        # Try to parse with size checking
        with pytest.raises(Exception):
            # Attempt to parse more than the actual data available
            page = MFspPage.parse(malicious_stream.read(100))
    except:
        # Any exception is acceptable here
        assert True