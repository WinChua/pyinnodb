"""Performance tests for PyInnoDB"""

import pytest
import time
from context import *
from pyinnodb.disk_struct.fsp import MFspPage
from pyinnodb.disk_struct.index import MSDIPage, MIndexPage
from pyinnodb.sdi.table import Table
import tempfile
import os


def test_parsing_speed_large_file():
    """Test parsing speed for reasonably sized files"""
    try:
        # Test FSP page parsing speed
        start_time = time.time()
        
        with open(test_mysql8_ibd, 'rb') as mysql_file:
            fsp_page = MFspPage.parse_stream(mysql_file)
        
        elapsed = time.time() - start_time
        
        # Parsing should be reasonably fast (under 1 second for FSP header)
        assert elapsed < 1.0, f"FSP parsing took too long: {elapsed:.3f}s"
        
        print(f"FSP page parsing completed in {elapsed:.3f}s")
        
    except FileNotFoundError:
        pytest.skip("Skipping performance test due to missing test data file")


def test_multiple_page_parsing_speed():
    """Test speed of parsing multiple pages"""
    try:
        start_time = time.time()
        
        with open(test_mysql8_ibd, 'rb') as mysql_file:
            # Parse several pages quickly
            for page_no in [0, 1, 2, 3, 4]:
                mysql_file.seek(page_no * const.PAGE_SIZE)
                try:
                    page = MFspPage.parse_stream(mysql_file) if page_no == 0 else MIndexPage.parse_stream(mysql_file)
                except:
                    # Some pages might not be the expected type, which is OK
                    mysql_file.seek(page_no * const.PAGE_SIZE)
                    # Try parsing as general page
                    page = MIndexPage.parse_stream(mysql_file)
        
        elapsed = time.time() - start_time
        
        # Should parse 5 pages in reasonable time
        assert elapsed < 2.0, f"Multiple page parsing took too long: {elapsed:.3f}s"
        
        print(f"Parsed 5 pages in {elapsed:.3f}s")
        
    except FileNotFoundError:
        pytest.skip("Skipping performance test due to missing test data file")


def test_table_iteration_performance():
    """Test performance of iterating through table records"""
    try:
        # Parse the table structure
        start_time = time.time()
        
        with open(test_mysql8_ibd, 'rb') as mysql_file:
            fsp_page = MFspPage.parse_stream(mysql_file)
            sdi_page_no = fsp_page.get_sdi_page_no_with_guess(mysql_file)
            
            if sdi_page_no is not None:
                mysql_file.seek(sdi_page_no * const.PAGE_SIZE)
                sdi_page = MSDIPage.parse_stream(mysql_file)
                
                dd_object_info = sdi_page.ddl(mysql_file, 0)
                table_object = Table(**dd_object_info["dd_object"])
                
                # Measure time to iterate through records
                iteration_start = time.time()
                records = table_object.iter_record(mysql_file)
                iteration_time = time.time() - iteration_start
                
                print(f"Retrieved {len(records)} records in {iteration_time:.3f}s")
                
                # Iteration should be reasonably fast
                assert iteration_time < 5.0, f"Record iteration took too long: {iteration_time:.3f}s"
        
        total_elapsed = time.time() - start_time
        print(f"Total table processing time: {total_elapsed:.3f}s")
        
    except FileNotFoundError:
        pytest.skip("Skipping performance test due to missing test data file")


def test_memory_usage_consistency():
    """Test that memory usage remains consistent during parsing"""
    import psutil
    import os
    
    try:
        # Get initial memory usage
        process = psutil.Process(os.getpid())
        initial_memory = process.memory_info().rss / 1024 / 1024  # MB
        
        # Perform parsing operations
        with open(test_mysql8_ibd, 'rb') as mysql_file:
            for i in range(10):  # Parse same page multiple times
                mysql_file.seek(0)  # Go back to beginning
                fsp_page = MFspPage.parse_stream(mysql_file)
        
        # Get final memory usage
        final_memory = process.memory_info().rss / 1024 / 1024  # MB
        
        memory_increase = final_memory - initial_memory
        
        # Memory increase should be reasonable (less than 10MB for this operation)
        assert memory_increase < 10.0, f"Memory increase too high: {memory_increase:.2f}MB"
        
        print(f"Memory usage: started at {initial_memory:.1f}MB, ended at {final_memory:.1f}MB, increase: {memory_increase:.2f}MB")
        
    except ImportError:
        # psutil not available, skip this specific test
        pytest.skip("psutil not available for memory testing")
    except FileNotFoundError:
        pytest.skip("Skipping performance test due to missing test data file")


def test_checksum_calculation_performance():
    """Test performance of checksum calculations"""
    try:
        # Read a page to test checksum performance
        with open(test_mysql8_ibd, 'rb') as mysql_file:
            mysql_file.seek(0)
            page_data = mysql_file.read(const.PAGE_SIZE)
        
        if len(page_data) == const.PAGE_SIZE:
            # Test checksum calculation speed
            start_time = time.time()
            
            # Calculate checksum multiple times
            for i in range(100):
                checksum = const.page_checksum_crc32c(page_data)
            
            elapsed = time.time() - start_time
            
            # Should calculate 100 checksums relatively quickly
            assert elapsed < 1.0, f"Checksum calculation too slow: {elapsed:.3f}s for 100 iterations"
            
            print(f"Calculated 100 checksums in {elapsed:.3f}s ({elapsed/100*1000:.3f}ms per checksum)")
        
    except FileNotFoundError:
        pytest.skip("Skipping performance test due to missing test data file")


def test_large_table_parsing_efficiency():
    """Test efficiency when dealing with tables that might have many records"""
    try:
        start_time = time.time()
        
        with open(test_mysql8_ibd, 'rb') as mysql_file:
            fsp_page = MFspPage.parse_stream(mysql_file)
            
            # Find SDI page
            sdi_page_no = fsp_page.get_sdi_page_no_with_guess(mysql_file)
            if sdi_page_no is not None:
                mysql_file.seek(sdi_page_no * const.PAGE_SIZE)
                sdi_page = MSDIPage.parse_stream(mysql_file)
                
                dd_object_info = sdi_page.ddl(mysql_file, 0)
                table_object = Table(**dd_object_info["dd_object"])
                
                # Just getting the table structure should be fast
                structure_time = time.time() - start_time
                assert structure_time < 2.0, f"Table structure parsing took too long: {structure_time:.3f}s"
                
                # Count columns as a basic measure
                num_columns = len(table_object.columns)
                print(f"Table '{table_object.name}' has {num_columns} columns, parsed in {structure_time:.3f}s")
        
    except FileNotFoundError:
        pytest.skip("Skipping performance test due to missing test data file")


def benchmark_different_parsing_methods():
    """Compare different parsing approaches if available"""
    try:
        with open(test_mysql8_ibd, 'rb') as mysql_file:
            # Method 1: Parse to object
            start1 = time.time()
            mysql_file.seek(0)
            fsp1 = MFspPage.parse_stream(mysql_file)
            time1 = time.time() - start1
            
            # Method 2: Parse from bytes
            mysql_file.seek(0)
            page_bytes = mysql_file.read(const.PAGE_SIZE)
            start2 = time.time()
            fsp2 = MFspPage.parse(page_bytes)
            time2 = time.time() - start2
            
            print(f"Stream parsing: {time1:.4f}s, Bytes parsing: {time2:.4f}s")
            
            # Both methods should complete in reasonable time
            assert time1 < 1.0, f"Stream parsing too slow: {time1:.4f}s"
            assert time2 < 1.0, f"Bytes parsing too slow: {time2:.4f}s"
            
    except FileNotFoundError:
        pytest.skip("Skipping benchmark test due to missing test data file")