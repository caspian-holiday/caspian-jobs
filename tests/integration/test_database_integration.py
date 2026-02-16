#!/usr/bin/env python3
"""
Simple integration test to verify DatabaseManager works with extractor usage patterns.
"""

from unittest.mock import Mock, patch
from victoria_metrics_jobs.scheduler.database import DatabaseManager

def test_database_manager_with_extractor_patterns():
    """Test DatabaseManager with extractor usage patterns."""
    
    db_config = {
        'host': 'localhost',
        'port': 5432,
        'name': 'test_db',
        'user': 'test_user',
        'password': 'test_password'
    }
    
    db_manager = DatabaseManager(db_config)
    
    # Mock the engine and connection
    mock_engine = Mock()
    mock_connection = Mock()
    mock_result = Mock()
    mock_result.fetchall.return_value = [('2024-01-15 10:30:00',)]
    
    # Mock engine.connect() to return a context manager
    mock_context = Mock()
    mock_context.__enter__ = Mock(return_value=mock_connection)
    mock_context.__exit__ = Mock(return_value=None)
    mock_engine.connect.return_value = mock_context
    
    mock_connection.execute.return_value = mock_result
    db_manager._engine = mock_engine
    
    print("Testing DatabaseManager with extractor patterns...")
    
    # Test 1: SELECT query with tuple parameters (like extractor does)
    query1 = "SELECT MAX(extracted_at) as last_extraction FROM vm_extracted_metrics WHERE business_date = %s"
    result1 = db_manager.execute_query(query1, ('2024-01-15',))
    print(f"SELECT query result: {result1}")
    print(f"Result access pattern result1[0][0]: {result1[0][0] if result1 and len(result1) > 0 else 'None'}")
    
    # Test 2: INSERT query with tuple parameters
    query2 = "INSERT INTO vm_extraction_jobs (business_date, job_id, status) VALUES (%s, %s, %s)"
    result2 = db_manager.execute_query(query2, ('2024-01-15', 'test_job', 'running'))
    print(f"INSERT query result: {result2}")
    
    # Test 3: SAVEPOINT query (no parameters)
    query3 = "SAVEPOINT data_extraction"
    result3 = db_manager.execute_query(query3)
    print(f"SAVEPOINT query result: {result3}")
    
    # Test 4: UPDATE query with tuple parameters
    query4 = "UPDATE vm_extraction_jobs SET status = %s WHERE job_id = %s"
    result4 = db_manager.execute_query(query4, ('completed', 'test_job'))
    print(f"UPDATE query result: {result4}")
    
    print("All tests passed!")

if __name__ == "__main__":
    test_database_manager_with_extractor_patterns()
