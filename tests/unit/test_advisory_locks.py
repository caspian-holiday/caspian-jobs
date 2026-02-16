"""
Unit tests for PostgreSQL advisory locking functionality using SQLAlchemy.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
from sqlalchemy.exc import SQLAlchemyError

from victoria_metrics_jobs.scheduler.database import DatabaseManager


@pytest.mark.unit
def test_database_manager_initialization():
    """Test DatabaseManager initialization."""
    db_config = {
        'host': 'localhost',
        'port': 5432,
        'name': 'test_db',
        'user': 'test_user',
        'password': 'test_password'
    }
    
    db_manager = DatabaseManager(db_config)
    assert db_manager.config == db_config
    assert db_manager._engine is None
    assert db_manager._transaction_connection is None


@pytest.mark.unit
def test_lock_id_generation():
    """Test that lock ID generation is consistent."""
    db_config = {
        'host': 'localhost',
        'port': 5432,
        'name': 'test',
        'user': 'test',
        'password': 'test'
    }
    
    db_manager = DatabaseManager(db_config)
    
    # Test that the same job_id generates the same lock_id
    job_id = "test_job"
    lock_id_1 = db_manager._generate_lock_id(job_id)
    lock_id_2 = db_manager._generate_lock_id(job_id)
    
    assert lock_id_1 == lock_id_2, "Lock ID generation should be consistent"
    
    # Test that different job_ids generate different lock_ids
    different_job_id = "different_job"
    different_lock_id = db_manager._generate_lock_id(different_job_id)
    
    assert lock_id_1 != different_lock_id, "Different job IDs should generate different lock IDs"
    
    # Test that lock_id is positive
    assert lock_id_1 > 0, "Lock ID should be positive"
    assert different_lock_id > 0, "Lock ID should be positive"


@pytest.mark.unit
def test_lock_id_hash_consistency():
    """Test that lock ID generation produces consistent hashes."""
    db_config = {
        'host': 'localhost',
        'port': 5432,
        'name': 'test',
        'user': 'test',
        'password': 'test'
    }
    
    db_manager = DatabaseManager(db_config)
    
    # Test with various job IDs
    test_job_ids = [
        "simple_job",
        "job_with_underscores",
        "job-with-dashes",
        "job.with.dots",
        "job123",
        "very_long_job_name_with_many_characters",
        "job with spaces",
        "Job_With_Mixed_Case"
    ]
    
    for job_id in test_job_ids:
        lock_id_1 = db_manager._generate_lock_id(job_id)
        lock_id_2 = db_manager._generate_lock_id(job_id)
        assert lock_id_1 == lock_id_2, f"Lock ID should be consistent for job '{job_id}'"
        assert lock_id_1 > 0, f"Lock ID should be positive for job '{job_id}'"


@pytest.mark.unit
def test_connection_string_building():
    """Test connection string building with various configurations."""
    # Test basic configuration
    db_config = {
        'host': 'localhost',
        'port': 5432,
        'name': 'test_db',
        'user': 'test_user',
        'password': 'test_password'
    }
    
    db_manager = DatabaseManager(db_config)
    conn_str = db_manager._connection_string
    
    assert 'postgresql://test_user:test_password@localhost:5432/test_db' in conn_str
    assert 'sslmode=prefer' in conn_str
    assert 'connect_timeout=10' in conn_str
    
    # Test with special characters in password
    db_config['password'] = 'pass@word#123'
    db_manager = DatabaseManager(db_config)
    conn_str = db_manager._connection_string
    
    # Password should be URL-encoded
    assert 'pass%40word%23123' in conn_str


@pytest.mark.unit
@patch('victoria_metrics_jobs.scheduler.database.create_engine')
def test_connect_method(mock_create_engine):
    """Test the connect method without triggering event listeners."""
    # Create a mock engine that doesn't support events
    mock_engine = Mock()
    mock_connection = Mock()
    mock_engine.connect.return_value.__enter__ = Mock(return_value=mock_connection)
    mock_engine.connect.return_value.__exit__ = Mock(return_value=None)
    mock_result = Mock()
    mock_result.scalar.return_value = "PostgreSQL 14.0"
    mock_connection.execute.return_value = mock_result
    mock_create_engine.return_value = mock_engine
    
    db_config = {
        'host': 'localhost',
        'port': 5432,
        'name': 'test_db',
        'user': 'test_user',
        'password': 'test_password'
    }
    
    db_manager = DatabaseManager(db_config)
    db_manager.connect()
    
    assert db_manager._engine == mock_engine
    mock_create_engine.assert_called_once()


@pytest.mark.unit
@patch('victoria_metrics_jobs.scheduler.database.create_engine')
def test_connect_failure(mock_create_engine):
    """Test connection failure handling."""
    mock_create_engine.side_effect = SQLAlchemyError("Connection failed")
    
    db_config = {
        'host': 'localhost',
        'port': 5432,
        'name': 'test_db',
        'user': 'test_user',
        'password': 'test_password'
    }
    
    db_manager = DatabaseManager(db_config)
    
    with pytest.raises(SQLAlchemyError, match="Connection failed"):
        db_manager.connect()


@pytest.mark.unit
@patch('victoria_metrics_jobs.scheduler.database.create_engine')
def test_is_connected(mock_create_engine):
    """Test is_connected method."""
    mock_engine = Mock()
    mock_connection = Mock()
    mock_result = Mock()
    mock_result.scalar.return_value = "PostgreSQL 14.0"
    
    mock_engine.connect.return_value.__enter__ = Mock(return_value=mock_connection)
    mock_engine.connect.return_value.__exit__ = Mock(return_value=None)
    mock_connection.execute.return_value = mock_result
    mock_create_engine.return_value = mock_engine
    
    db_config = {
        'host': 'localhost',
        'port': 5432,
        'name': 'test_db',
        'user': 'test_user',
        'password': 'test_password'
    }
    
    db_manager = DatabaseManager(db_config)
    
    # Test when not connected
    assert db_manager.is_connected() is False
    
    # Test when connected
    db_manager.connect()
    assert db_manager.is_connected() is True


@pytest.mark.unit
def test_ensure_connection():
    """Test ensure_connection method."""
    db_config = {
        'host': 'localhost',
        'port': 5432,
        'name': 'test_db',
        'user': 'test_user',
        'password': 'test_password'
    }
    
    db_manager = DatabaseManager(db_config)
    
    # Mock the connect method
    with patch.object(db_manager, 'connect') as mock_connect:
        with patch.object(db_manager, 'is_connected', return_value=False):
            db_manager.ensure_connection()
            mock_connect.assert_called_once()


@pytest.mark.unit
def test_disconnect():
    """Test disconnect method."""
    db_config = {
        'host': 'localhost',
        'port': 5432,
        'name': 'test_db',
        'user': 'test_user',
        'password': 'test_password'
    }
    
    db_manager = DatabaseManager(db_config)
    mock_engine = Mock()
    db_manager._engine = mock_engine
    
    db_manager.disconnect()
    
    mock_engine.dispose.assert_called_once()
    assert db_manager._engine is None


@pytest.mark.unit
@patch('victoria_metrics_jobs.scheduler.database.create_engine')
def test_advisory_lock_success(mock_create_engine):
    """Test successful advisory lock acquisition."""
    mock_engine = Mock()
    mock_connection = Mock()
    mock_result = Mock()
    mock_result.scalar.return_value = True  # Lock acquired
    
    mock_engine.connect.return_value = mock_connection
    mock_connection.execution_options.return_value = mock_connection
    mock_connection.execute.return_value = mock_result
    mock_create_engine.return_value = mock_engine
    
    db_config = {
        'host': 'localhost',
        'port': 5432,
        'name': 'test_db',
        'user': 'test_user',
        'password': 'test_password'
    }
    
    db_manager = DatabaseManager(db_config)
    db_manager._engine = mock_engine  # Skip connect() to avoid event listener issues
    
    # Test successful lock acquisition
    with db_manager.advisory_lock("test_job") as lock_acquired:
        assert lock_acquired is True
        mock_connection.execute.assert_called()
    
    # Test lock release
    assert mock_connection.close.called


@pytest.mark.unit
@patch('victoria_metrics_jobs.scheduler.database.create_engine')
def test_advisory_lock_failure(mock_create_engine):
    """Test advisory lock when already locked."""
    mock_engine = Mock()
    mock_connection = Mock()
    mock_result = Mock()
    mock_result.scalar.return_value = False  # Lock not acquired
    
    mock_engine.connect.return_value = mock_connection
    mock_connection.execution_options.return_value = mock_connection
    mock_connection.execute.return_value = mock_result
    mock_create_engine.return_value = mock_engine
    
    db_config = {
        'host': 'localhost',
        'port': 5432,
        'name': 'test_db',
        'user': 'test_user',
        'password': 'test_password'
    }
    
    db_manager = DatabaseManager(db_config)
    db_manager._engine = mock_engine  # Skip connect() to avoid event listener issues
    
    # Test when lock cannot be acquired
    with db_manager.advisory_lock("test_job") as lock_acquired:
        assert lock_acquired is False
        mock_connection.execute.assert_called()
    
    # Connection should still be closed
    assert mock_connection.close.called


@pytest.mark.unit
def test_transaction_methods():
    """Test transaction management methods."""
    db_config = {
        'host': 'localhost',
        'port': 5432,
        'name': 'test_db',
        'user': 'test_user',
        'password': 'test_password'
    }
    
    db_manager = DatabaseManager(db_config)
    mock_engine = Mock()
    mock_connection = Mock()
    mock_engine.connect.return_value = mock_connection
    mock_connection.execution_options.return_value = mock_connection
    db_manager._engine = mock_engine
    
    # Test begin_transaction
    db_manager.begin_transaction()
    assert db_manager._transaction_connection == mock_connection
    mock_connection.execution_options.assert_called_with(autocommit=False)
    
    # Test commit_transaction
    db_manager.commit_transaction()
    mock_connection.commit.assert_called_once()
    mock_connection.close.assert_called_once()
    assert db_manager._transaction_connection is None


@pytest.mark.unit
def test_rollback_transaction():
    """Test rollback transaction method."""
    db_config = {
        'host': 'localhost',
        'port': 5432,
        'name': 'test_db',
        'user': 'test_user',
        'password': 'test_password'
    }
    
    db_manager = DatabaseManager(db_config)
    mock_engine = Mock()
    mock_connection = Mock()
    mock_engine.connect.return_value = mock_connection
    mock_connection.execution_options.return_value = mock_connection
    db_manager._engine = mock_engine
    
    # Test begin_transaction
    db_manager.begin_transaction()
    assert db_manager._transaction_connection == mock_connection
    
    # Test rollback_transaction
    db_manager.rollback_transaction()
    mock_connection.rollback.assert_called_once()
    mock_connection.close.assert_called_once()
    assert db_manager._transaction_connection is None


@pytest.mark.unit
def test_execute_query_parameter_conversion():
    """Test execute_query with named parameters."""
    db_config = {
        'host': 'localhost',
        'port': 5432,
        'name': 'test_db',
        'user': 'test_user',
        'password': 'test_password'
    }
    
    db_manager = DatabaseManager(db_config)
    mock_engine = Mock()
    mock_connection = Mock()
    mock_result = Mock()
    mock_result.fetchall.return_value = [('test_value',)]
    
    # Mock engine.connect() to return a context manager
    mock_context = Mock()
    mock_context.__enter__ = Mock(return_value=mock_connection)
    mock_context.__exit__ = Mock(return_value=None)
    mock_engine.connect.return_value = mock_context
    
    mock_connection.execute.return_value = mock_result
    db_manager._engine = mock_engine
    
    # Test SELECT query with named parameters
    result = db_manager.execute_query("SELECT * FROM test WHERE col = :col_value", {'col_value': 'test_value'})
    assert result == [('test_value',)]


@pytest.mark.unit
def test_execute_query_with_transaction():
    """Test execute_query method with active transaction."""
    db_config = {
        'host': 'localhost',
        'port': 5432,
        'name': 'test_db',
        'user': 'test_user',
        'password': 'test_password'
    }
    
    db_manager = DatabaseManager(db_config)
    mock_engine = Mock()
    mock_connection = Mock()
    mock_result = Mock()
    mock_result.fetchall.return_value = [('test_value',)]
    
    mock_engine.connect.return_value = mock_connection
    mock_connection.execution_options.return_value = mock_connection
    mock_connection.execute.return_value = mock_result
    db_manager._engine = mock_engine
    
    # Start transaction
    db_manager.begin_transaction()
    
    # Test SELECT query
    result = db_manager.execute_query("SELECT * FROM test WHERE col = :col_value", {'col_value': 'test_value'})
    assert result == [('test_value',)]
    mock_connection.execute.assert_called()
    
    # Test INSERT query (should return None)
    result = db_manager.execute_query("INSERT INTO test VALUES (:value)", {'value': 'new_value'})
    assert result is None
    mock_connection.execute.assert_called()


@pytest.mark.unit
def test_execute_query_without_transaction():
    """Test execute_query method without active transaction."""
    db_config = {
        'host': 'localhost',
        'port': 5432,
        'name': 'test_db',
        'user': 'test_user',
        'password': 'test_password'
    }
    
    db_manager = DatabaseManager(db_config)
    mock_engine = Mock()
    mock_connection = Mock()
    mock_result = Mock()
    mock_result.fetchall.return_value = [('test_value',)]
    
    mock_engine.connect.return_value.__enter__ = Mock(return_value=mock_connection)
    mock_engine.connect.return_value.__exit__ = Mock(return_value=None)
    mock_connection.execute.return_value = mock_result
    db_manager._engine = mock_engine
    
    # Test SELECT query without transaction
    result = db_manager.execute_query("SELECT * FROM test WHERE col = :col_value", {'col_value': 'test_value'})
    assert result == [('test_value',)]
    mock_connection.execute.assert_called()