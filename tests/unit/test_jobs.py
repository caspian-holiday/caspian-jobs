"""
Unit tests for job execution functionality.
"""

import pytest
from unittest.mock import Mock, patch

from scheduler.jobs import JobExecutor


@pytest.mark.unit
def test_job_executor_initialization():
    """Test JobExecutor initialization."""
    executor = JobExecutor()
    assert executor.database_manager is None
    
    db_manager = Mock()
    executor_with_db = JobExecutor(db_manager)
    assert executor_with_db.database_manager == db_manager


@pytest.mark.unit
@patch('scheduler.jobs.subprocess.run')
@patch('scheduler.jobs.os.path.exists')
def test_execute_python_script_success(mock_exists, mock_subprocess):
    """Test successful Python script execution."""
    mock_exists.return_value = True
    mock_subprocess.return_value = Mock(returncode=0, stdout="Success", stderr="")
    
    executor = JobExecutor()
    script_path = "/path/to/script.py"
    args = ["--arg1", "value1"]
    
    # Should not raise an exception
    executor._execute_python_script(script_path, args)
    
    mock_subprocess.assert_called_once()
    call_args = mock_subprocess.call_args[0][0]
    assert call_args[0].endswith("python")  # sys.executable
    assert call_args[1] == script_path
    assert call_args[2:] == args


@pytest.mark.unit
@patch('scheduler.jobs.subprocess.run')
@patch('scheduler.jobs.os.path.exists')
def test_execute_python_script_failure(mock_exists, mock_subprocess):
    """Test Python script execution failure."""
    mock_exists.return_value = True
    mock_subprocess.return_value = Mock(returncode=1, stdout="", stderr="Error occurred")
    
    executor = JobExecutor()
    script_path = "/path/to/script.py"
    args = ["--arg1", "value1"]
    
    with pytest.raises(RuntimeError, match="Script failed with return code 1"):
        executor._execute_python_script(script_path, args)


@pytest.mark.unit
@patch('scheduler.jobs.subprocess.run')
@patch('scheduler.jobs.os.path.exists')
def test_execute_python_script_timeout(mock_exists, mock_subprocess):
    """Test Python script execution timeout."""
    mock_exists.return_value = True
    from subprocess import TimeoutExpired
    mock_subprocess.side_effect = TimeoutExpired("python", 300)
    
    executor = JobExecutor()
    script_path = "/path/to/script.py"
    args = ["--arg1", "value1"]
    
    with pytest.raises(TimeoutError, match="Script timed out"):
        executor._execute_python_script(script_path, args)


@pytest.mark.unit
@patch('scheduler.jobs.subprocess.run')
@patch('scheduler.jobs.os.path.exists')
def test_execute_python_job_with_job_id(mock_exists, mock_subprocess):
    """Test Python job execution with job_id added to args."""
    mock_exists.return_value = True
    mock_subprocess.return_value = Mock(returncode=0, stdout="Success", stderr="")
    
    executor = JobExecutor()
    job_config = {
        'script': '/path/to/script.py',
        'args': ['--arg1', 'value1'],
        'id': 'test_job'
    }
    
    executor._execute_python_job(job_config)
    
    mock_subprocess.assert_called_once()
    call_args = mock_subprocess.call_args[0][0]
    assert '--job-id' in call_args
    assert 'test_job' in call_args


@pytest.mark.unit
@patch('scheduler.jobs.subprocess.run')
@patch('scheduler.jobs.os.path.exists')
def test_execute_python_job_without_job_id(mock_exists, mock_subprocess):
    """Test Python job execution without job_id in args."""
    mock_exists.return_value = True
    mock_subprocess.return_value = Mock(returncode=0, stdout="Success", stderr="")
    
    executor = JobExecutor()
    job_config = {
        'script': '/path/to/script.py',
        'args': ['--arg1', 'value1', '--job-id', 'existing_job']
    }
    
    executor._execute_python_job(job_config)
    
    mock_subprocess.assert_called_once()
    call_args = mock_subprocess.call_args[0][0]
    # Should not add duplicate --job-id
    assert call_args.count('--job-id') == 1


@pytest.mark.unit
def test_execute_job_missing_script():
    """Test job execution with missing script field."""
    executor = JobExecutor()
    job_config = {
        'id': 'test_job',
        'args': ['--arg1', 'value1']
    }
    
    with pytest.raises(ValueError, match="Job missing 'script' field"):
        executor.execute_job(job_config)


@pytest.mark.unit
@patch('scheduler.jobs.subprocess.run')
@patch('scheduler.jobs.os.path.exists')
def test_execute_job_with_database_manager(mock_exists, mock_subprocess):
    """Test job execution with database manager (advisory locking)."""
    mock_exists.return_value = True
    mock_subprocess.return_value = Mock(returncode=0, stdout="Success", stderr="")
    
    db_manager = Mock()
    db_manager.advisory_lock.return_value.__enter__ = Mock(return_value=True)
    db_manager.advisory_lock.return_value.__exit__ = Mock(return_value=None)
    
    executor = JobExecutor(db_manager)
    job_config = {
        'id': 'test_job',
        'script': '/path/to/script.py',
        'args': ['--arg1', 'value1']
    }
    
    executor.execute_job(job_config)
    
    # Should use advisory lock
    db_manager.advisory_lock.assert_called_once_with('test_job')
    mock_subprocess.assert_called_once()


@pytest.mark.unit
@patch('scheduler.jobs.subprocess.run')
@patch('scheduler.jobs.os.path.exists')
def test_execute_job_lock_not_acquired(mock_exists, mock_subprocess):
    """Test job execution when advisory lock cannot be acquired."""
    mock_exists.return_value = True
    db_manager = Mock()
    db_manager.advisory_lock.return_value.__enter__ = Mock(return_value=False)
    db_manager.advisory_lock.return_value.__exit__ = Mock(return_value=None)
    
    executor = JobExecutor(db_manager)
    job_config = {
        'id': 'test_job',
        'script': '/path/to/script.py',
        'args': ['--arg1', 'value1']
    }
    
    executor.execute_job(job_config)
    
    # Should not execute the script if lock not acquired
    mock_subprocess.assert_not_called()