"""
Integration tests for job execution.
"""

import os
import tempfile
import yaml
from datetime import datetime
from pathlib import Path
from typing import List

import pytest

from jobs.common import BaseJob, BaseJobState, Result, Ok, Err


class TestJobState(BaseJobState):
    """Test state for integration testing."""
    test_data: str = "test"


class TestJob(BaseJob):
    """Test job implementation for integration testing."""
    
    def create_initial_state(self, job_id: str) -> Result[TestJobState, Exception]:
        """Create initial state for test job."""
        try:
            job_config = self.get_job_config(job_id)
            initial_state = TestJobState(
                job_id=job_id,
                job_config=job_config,
                started_at=datetime.now(),
                test_data="test"
            )
            return Ok(initial_state)
        except Exception as e:
            return Err(e)
    
    def get_workflow_steps(self) -> List[callable]:
        """Get workflow steps for test job."""
        return [self._test_step]
    
    def finalize_state(self, state: TestJobState) -> TestJobState:
        """Finalize test state."""
        state.completed_at = datetime.now()
        state.status = 'success'
        state.message = f'Test job {state.job_id} completed'
        return state
    
    def _test_step(self, state: TestJobState) -> Result[TestJobState, Exception]:
        """Simple test step."""
        return Ok(state)


@pytest.mark.integration
def test_job_execution_with_config(temp_job_config_file, mock_environment):
    """Test complete job execution with configuration."""
    job = TestJob('test_job', temp_job_config_file, verbose=True)
    
    # Test listing jobs
    jobs = job.list_jobs()
    assert 'test_job' in jobs
    
    # Test getting job info
    jobs_info = job.get_available_jobs_info()
    assert 'test_job' in jobs_info
    
    # Test running job
    result = job.run('test_job')
    
    assert result['status'] == 'success'
    assert result['job_id'] == 'test_job'
    assert 'execution_time_seconds' in result
    assert result['execution_time_seconds'] > 0


@pytest.mark.integration
def test_job_execution_error_handling():
    """Test job execution error handling."""
    # Create a job with invalid config
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yml', delete=False) as f:
        yaml.dump({'jobs': {}}, f)
        temp_file = f.name
    
    try:
        job = TestJob('test_job', temp_file, verbose=True)
        
        # This should return an error result because job doesn't exist
        result = job.run('nonexistent')
        assert result['status'] == 'error'
        assert 'Job \'nonexistent\' not found' in result['message']
    finally:
        os.unlink(temp_file)


@pytest.mark.integration
def test_job_config_loading():
    """Test job configuration loading from different sources."""
    # Test with environment variables
    os.environ['TEST_VAR'] = 'test_value'
    
    config_data = {
        'common': {
            'test_url': '${TEST_VAR}'
        },
        'jobs': {
            'test_job': {
                'param1': 'value1',
                'test_url': '${TEST_VAR}'  # Add to job config too
            }
        }
    }
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yml', delete=False) as f:
        yaml.dump(config_data, f)
        temp_file = f.name
    
    try:
        job = TestJob('test_job', temp_file, verbose=True)
        job_config = job.get_job_config('test_job')
        
        # Environment variable should be resolved
        assert job_config['test_url'] == 'test_value'
        assert job_config['param1'] == 'value1'
    finally:
        os.unlink(temp_file)
        os.environ.pop('TEST_VAR', None)
