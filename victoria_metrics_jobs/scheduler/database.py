#!/usr/bin/env python3
"""
Database manager for PostgreSQL connections and advisory locks using SQLAlchemy.
"""

import logging
import hashlib
import contextlib
from typing import Dict, Any, Optional
from urllib.parse import quote_plus
from sqlalchemy import create_engine, text, event
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError, DisconnectionError
from sqlalchemy.pool import QueuePool


class DatabaseManager:
    """Manages PostgreSQL connections and advisory locks for job execution using SQLAlchemy."""
    
    def __init__(self, config: Dict[str, Any]):
        """Initialize the database manager.
        
        Args:
            config: Database configuration dictionary
        """
        self.config = config
        self.logger = logging.getLogger(__name__)
        self._engine: Optional[Engine] = None
        self._connection_string = self._build_connection_string()
        self._transaction_connection = None
    
    def _build_connection_string(self) -> str:
        """Build PostgreSQL connection string from config.
        
        Returns:
            PostgreSQL connection string
        """
        # Extract connection parameters
        host = self.config.get('host', 'localhost')
        port = self.config.get('port', 5432)
        dbname = self.config.get('name', 'scheduler')
        user = self.config.get('user', 'scheduler')
        password = self.config.get('password')
        sslmode = self.config.get('ssl_mode', 'prefer')
        connect_timeout = self.config.get('connection_timeout', 10)
        
        # URL-encode password to handle special characters
        if password:
            password = quote_plus(password)
        
        # Build connection string with URL-encoded password
        connection_string = (
            f"postgresql://{user}:{password}@{host}:{port}/{dbname}"
            f"?sslmode={sslmode}&connect_timeout={connect_timeout}"
        )
        
        return connection_string
    
    def connect(self):
        """Establish connection to PostgreSQL database using SQLAlchemy."""
        try:
            # Create engine with connection pooling
            self._engine = create_engine(
                self._connection_string,
                poolclass=QueuePool,
                pool_size=5,
                max_overflow=10,
                pool_pre_ping=True,  # Verify connections before use
                pool_recycle=3600,   # Recycle connections after 1 hour
                echo=False,          # Set to True for SQL debugging
                future=True          # Use SQLAlchemy 2.0 style
            )
            
            # Add connection event listeners
            try:
                @event.listens_for(self._engine, "connect")
                def set_sqlite_pragma(dbapi_connection, connection_record):
                    """Set autocommit mode for advisory locks."""
                    # PostgreSQL doesn't need special pragma settings
                    pass
            except Exception:
                # Skip event listener setup if engine doesn't support it (e.g., in tests)
                pass
            
            # Test the connection
            with self._engine.connect() as conn:
                result = conn.execute(text("SELECT version()"))
                version = result.scalar()
                self.logger.info(f"Connected to PostgreSQL database: {version}")
            
            self.logger.info("SQLAlchemy engine created successfully")
            
        except SQLAlchemyError as e:
            self.logger.error(f"Failed to connect to PostgreSQL database: {e}")
            raise
    
    def disconnect(self):
        """Close the database engine and all connections."""
        if self._engine:
            try:
                self._engine.dispose()
                self.logger.info("Disconnected from PostgreSQL database")
            except SQLAlchemyError as e:
                self.logger.warning(f"Error closing database engine: {e}")
            finally:
                self._engine = None
    
    def is_connected(self) -> bool:
        """Check if database engine is active and can connect."""
        if not self._engine:
            return False
        
        try:
            with self._engine.connect() as conn:
                conn.execute(text("SELECT 1"))
                return True
        except SQLAlchemyError:
            return False
    
    def ensure_connection(self):
        """Ensure database engine is active, reconnect if necessary."""
        if not self.is_connected():
            self.logger.info("Database connection lost, reconnecting...")
            self.connect()
    
    def _generate_lock_id(self, job_id: str) -> int:
        """Generate a consistent integer lock ID from job_id.
        
        PostgreSQL advisory locks require a bigint (64-bit integer).
        We'll use a hash of the job_id to generate a consistent lock ID.
        
        Args:
            job_id: Job identifier string
            
        Returns:
            Integer lock ID for PostgreSQL advisory lock
        """
        # Use SHA-256 hash and take first 8 bytes to create a 64-bit integer
        hash_obj = hashlib.sha256(job_id.encode('utf-8'))
        hash_bytes = hash_obj.digest()[:8]
        
        # Convert to signed 64-bit integer
        lock_id = int.from_bytes(hash_bytes, byteorder='big', signed=True)
        
        # Ensure it's positive (PostgreSQL advisory locks work better with positive numbers)
        if lock_id < 0:
            lock_id = -lock_id
        
        return lock_id
    
    @contextlib.contextmanager
    def advisory_lock(self, job_id: str):
        """Context manager for PostgreSQL advisory lock using SQLAlchemy.
        
        This prevents the same job_id from running simultaneously across
        multiple scheduler instances.
        
        Args:
            job_id: Job identifier to lock
            
        Yields:
            bool: True if lock was acquired, False if already locked
            
        Raises:
            SQLAlchemyError: If database operation fails
        """
        lock_id = self._generate_lock_id(job_id)
        acquired = False
        connection = None
        
        try:
            # Ensure engine exists
            if not self._engine:
                self.connect()
            
            # Get a connection from the pool (pool_pre_ping=True handles connection health)
            connection = self._engine.connect()
            
            # Set autocommit mode for advisory locks
            connection = connection.execution_options(autocommit=True)
            
            # Try to acquire advisory lock
            result = connection.execute(text("SELECT pg_try_advisory_lock(:lock_id)"), {"lock_id": lock_id})
            acquired = result.scalar()
            
            if acquired:
                self.logger.debug(f"Acquired advisory lock for job: {job_id} (lock_id: {lock_id})")
            else:
                self.logger.warning(f"Could not acquire advisory lock for job: {job_id} (lock_id: {lock_id}) - job already running")
            
            yield acquired
            
        except SQLAlchemyError as e:
            self.logger.error(f"Database error during advisory lock operation for job {job_id}: {e}")
            raise
        finally:
            if acquired and connection:
                try:
                    # Release the advisory lock
                    result = connection.execute(text("SELECT pg_advisory_unlock(:lock_id)"), {"lock_id": lock_id})
                    unlocked = result.scalar()
                    
                    if unlocked:
                        self.logger.debug(f"Released advisory lock for job: {job_id} (lock_id: {lock_id})")
                    else:
                        self.logger.warning(f"Failed to release advisory lock for job: {job_id} (lock_id: {lock_id})")
                        
                except SQLAlchemyError as e:
                    self.logger.error(f"Error releasing advisory lock for job {job_id}: {e}")
            
            if connection:
                connection.close()
    
    def test_connection(self) -> bool:
        """Test database connection and advisory lock functionality.
        
        Returns:
            True if connection and advisory locks work correctly
        """
        try:
            # Ensure engine exists
            if not self._engine:
                self.connect()
            
            # Test basic connection
            with self._engine.connect() as conn:
                result = conn.execute(text("SELECT version()"))
                version = result.scalar()
                self.logger.info(f"PostgreSQL version: {version}")
            
            # Test advisory lock functionality
            test_job_id = "test_connection_lock"
            with self.advisory_lock(test_job_id) as acquired:
                if not acquired:
                    self.logger.error("Failed to acquire test advisory lock")
                    return False
                
                self.logger.info("Advisory lock test successful")
                return True
                
        except Exception as e:
            self.logger.error(f"Database connection test failed: {e}")
            return False
    
    def get_engine(self) -> Optional[Engine]:
        """Get the SQLAlchemy engine instance.
        
        Returns:
            SQLAlchemy engine or None if not connected
        """
        return self._engine
    
    def begin_transaction(self):
        """Begin a database transaction."""
        # Ensure engine exists
        if not self._engine:
            self.connect()
        
        # Start a new connection for transaction control
        if self._transaction_connection is None:
            self._transaction_connection = self._engine.connect()
            # Begin transaction (disable autocommit)
            self._transaction_connection = self._transaction_connection.execution_options(autocommit=False)
            self.logger.debug("Started database transaction")
    
    def commit_transaction(self):
        """Commit the current transaction."""
        if self._transaction_connection:
            try:
                self._transaction_connection.commit()
                self.logger.debug("Committed database transaction")
            except SQLAlchemyError as e:
                self.logger.error(f"Failed to commit transaction: {e}")
                raise
            finally:
                self._transaction_connection.close()
                self._transaction_connection = None
    
    def rollback_transaction(self):
        """Rollback the current transaction."""
        if self._transaction_connection:
            try:
                self._transaction_connection.rollback()
                self.logger.debug("Rolled back database transaction")
            except SQLAlchemyError as e:
                self.logger.error(f"Failed to rollback transaction: {e}")
                raise
            finally:
                self._transaction_connection.close()
                self._transaction_connection = None
    
    def execute_query(self, query: str, params: Optional[Dict[str, Any]] = None):
        """Execute a raw SQL query using SQLAlchemy with named parameters.
        
        Args:
            query: SQL query string with :param_name placeholders
            params: Optional dictionary of parameters for the query
            
        Returns:
            Query result - for SELECT queries returns list of tuples, for other queries returns None
        """
        # Ensure engine exists
        if not self._engine:
            self.connect()
        
        # Use transaction connection if available, otherwise create a new connection
        if self._transaction_connection:
            conn = self._transaction_connection
            if params:
                result = conn.execute(text(query), params)
            else:
                result = conn.execute(text(query))
            
            # For SELECT queries, return results as list of tuples
            if query.strip().upper().startswith('SELECT'):
                return result.fetchall()
            return None
        else:
            # Use autocommit connection for standalone queries
            with self._engine.connect() as conn:
                if params:
                    result = conn.execute(text(query), params)
                else:
                    result = conn.execute(text(query))
                
                # For SELECT queries, return results as list of tuples
                if query.strip().upper().startswith('SELECT'):
                    return result.fetchall()
                return None
    
    def execute_batch_insert(self, query: str, params_list: list):
        """Execute a batch insert query using SQLAlchemy with multiple parameter sets.
        
        Args:
            query: SQL INSERT query string with %s placeholders (for psycopg2 style)
            params_list: List of parameter tuples for batch insert
            
        Returns:
            None
        """
        # Ensure engine exists
        if not self._engine:
            self.connect()
        
        # Use transaction connection if available, otherwise create a new connection
        if self._transaction_connection:
            conn = self._transaction_connection
            # Use SQLAlchemy connection for batch operations to maintain transaction consistency
            for params in params_list:
                conn.execute(text(query), params)
        else:
            # Use autocommit connection for standalone batch operations
            with self._engine.connect() as conn:
                for params in params_list:
                    conn.execute(text(query), params)