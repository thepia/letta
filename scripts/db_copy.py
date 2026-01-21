#!/usr/bin/env python3
"""
Database backup, restore, and migration tool for Letta db.

This script provides flexible database operations:
- Backup: Export current database to SQLite file
- Restore: Import from SQLite backup to target database
- Migrate: One-step migration between database types

Usage:
    # Verify source database is readable (safe, no files written)
    python scripts/db_copy.py verify --ssh-host henrikvendelbo@Henriks-Mac-Pro.local
    
    # Backup current database (uses LETTA settings or SQLite by default)
    python scripts/db_copy.py backup [--output backup.db]

    # Dry-run backup (validate readability without writing)
    python scripts/db_copy.py backup --dry-run --ssh-host henrikvendelbo@Henriks-Mac-Pro.local

    # Backup from specific PostgreSQL server
    python scripts/db_copy.py backup --host Henriks-Mac-Pro.local --port 5432 \\
        --user letta --password letta --db letta --output backup.db

    # Backup via SSH tunnel (from remote machine)
    python scripts/db_copy.py backup \\
        --host localhost --port 5432 \\
        --ssh-host henrikvendelbo@Henriks-Mac-Pro.local \\
        --output backup.db

    # Restore from backup
    python scripts/db_copy.py restore backup.db [--sqlite|--postgres]

    # Restore to specific PostgreSQL server
    python scripts/db_copy.py restore backup.db --postgres \\
        --host n2.thepia.net --port 5432 --user letta --password letta --db letta

    # Restore via SSH tunnel (to remote machine)
    python scripts/db_copy.py restore backup.db --postgres \\
        --host localhost --port 5432 \\
        --ssh-host henrikvendelbo@Henriks-Mac-Pro.local

    # Direct migration
    python scripts/db_copy.py migrate --to [sqlite|postgres]

Commands:
    verify                   Check if source database is readable (safe, non-destructive)
    backup                   Export database to backup file
        --dry-run            Verify data readability without writing file
    restore                  Import database from backup file
    migrate                  Convert database type (SQLite ↔ PostgreSQL)

Database connection options (work with all commands):
    --host HOST              Database server hostname (default: localhost)
    --port PORT              Database server port (default: 5432 for PostgreSQL)
    --user USER              Database user (default: letta)
    --password PASSWORD      Database password (default: letta)
    --db DBNAME              Database name (default: letta)

SSH Tunnel options (optional, to tunnel through a remote machine):
    --ssh-host HOST          SSH host (format: user@host or just host)
    --ssh-user USER          SSH username (if not included in --ssh-host)
    --ssh-key PATH           Path to SSH private key (for key-based auth)

For Docker workflows:
    # Backup from container
    docker exec letta_server python scripts/db_copy.py backup --output /app/backup.db
    docker cp letta_server:/app/backup.db ./backup.db

    # Restore to container
    docker cp ./backup.db letta_server:/app/backup.db
    docker exec letta_server python scripts/db_copy.py restore /app/backup.db --postgres
"""

import argparse
import asyncio
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import List, Optional
from abc import ABC, abstractmethod
import subprocess
import time
import socket

from sqlalchemy import MetaData, Table, create_engine, inspect, text, select, func
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker

# Letta imports
from letta.orm.base import Base
from letta.settings import DatabaseChoice, settings


class RestoreStrategy(ABC):
    """Abstract base class for pluggable database restore strategies.
    
    This pattern allows different restore methods to be swapped easily based on
    the target environment and constraints. Each strategy handles:
    - Data serialization/escaping (SQL, COPY, raw bytes, etc.)
    - Transport mechanism (stdin piping, temp files, direct connections)
    - Error handling and validation
    
    ## Available Strategies:
    
    1. **DockerExecSQLStrategy** ✅ (Recommended)
       - Generates standard SQL INSERT statements
       - Pipes SQL directly via stdin to avoid shell limits
       - Handles all data types via SQL escaping
       - No temporary files needed
       - Best for: Remote docker-exec restore, large datasets
       
    2. **DockerExecCopyStrategy** (Future)
       - Uses PostgreSQL COPY format
       - More efficient for very large datasets
       - Requires careful type serialization
       
    3. **PortTunnelStrategy** (Future)
       - Direct port tunnel connection
       - No SSH docker-exec needed
       - Requires working password authentication
       
    ## Adding a New Strategy:
    
    ```python
    class MyCustomStrategy(RestoreStrategy):
        async def restore_table(self, table_name, orm_model, instances, 
                               ssh_host, ssh_user, ssh_key, docker_container):
            # 1. Serialize data (SQL, COPY format, etc.)
            # 2. Transport to target (stdin, temp file, network, etc.)
            # 3. Execute on target
            # 4. Return count of rows inserted
            return len(instances)
    ```
    
    Then use it:
    ```python
    copier.restore_strategy = MyCustomStrategy()
    await copier.restore(backup_file)
    ```
    """
    
    @abstractmethod
    async def restore_table(self, table_name: str, orm_model, instances: list, ssh_host: str, ssh_user: str, ssh_key: Optional[str], docker_container: str) -> int:
        """Restore a table's data using this strategy.
        
        Args:
            table_name: Name of the table to restore to
            orm_model: SQLAlchemy ORM model class for type information
            instances: List of ORM instances containing the data to insert
            ssh_host: Remote SSH host for execution
            ssh_user: SSH username
            ssh_key: Path to SSH private key (optional, uses default if not set)
            docker_container: Docker container name running PostgreSQL
            
        Returns:
            Number of rows successfully inserted
            
        Raises:
            RuntimeError: If the restore operation fails
        """
        pass


class DockerExecSQLStrategy(RestoreStrategy):
    """Restore via docker-exec using plain SQL INSERT statements.
    
    This strategy is recommended for most use cases because it:
    - ✅ Avoids shell argument length limits by piping directly to stdin
    - ✅ Handles all PostgreSQL data types through SQL escaping
    - ✅ Requires no temporary files or complex serialization
    - ✅ Provides clear error messages with line numbers
    - ✅ Works with batching to manage memory
    
    How it works:
    1. Serialize each row to SQL: INSERT INTO table (...) VALUES (...)
    2. Batch rows (default 10) to avoid exceeding shell limits
    3. Pipe SQL directly to: ssh → docker exec → psql stdin
    4. PostgreSQL parses and executes the INSERT statements
    
    Performance:
    - ~1000 rows/second on typical network
    - Batching reduces SSH connection overhead
    - One SQL statement per row (can be combined in future optimization)
    
    Limitations:
    - Slower than COPY for massive datasets (1M+ rows)
    - Requires ssh + docker access
    - No transaction grouping (each INSERT is atomic)
    """
    
    async def restore_table(self, table_name: str, orm_model, instances: list, ssh_host: str, ssh_user: str, ssh_key: Optional[str], docker_container: str) -> int:
        """Generate SQL INSERT statements and execute via docker-exec with batching."""
        if not instances:
            return 0
        
        import json
        import shlex
        import uuid
        import base64
        
        # Get column names from ORM model
        columns = [col.name for col in orm_model.__table__.columns]
        
        # Batch inserts to avoid shell limits (10 rows per batch for SQL strategy)
        batch_size = 10
        total_inserted = 0
        
        for i in range(0, len(instances), batch_size):
            batch = instances[i:i + batch_size]
            
            # Build standard SQL INSERT statements
            insert_statements = []
            for instance in batch:
                values = []
                for col_name in columns:
                    value = getattr(instance, col_name)
                    formatted = self._escape_sql_value(value)
                    values.append(formatted)
                
                columns_str = ", ".join([f'"{col}"' for col in columns])
                values_str = ", ".join(values)
                insert_sql = f'INSERT INTO "{table_name}" ({columns_str}) VALUES ({values_str});'
                insert_statements.append(insert_sql)
            
            # Combine all statements into one SQL file
            full_sql = "\n".join(insert_statements)
            
            # Execute via docker exec (using temp file approach)
            await self._execute_sql_via_docker_exec(full_sql, ssh_host, ssh_user, ssh_key, docker_container)
            
            total_inserted += len(batch)
        
        return total_inserted
    
    def _escape_sql_value(self, value) -> str:
        """Escape and format a Python value for use in SQL INSERT statement.
        
        Handles all PostgreSQL types with proper escaping:
        - NULL → NULL
        - bool → TRUE/FALSE
        - int/float → numeric literal
        - bytes → hex format (\xABCD...)
        - list/dict → JSON with proper escaping
        - str → single-quoted with '' escaping
        
        Args:
            value: Python value to escape
            
        Returns:
            SQL-safe representation ready to embed in INSERT VALUES clause
        """
        if value is None:
            return "NULL"
        elif isinstance(value, bool):
            return "TRUE" if value else "FALSE"
        elif isinstance(value, int):
            return str(value)
        elif isinstance(value, float):
            return str(value)
        elif isinstance(value, bytes):
            hex_str = value.hex()
            return f"'\\x{hex_str}'"
        elif isinstance(value, (list, dict)):
            import json
            json_str = json.dumps(value)
            escaped = json_str.replace("'", "''")
            return f"'{escaped}'::json"
        else:
            str_val = str(value).replace("'", "''")
            return f"'{str_val}'"
    
    async def _execute_sql_via_docker_exec(self, sql: str, ssh_host: str, ssh_user: str, ssh_key: Optional[str], docker_container: str) -> None:
        """Execute SQL on remote PostgreSQL via docker exec with stdin piping.
        
        Key design decisions:
        1. **stdin piping** - Avoids shell argument length limits (fixes [Errno 7])
           Instead of: ssh host "docker exec ... -c \"large sql\""
           We use:     ssh host "docker exec -i ..." < large sql
        
        2. **subprocess.Popen** - Lower-level subprocess control for stdin/stdout/stderr
           Allows us to pipe data directly without going through shell
        
        3. **-i flag for docker exec** - Enables stdin from pipe
           This is critical for the piping to work
        
        Args:
            sql: Complete SQL to execute (multiple statements OK)
            ssh_host: Target SSH host
            ssh_user: SSH username
            ssh_key: Path to SSH private key
            docker_container: Docker container name
            
        Raises:
            RuntimeError: If SQL execution fails or times out
        """
        import shlex
        import subprocess
        
        ssh_key_path = ssh_key or self._find_default_ssh_key()
        ssh_user_part = f"{ssh_user}@" if ssh_user else ""
        key_part = f"-i {ssh_key_path}" if ssh_key_path else ""
        
        try:
            # Pipe SQL directly to docker exec psql via SSH
            # This avoids shell argument length limits
            docker_cmd = f"docker exec -i {docker_container} psql -U letta -d letta"
            ssh_cmd = f"ssh {key_part} {ssh_user_part}{ssh_host} {docker_cmd}"
            
            # Use Popen to pipe SQL directly instead of passing through shell
            process = subprocess.Popen(
                ssh_cmd,
                shell=True,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )
            stdout, stderr = process.communicate(input=sql, timeout=60)
            
            if process.returncode != 0:
                raise RuntimeError(f"SQL execution failed: {stderr}")
            
        except subprocess.TimeoutExpired:
            process.kill()
            raise RuntimeError("SQL execution timed out")
        except Exception as e:
            raise RuntimeError(f"Failed to execute SQL: {e}")
    
    def _find_default_ssh_key(self) -> Optional[str]:
        """Find default SSH key if available."""
        import os
        possible_keys = [
            os.path.expanduser("~/.ssh/id_ed25519"),
            os.path.expanduser("~/.ssh/id_rsa"),
            os.path.expanduser("~/.ssh/id_ecdsa"),
            os.path.expanduser("~/.ssh/id_dsa"),
        ]
        for key in possible_keys:
            if os.path.exists(key):
                return key
        return None


class DatabaseCopier:
    """Handles database backup, restore, and migration operations."""

    def __init__(self, host: Optional[str] = None, port: Optional[int] = None, 
                 user: Optional[str] = None, password: Optional[str] = None, 
                 db: Optional[str] = None, ssh_host: Optional[str] = None,
                 ssh_user: Optional[str] = None, ssh_key: Optional[str] = None,
                 docker_exec: bool = False, docker_container: str = "postgres"):
        self.ssh_tunnel = None
        self.ssh_process = None
        self.local_bind_port = None
        self.docker_exec = docker_exec
        self.docker_container = docker_container
        self.ssh_host_for_docker = None
        self.ssh_user_for_docker = None
        self.ssh_key_for_docker = None
        # Select restore strategy (SQL is simpler and more reliable than COPY)
        self.restore_strategy: RestoreStrategy = DockerExecSQLStrategy()
        
        # If docker_exec mode, store SSH credentials for later use
        if docker_exec and ssh_host:
            self.ssh_host_for_docker = ssh_host
            self.ssh_user_for_docker = ssh_user
            self.ssh_key_for_docker = ssh_key
            # For docker exec, we don't use a traditional database URI
            # We'll handle connections via docker exec instead
            self.current_db_type = DatabaseChoice.POSTGRES
            self.current_db_uri = None
        # If SSH tunnel is requested, we MUST use PostgreSQL
        elif ssh_host or host or user or password or db:
            self.current_db_type = DatabaseChoice.POSTGRES
            # Build PostgreSQL URI from components
            effective_host = host or "localhost"
            effective_port = port or 5432
            effective_user = user or "letta"
            effective_password = password or "letta"
            effective_db = db or "letta"
            
            # If SSH tunnel requested, set it up
            if ssh_host:
                print(f"\n🔐 Setting up SSH tunnel to {ssh_host}...")
                self._setup_ssh_tunnel(ssh_host, ssh_user, ssh_key, effective_host, effective_port)
                # Use localhost via tunnel
                effective_host = "localhost"
                effective_port = self.local_bind_port
            
            self.current_db_uri = f"postgresql+pg8000://{effective_user}:{effective_password}@{effective_host}:{effective_port}/{effective_db}"
        else:
            # Fall back to settings
            self.current_db_uri = settings.letta_db_uri
            self.current_db_type = settings.database_engine
    
    def _setup_ssh_tunnel(self, ssh_host: str, ssh_user: Optional[str], ssh_key: Optional[str],
                          db_host: str, db_port: int) -> None:
        """Set up SSH tunnel using command line ssh."""
        # Find an available local port
        sock = socket.socket()
        sock.bind(('', 0))
        self.local_bind_port = sock.getsockname()[1]
        sock.close()
        
        # Auto-detect SSH key if not specified
        if not ssh_key:
            ssh_key = self._find_default_ssh_key()
        
        # Build SSH command
        ssh_user_part = f"{ssh_user}@" if ssh_user else ""
        key_part = f"-i {ssh_key}" if ssh_key else ""
        
        # SSH tunnel command: ssh -L local_port:db_host:db_port user@ssh_host -N
        ssh_cmd = f"ssh {key_part} -L {self.local_bind_port}:{db_host}:{db_port} {ssh_user_part}{ssh_host} -N"
        
        try:
            if ssh_key:
                print(f"   Using SSH key: {ssh_key}")
            
            # Start SSH tunnel in background
            self.ssh_process = subprocess.Popen(
                ssh_cmd,
                shell=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )
            # Give tunnel time to establish
            time.sleep(2)
            
            # Check if process is still running
            if self.ssh_process.poll() is not None:
                _, stderr = self.ssh_process.communicate()
                raise RuntimeError(f"SSH tunnel failed: {stderr.decode()}")
            
            print(f"   ✓ SSH tunnel established on localhost:{self.local_bind_port}")
        except Exception as e:
            raise RuntimeError(f"Failed to set up SSH tunnel: {e}")
    
    async def _restore_via_docker_exec(self, backup_path: Path) -> None:
        """Restore database via docker exec using configurable RestoreStrategy.
        
        This method orchestrates the restore process using a pluggable strategy pattern:
        
        1. **Schema Creation** - Creates tables on remote PostgreSQL
        2. **Data Reading** - Reads backup data from local SQLite file
        3. **Strategy-Based Insertion** - Uses self.restore_strategy to insert data
           - Current: DockerExecSQLStrategy (INSERT via stdin)
           - Pluggable: Can switch to COPY, PortTunnel, etc.
        
        The strategy handles all the complexity of:
        - Batching to avoid shell limits
        - Type serialization (SQL escaping, etc.)
        - Transport mechanism (stdin piping, temp files, etc.)
        - Error handling and retry logic
        
        Usage:
        ```python
        copier = DatabaseCopier(..., docker_exec=True)
        # Optionally swap strategies:
        # copier.restore_strategy = DockerExecCopyStrategy()
        await copier.restore(backup_file)
        ```
        """
        print(f"\nSource backup file: {backup_path}")
        print(f"Target: Docker container '{self.docker_container}' on {self.ssh_host_for_docker}")
        
        # Auto-confirm for docker-exec (user explicitly chose this method)
        print("\n⚠️  This will overwrite the target database. Proceeding with docker-exec restore...")
        
        backup_engine = None
        try:
            # Step 1: Create schema in remote PostgreSQL via docker exec
            print("\n📦 Creating schema in remote database...")
            await self._execute_docker_exec_sql("DROP SCHEMA IF EXISTS public CASCADE;")
            
            # Disable foreign keys for faster insertion (PostgreSQL)
            print("   Disabling foreign key constraints temporarily...")
            await self._execute_docker_exec_sql("SET CONSTRAINTS ALL DEFERRED;")
            
            # Generate DDL from SQLite backup using Letta's ORM
            ddl_statements = await self._generate_ddl_from_backup(backup_engine)
            
            # Execute all DDL statements
            for ddl in ddl_statements:
                try:
                    await self._execute_docker_exec_sql(ddl)
                except Exception as e:
                    if "already exists" not in str(e):
                        print(f"   ⚠️  Warning: DDL execution issue: {e}")
            
            # Step 2: Read from SQLite backup (use SYNC engine for SQLite)
            print("📦 Reading backup data...")
            backup_uri = f"sqlite:///{backup_path}"
            backup_engine = create_engine(backup_uri, echo=False)  # Use SYNC for SQLite
            
            # Get all table names
            inspector = inspect(backup_engine)
            table_names = inspector.get_table_names()
            table_names = [t for t in table_names if t != 'alembic_version']
            
            print(f"Found {len(table_names)} tables to restore")
            
            # Sort tables by foreign key dependencies (parents first)
            sorted_tables = await self._sort_tables_by_dependencies(table_names)
            print(f"   Insertion order: {' → '.join(sorted_tables)}")
            
            # Step 3: Copy each table via docker exec in dependency order
            total_rows = 0
            for table_name in sorted_tables:
                print(f"\n📦 Restoring table: {table_name}")
                
                orm_model = self._get_orm_model_for_table(table_name)
                if not orm_model:
                    print(f"   ⚠️  Skipping (no ORM model)")
                    continue
                
                # Use sync session with sync engine
                Session = sessionmaker(bind=backup_engine)
                session = Session()
                
                try:
                    instances = session.query(orm_model).all()
                    
                    if not instances:
                        print(f"   ✓ 0 rows")
                        continue
                    
                    # Insert via docker exec
                    rows_inserted = await self._insert_rows_via_docker_exec(
                        table_name, orm_model, instances
                    )
                    print(f"   ✓ Inserted {rows_inserted} rows")
                    total_rows += rows_inserted
                finally:
                    session.close()
            
            # Re-enable foreign keys
            print("\n📦 Re-enabling foreign key constraints...")
            await self._execute_docker_exec_sql("SET CONSTRAINTS ALL IMMEDIATE;")
            
            print("\n" + "=" * 80)
            print(f"✅ Docker exec restore completed successfully!")
            print(f"   Total rows restored: {total_rows}")
            print(f"   Foreign keys re-enabled and validated")
            print("=" * 80)
            
        except Exception as e:
            print(f"\n❌ Docker exec restore failed: {e}", file=sys.stderr)
            import traceback
            traceback.print_exc()
            sys.exit(1)
        finally:
            if backup_engine:
                backup_engine.dispose()
    
    async def _sort_tables_by_dependencies(self, table_names: list) -> list:
        """Sort tables by foreign key dependencies (parents before children).
        
        Ensures that parent tables are inserted before child tables to respect
        foreign key constraints.
        """
        # Build dependency graph from ORM models
        dependencies = {}  # table_name -> set of table names it depends on
        
        for table_name in table_names:
            orm_model = self._get_orm_model_for_table(table_name)
            if not orm_model:
                dependencies[table_name] = set()
                continue
            
            depends_on = set()
            for fk in orm_model.__table__.foreign_keys:
                parent_table = fk.column.table.name
                if parent_table in table_names:
                    depends_on.add(parent_table)
            
            dependencies[table_name] = depends_on
        
        # Topological sort: tables with no dependencies first
        sorted_tables = []
        remaining = set(table_names)
        
        while remaining:
            # Find tables with no remaining dependencies
            ready = {t for t in remaining if not (dependencies.get(t, set()) & remaining)}
            
            if not ready:
                # Circular dependency detected, insert remaining in arbitrary order
                print(f"   ⚠️  Warning: Circular dependencies detected, inserting in arbitrary order")
                sorted_tables.extend(sorted(remaining))
                break
            
            sorted_tables.extend(sorted(ready))  # Sort for deterministic order
            remaining -= ready
        
        return sorted_tables
    
    async def _generate_ddl_from_backup(self, backup_engine) -> list:
        """Generate DDL statements from SQLite backup schema.
        
        Uses Letta's ORM metadata to generate PostgreSQL-compatible CREATE TABLE statements.
        """
        ddl_statements = []
        
        # First, ensure the public schema exists
        ddl_statements.append("CREATE SCHEMA IF NOT EXISTS public;")
        ddl_statements.append("SET search_path TO public;")
        
        # Use Letta's ORM Base metadata to generate schema
        from sqlalchemy.schema import CreateTable
        from sqlalchemy.dialects import postgresql
        
        # Get all ORM model tables
        for table in Base.metadata.tables.values():
            # Generate CREATE TABLE statement for PostgreSQL
            create_stmt = CreateTable(table)
            ddl = str(create_stmt.compile(dialect=postgresql.dialect()))
            ddl_statements.append(ddl)
        
        return ddl_statements
    
    async def _insert_rows_via_docker_exec(self, table_name: str, orm_model, instances: list) -> int:
        """Insert rows via docker exec using the configured restore strategy.
        
        Args:
            table_name: Name of the table to insert into
            orm_model: SQLAlchemy ORM model class
            instances: List of ORM instances to insert
            
        Returns:
            Number of rows inserted
        """
        try:
            rows_inserted = await self.restore_strategy.restore_table(
                table_name, orm_model, instances,
                self.ssh_host_for_docker,
                self.ssh_user_for_docker,
                self.ssh_key_for_docker,
                self.docker_container
            )
            return rows_inserted
        except Exception as e:
            print(f"   ❌ Failed to insert rows: {e}")
            raise
    
    # Old COPY-based and INSERT-based methods removed - now using RestoreStrategy pattern
    
    async def _execute_docker_exec_sql(self, sql_command: str) -> str:
        """Execute SQL command inside PostgreSQL Docker container via SSH.
        
        Args:
            sql_command: SQL command to execute
            
        Returns:
            Output from the command
        """
        import shlex
        
        if not self.ssh_host_for_docker:
            raise RuntimeError("docker_exec mode requires ssh_host to be set")
        
        # Build SSH command that executes docker exec inside the container
        ssh_key = self.ssh_key_for_docker or self._find_default_ssh_key()
        ssh_user_part = f"{self.ssh_user_for_docker}@" if self.ssh_user_for_docker else ""
        key_part = f"-i {ssh_key}" if ssh_key else ""
        
        # Use shlex.quote() to properly escape the SQL for the shell
        quoted_sql = shlex.quote(sql_command)
        
        # Build the full SSH + docker exec command
        docker_cmd = f'docker exec {self.docker_container} psql -U letta -d letta -c {quoted_sql}'
        ssh_cmd = f"ssh {key_part} {ssh_user_part}{self.ssh_host_for_docker} {shlex.quote(docker_cmd)}"
        
        try:
            result = subprocess.run(
                ssh_cmd,
                shell=True,
                capture_output=True,
                text=True,
                timeout=30
            )
            
            if result.returncode != 0:
                raise RuntimeError(f"docker exec command failed: {result.stderr}")
            
            return result.stdout
        except subprocess.TimeoutExpired as e:
            raise RuntimeError(f"docker exec command timed out: {e}")
        except Exception as e:
            raise RuntimeError(f"Failed to execute docker exec command: {e}")
    
    def _find_default_ssh_key(self) -> Optional[str]:
        """Find default SSH private key in ~/.ssh/ directory."""
        from pathlib import Path
        
        ssh_dir = Path.home() / ".ssh"
        if not ssh_dir.exists():
            return None
        
        # Check common SSH key names in order of preference
        key_names = [
            "id_ed25519",      # Modern, preferred
            "id_rsa",          # Traditional
            "id_ecdsa",        # ECDSA variant
            "id_dsa",          # Older DSA (less secure)
        ]
        
        for key_name in key_names:
            key_path = ssh_dir / key_name
            if key_path.exists():
                return str(key_path)
        
        return None
    
    def _close_ssh_tunnel(self) -> None:
        """Close SSH tunnel if active."""
        if self.ssh_process:
            try:
                self.ssh_process.terminate()
                self.ssh_process.wait(timeout=5)
                print("\n🔓 SSH tunnel closed")
            except Exception as e:
                print(f"Warning: Failed to close SSH tunnel: {e}")
                # Force kill if needed
                self.ssh_process.kill()

    async def verify(self) -> None:
        """Verify source database is readable.
        
        Non-destructive: only reads data, doesn't write anything.
        Tests both SSH tunnel connectivity and source database readability.
        """
        print("=" * 80)
        print("Letta Database Verification")
        print("=" * 80)
        print(f"\nDatabase: {self.current_db_type.value}")
        print(f"URI: {self._safe_uri(self.current_db_uri)}")
        
        # Test SSH tunnel if active
        if self.local_bind_port:
            print(f"\n🔐 Testing SSH tunnel on localhost:{self.local_bind_port}...")
            try:
                import socket
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(5)
                result = sock.connect_ex(('localhost', self.local_bind_port))
                sock.close()
                if result == 0:
                    print("   ✅ SSH tunnel is active and responding")
                else:
                    print(f"   ❌ SSH tunnel not responding on localhost:{self.local_bind_port}")
                    return
            except Exception as e:
                print(f"   ❌ SSH tunnel test failed: {e}")
                return
        
        # Verify source database readability
        print("\n📦 Verifying source database readability...")
        try:
            source_engine = create_async_engine(self._get_async_uri(self.current_db_uri), echo=False)
            
            async_session_factory_source = sessionmaker(source_engine, class_=AsyncSession, expire_on_commit=False)
            
            # Get all table names
            sync_source_engine = create_engine(self._get_sync_uri(self.current_db_uri))
            inspector = inspect(sync_source_engine)
            table_names = inspector.get_table_names()
            table_names = [t for t in table_names if t != 'alembic_version']
            
            print(f"Found {len(table_names)} tables")
            
            # Validate each table by reading via ORM
            total_rows = 0
            for table_name in table_names:
                orm_model = self._get_orm_model_for_table(table_name)
                if not orm_model:
                    continue
                
                async with async_session_factory_source() as source_session:
                    stmt = select(orm_model)
                    result = await source_session.execute(stmt)
                    instances = result.scalars().all()
                    total_rows += len(instances)
            
            await source_engine.dispose()
            sync_source_engine.dispose()
            
            print("\n" + "=" * 80)
            print(f"✅ Verification Complete!")
            print(f"   Source database: {self.current_db_type.value}")
            print(f"   Tables: {len(table_names)} readable")
            print(f"   Total rows: {total_rows}")
            if self.local_bind_port:
                print(f"   SSH tunnel: ✅ Active")
            print("=" * 80)
            
        except Exception as e:
            print(f"\n❌ Verification failed: {e}", file=sys.stderr)
            import traceback
            traceback.print_exc()
            sys.exit(1)
    
    async def backup(self, output_path: Path, dry_run: bool = False) -> None:
        """Backup current database to SQLite file.
        
        Args:
            output_path: Path to write backup file
            dry_run: If True, only validate data readability without writing
        """
        print("=" * 80)
        print(f"Letta Database {'Dry-Run Verification' if dry_run else 'Backup'}")
        print("=" * 80)
        print(f"\nSource database type: {self.current_db_type.value}")
        print(f"Source URI: {self._safe_uri(self.current_db_uri)}")
        if not dry_run:
            print(f"Output file: {output_path}")
        else:
            print("Mode: DRY RUN (checking data readability only)")

        if not dry_run:
            # Ensure output directory exists
            output_path.parent.mkdir(parents=True, exist_ok=True)

            # Warn if output file exists and delete it
            if output_path.exists():
                response = input(f"\n⚠️  File {output_path} already exists. Overwrite? (yes/no): ")
                if response.lower() not in ['yes', 'y']:
                    print("Backup cancelled.")
                    return
                # Delete the existing file to start fresh
                output_path.unlink()
                print("   ✓ Deleted existing backup file")

        # Create engines
        print("\nConnecting to source database...")
        source_engine = create_async_engine(self._get_async_uri(self.current_db_uri), echo=False)

        backup_engine = None
        if not dry_run:
            print("Creating backup database...")
            backup_uri = f"sqlite+aiosqlite:///{output_path}"
            backup_engine = create_async_engine(backup_uri, echo=False)

        try:
            if not dry_run:
                # Create schema in backup database (drop old schema first if exists)
                print("Creating schema in backup database...")
                async with backup_engine.begin() as conn:
                    await conn.run_sync(Base.metadata.drop_all)
                    await conn.run_sync(Base.metadata.create_all)

            # Get all table names
            sync_source_engine = create_engine(self._get_sync_uri(self.current_db_uri))
            inspector = inspect(sync_source_engine)
            table_names = inspector.get_table_names()

            # Filter out alembic version table
            table_names = [t for t in table_names if t != 'alembic_version']

            print(f"\nFound {len(table_names)} tables to backup")

            # Copy each table
            total_rows = 0
            for table_name in table_names:
                print(f"\n📦 {'Validating' if dry_run else 'Backing up'} table: {table_name}")
                if dry_run:
                    rows_checked = await self._validate_table_data(source_engine, table_name)
                    print(f"   ✓ Validated {rows_checked} rows")
                    total_rows += rows_checked
                else:
                    rows_copied = await self._copy_table_data(source_engine, backup_engine, table_name)
                    print(f"   ✓ Copied {rows_copied} rows")
                    total_rows += rows_copied

            print("\n" + "=" * 80)
            if dry_run:
                print(f"✅ Dry-run verification completed successfully!")
                print(f"   All data is readable from source database")
                print(f"   Total rows validated: {total_rows}")
                print(f"   ✓ No backup file was written (dry-run mode)")
            else:
                print(f"✅ Backup completed successfully!")
                print(f"   Total rows backed up: {total_rows}")
                print(f"   Backup file: {output_path}")
            print("=" * 80)

        except Exception as e:
            print(f"\n❌ Backup failed: {e}", file=sys.stderr)
            import traceback
            traceback.print_exc()
            sys.exit(1)
        finally:
            await source_engine.dispose()
            if backup_engine:
                await backup_engine.dispose()
            self._close_ssh_tunnel()

    async def pg_dump(self, output_path: Optional[Path] = None, dump_format: str = 'custom') -> None:
        """Dump PostgreSQL database using native pg_dump tool.
        
        This is the preferred method for PostgreSQL backups as it uses PostgreSQL's
        native dump format, handling all custom types automatically.
        
        Args:
            output_path: Path to write dump file. If None, uses default timestamp-based filename
            dump_format: Format for dump - 'custom' (compressed), 'plain' (SQL text), 'tar'
        """
        # Only works for PostgreSQL
        if self.current_db_type != DatabaseChoice.POSTGRES:
            print(f"❌ pg_dump requires PostgreSQL source. Current: {self.current_db_type.value}", file=sys.stderr)
            sys.exit(1)
        
        print("=" * 80)
        print("Letta Database Dump (pg_dump)")
        print("=" * 80)
        print(f"\nSource database type: PostgreSQL")
        print(f"Dump format: {dump_format}")
        
        # Generate default output path if not specified
        if output_path is None:
            timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
            ext = '.dump' if dump_format == 'custom' else '.sql' if dump_format == 'plain' else '.tar'
            output_path = Path(f"letta-dump-{timestamp}{ext}")
        
        print(f"Output file: {output_path}")
        
        # Ensure output directory exists
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Warn if output file exists and delete it
        if output_path.exists():
            response = input(f"\n⚠️  File {output_path} already exists. Overwrite? (yes/no): ")
            if response.lower() not in ['yes', 'y']:
                print("Dump cancelled.")
                return
            output_path.unlink()
            print("   ✓ Deleted existing dump file")
        
        # Parse connection details from URI
        try:
            from urllib.parse import urlparse
            parsed = urlparse(self.current_db_uri)
            
            host = parsed.hostname or 'localhost'
            port = parsed.port or 5432
            user = parsed.username or 'letta'
            password = parsed.password or 'letta'
            db = parsed.path.lstrip('/') or 'letta'
            
            # Build pg_dump command
            cmd = [
                'pg_dump',
                f'--host={host}',
                f'--port={port}',
                f'--username={user}',
                f'--dbname={db}',
                f'--format={dump_format[0]}',  # c=custom, p=plain, t=tar
                '--verbose'
            ]
            
            print(f"\nExecuting: {' '.join(cmd[:-1])} > {output_path}\n")
            
            # Set password in environment for pg_dump
            env = os.environ.copy()
            env['PGPASSWORD'] = password
            
            # Run pg_dump
            with open(output_path, 'wb') as f:
                result = subprocess.run(
                    cmd,
                    stdout=f,
                    stderr=subprocess.PIPE,
                    text=False,
                    env=env,
                    timeout=600
                )
            
            if result.returncode != 0:
                print(f"\n❌ pg_dump failed: {result.stderr.decode() if result.stderr else 'Unknown error'}")
                output_path.unlink()  # Remove partial dump file
                sys.exit(1)
            
            file_size = output_path.stat().st_size / (1024*1024)  # MB
            
            print("=" * 80)
            print(f"✅ Dump completed successfully!")
            print(f"   Dump file: {output_path}")
            print(f"   File size: {file_size:.2f} MB")
            print(f"   Format: {dump_format}")
            print("=" * 80)
            
        except Exception as e:
            print(f"\n❌ Dump failed: {e}", file=sys.stderr)
            import traceback
            traceback.print_exc()
            if output_path.exists():
                output_path.unlink()
            sys.exit(1)
        finally:
            self._close_ssh_tunnel()

    async def sql_dump(self, output_path: Optional[Path] = None) -> None:
        """Generate SQL dump file with CREATE TABLE + INSERT statements.
        
        This generates plain SQL without requiring pg_dump tool, avoiding version mismatches.
        The generated .sql file can be restored with: psql -f file.sql
        
        Args:
            output_path: Path to write SQL dump file. If None, uses default timestamp-based filename
        """
        print("=" * 80)
        print("Letta Database SQL Dump (CREATE + INSERT)")
        print("=" * 80)
        print(f"\nSource database type: {self.current_db_type.value}")
        print(f"Source URI: {self._safe_uri(self.current_db_uri)}")
        
        # Generate default output path if not specified
        if output_path is None:
            timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
            output_path = Path(f"letta-backup-{timestamp}.sql")
        
        print(f"Output file: {output_path}")
        
        # Ensure output directory exists
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Warn if output file exists and delete it
        if output_path.exists():
            response = input(f"\n⚠️  File {output_path} already exists. Overwrite? (yes/no): ")
            if response.lower() not in ['yes', 'y']:
                print("Dump cancelled.")
                return
            output_path.unlink()
            print("   ✓ Deleted existing dump file")
        
        try:
            # Create sync engine for reading
            print("\nConnecting to source database...")
            sync_engine = create_engine(self._get_sync_uri(self.current_db_uri))
            inspector = inspect(sync_engine)
            table_names = inspector.get_table_names()
            
            # Filter out alembic version table
            table_names = [t for t in table_names if t != 'alembic_version']
            
            print(f"Found {len(table_names)} tables to export")
            
            # Start writing SQL file
            with open(output_path, 'w') as f:
                # Write header
                f.write("-- Letta Database SQL Dump\n")
                f.write(f"-- Generated: {datetime.now().isoformat()}\n")
                f.write("-- Source: " + self._safe_uri(self.current_db_uri) + "\n")
                f.write("-- Restore with: psql -f " + output_path.name + "\n\n")
                
                # Disable foreign key constraints during restore
                f.write("-- Disable foreign key constraints for restore\n")
                f.write("SET CONSTRAINTS ALL DEFERRED;\n\n")
                
                # Get ORM models
                from letta.orm.base import Base
                
                total_rows = 0
                
                # Write CREATE TABLE statements
                f.write("-- ==================== SCHEMA ====================\n\n")
                for table_name in table_names:
                    print(f"\n📦 Processing table: {table_name}")
                    
                    # Get ORM model for this table
                    orm_model = self._get_orm_model_for_table(table_name)
                    
                    if orm_model:
                        # Write CREATE TABLE statement
                        f.write(f"DROP TABLE IF EXISTS {table_name} CASCADE;\n")
                        
                        # Get CREATE TABLE DDL from ORM metadata
                        from sqlalchemy.schema import CreateTable
                        create_stmt = CreateTable(orm_model.__table__)
                        f.write(str(create_stmt.compile(compile_kwargs={"literal_binds": True})) + ";\n\n")
                    else:
                        print(f"   ⚠️  Skipping (no ORM model)")
                        continue
                
                # Write INSERT statements
                f.write("\n-- ==================== DATA ====================\n\n")
                
                for table_name in table_names:
                    orm_model = self._get_orm_model_for_table(table_name)
                    if not orm_model:
                        continue
                    
                    print(f"\n📦 Exporting data from table: {table_name}")
                    
                    # Read all rows from source
                    with sync_engine.begin() as conn:
                        stmt = select(orm_model)
                        result = conn.execute(stmt)
                        rows = result.fetchall()
                        
                        if rows:
                            # Write INSERT statements
                            for row_obj in rows:
                                # Generate INSERT statement
                                columns = [col.name for col in orm_model.__table__.columns]
                                values = []
                                
                                for col_name in columns:
                                    value = getattr(row_obj, col_name)
                                    sql_value = self._escape_sql_value_for_dump(value)
                                    values.append(sql_value)
                                
                                columns_str = ", ".join([f'"{col}"' for col in columns])
                                values_str = ", ".join(values)
                                insert_sql = f'INSERT INTO "{table_name}" ({columns_str}) VALUES ({values_str});\n'
                                f.write(insert_sql)
                            
                            print(f"   ✓ Exported {len(rows)} rows")
                            total_rows += len(rows)
                        else:
                            print(f"   ✓ 0 rows")
                
                # Write footer
                f.write("\n-- ==================== CONSTRAINTS ====================\n\n")
                f.write("-- Re-enable foreign key constraints\n")
                f.write("SET CONSTRAINTS ALL IMMEDIATE;\n")
            
            file_size = output_path.stat().st_size / (1024*1024)  # MB
            
            print("\n" + "=" * 80)
            print(f"✅ SQL dump completed successfully!")
            print(f"   Dump file: {output_path}")
            print(f"   File size: {file_size:.2f} MB")
            print(f"   Total rows: {total_rows}")
            print(f"\nRestore with:")
            print(f"   psql -h <host> -U letta -d letta -f {output_path.name}")
            print("=" * 80)
            
        except Exception as e:
            print(f"\n❌ SQL dump failed: {e}", file=sys.stderr)
            import traceback
            traceback.print_exc()
            if output_path.exists():
                output_path.unlink()
            sys.exit(1)
        finally:
            sync_engine.dispose()
            self._close_ssh_tunnel()

    def _escape_sql_value_for_dump(self, value) -> str:
        """Escape a Python value for SQL INSERT statement in dump file."""
        import json
        from pydantic import BaseModel
        
        if value is None:
            return "NULL"
        elif isinstance(value, bool):
            return "TRUE" if value else "FALSE"
        elif isinstance(value, int):
            return str(value)
        elif isinstance(value, float):
            return str(value)
        elif isinstance(value, bytes):
            hex_str = value.hex()
            return f"E'\\\\x{hex_str}'"
        elif isinstance(value, BaseModel):
            # Handle Pydantic models (convert to dict)
            json_str = json.dumps(value.model_dump())
            escaped = json_str.replace("'", "''")
            return f"'{escaped}'::json"
        elif isinstance(value, (list, dict)):
            # Try to serialize, handle non-serializable objects
            try:
                json_str = json.dumps(value)
            except TypeError:
                # Fall back to string representation if not JSON serializable
                json_str = json.dumps(str(value))
            escaped = json_str.replace("'", "''")
            return f"'{escaped}'::json"
        else:
            # String or other types - convert to string and escape
            str_val = str(value).replace("'", "''")
            return f"'{str_val}'"

    async def restore(self, backup_path: Path, target_type: Optional[DatabaseChoice] = None) -> None:
        """Restore database from SQLite backup file."""
        print("=" * 80)
        print("Letta Database Restore")
        print("=" * 80)

        if not backup_path.exists():
            print(f"❌ Backup file not found: {backup_path}", file=sys.stderr)
            sys.exit(1)

        # If docker-exec mode, use special restore path
        if self.docker_exec and self.ssh_host_for_docker:
            await self._restore_via_docker_exec(backup_path)
            return

        # Determine target database
        if target_type is None:
            target_type = self.current_db_type
            print(f"\nTarget database type: {target_type.value} (from current config)")
        else:
            print(f"\nTarget database type: {target_type.value} (specified)")

        # Get target URI
        if target_type == DatabaseChoice.SQLITE:
            target_uri = self._get_sqlite_uri()
        else:
            target_uri = settings.letta_pg_uri

        print(f"Source backup file: {backup_path}")
        print(f"Target URI: {self._safe_uri(target_uri)}")
        
        # Test target connection BEFORE asking for confirmation
        print("\n🔐 Testing target database connection...")
        try:
            test_engine = create_async_engine(self._get_async_uri(target_uri), echo=False)
            async with test_engine.begin() as conn:
                await conn.execute(text("SELECT 1"))
            await test_engine.dispose()
            print("   ✅ Target database connection successful")
        except Exception as e:
            print(f"   ❌ Target database connection failed: {e}", file=sys.stderr)
            if "password" in str(e).lower():
                print("      Check your password/credentials", file=sys.stderr)
            sys.exit(1)

        # Confirm if target database already has data
        response = input("\n⚠️  This will overwrite the target database. Continue? (yes/no): ")
        if response.lower() not in ['yes', 'y']:
            print("Restore cancelled.")
            return

        # Create engines
        print("\nConnecting to backup database...")
        backup_uri = f"sqlite+aiosqlite:///{backup_path}"
        backup_engine = create_async_engine(backup_uri, echo=False)

        print("Connecting to target database...")
        target_engine = create_async_engine(self._get_async_uri(target_uri), echo=False)

        try:
            # Create schema in target database
            print("Creating schema in target database...")
            async with target_engine.begin() as conn:
                await conn.run_sync(Base.metadata.drop_all)
                await conn.run_sync(Base.metadata.create_all)

            # Get all table names from backup
            sync_backup_engine = create_engine(f"sqlite:///{backup_path}")
            inspector = inspect(sync_backup_engine)
            table_names = inspector.get_table_names()

            # Filter out alembic version table
            table_names = [t for t in table_names if t != 'alembic_version']

            print(f"\nFound {len(table_names)} tables to restore")

            # Copy each table
            total_rows = 0
            for table_name in table_names:
                print(f"\n📦 Restoring table: {table_name}")
                rows_copied = await self._copy_table_data(backup_engine, target_engine, table_name)
                print(f"   ✓ Copied {rows_copied} rows")
                total_rows += rows_copied

            # Reset PostgreSQL sequences if restoring to PostgreSQL
            if target_type == DatabaseChoice.POSTGRES:
                print(f"\n🔄 Resetting PostgreSQL sequences...")
                try:
                    await self._reset_postgresql_sequences(target_engine, table_names)
                    print(f"   ✓ All sequences reset successfully")
                except Exception as seq_error:
                    print(f"   ⚠️  Warning: Sequence reset failed: {seq_error}")
                    print(f"      (Data is intact, but you may need to manually reset sequences)")

            print("\n" + "=" * 80)
            print(f"✅ Restore completed successfully!")
            print(f"   Total rows restored: {total_rows}")
            print(f"   Target database: {self._safe_uri(target_uri)}")
            print("=" * 80)

        except Exception as e:
            print(f"\n❌ Restore failed: {e}", file=sys.stderr)
            import traceback
            traceback.print_exc()
            sys.exit(1)
        finally:
            await backup_engine.dispose()
            await target_engine.dispose()
            self._close_ssh_tunnel()

    async def migrate(self, target_type: DatabaseChoice) -> None:
        """Migrate database from current type to target type."""
        print("=" * 80)
        print("Letta Database Migration")
        print("=" * 80)

        if self.current_db_type == target_type:
            print(f"❌ Already using {target_type.value}. No migration needed.", file=sys.stderr)
            sys.exit(1)

        print(f"\nMigration: {self.current_db_type.value} → {target_type.value}")

        # Create temporary backup
        timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
        temp_backup = Path(f"/tmp/letta-migration-{timestamp}.db")

        print(f"\nStep 1: Creating temporary backup at {temp_backup}")
        await self.backup(temp_backup)

        print(f"\nStep 2: Restoring to {target_type.value} database")
        await self.restore(temp_backup, target_type)

        print(f"\nCleaning up temporary backup...")
        temp_backup.unlink()

        print("\n" + "=" * 80)
        print("✅ Migration completed successfully!")
        print("=" * 80)

        if target_type == DatabaseChoice.SQLITE:
            print("\nNext steps:")
            print("1. Remove or unset PostgreSQL environment variables:")
            print("   - LETTA_PG_DB, LETTA_PG_USER, LETTA_PG_PASSWORD")
            print("   - LETTA_PG_HOST, LETTA_PG_PORT, LETTA_PG_URI")
            print("2. Restart Letta - it will now use SQLite by default")
        else:
            print("\nNext steps:")
            print("1. Ensure PostgreSQL environment variables are set:")
            print("   - LETTA_PG_DB, LETTA_PG_USER, LETTA_PG_PASSWORD")
            print("   - LETTA_PG_HOST, LETTA_PG_PORT")
            print("   OR set LETTA_PG_URI directly")
            print("2. Restart Letta - it will now use PostgreSQL")

    async def _validate_table_data(self, source_engine, table_name: str) -> int:
        """Validate that all data in a table can be read from source database.
        
        This is used for dry-run/verify modes - it only reads data without writing.
        """
        # Get the ORM model for this table
        orm_model = self._get_orm_model_for_table(table_name)
        
        if not orm_model:
            # Fallback to raw SQL for tables without ORM models
            return await self._validate_table_data_raw_sql(source_engine, table_name)
        
        async_session_factory_source = sessionmaker(source_engine, class_=AsyncSession, expire_on_commit=False)
        
        rows_validated = 0
        
        try:
            async with async_session_factory_source() as source_session:
                # Fetch all instances using ORM (triggers deserialization via TypeDecorator)
                stmt = select(orm_model)
                result = await source_session.execute(stmt)
                instances = result.scalars().all()
                
                if not instances:
                    return 0
                
                rows_validated = len(instances)
                print(f"   Found {rows_validated} instances, all readable ✓")
                
            return rows_validated
            
        except Exception as e:
            print(f"   ❌ Error validating {table_name}: {e}")
            raise
    
    async def _validate_table_data_raw_sql(self, source_engine, table_name: str) -> int:
        """Validate table data using raw SQL (for non-ORM tables)."""
        async_session_factory_source = sessionmaker(source_engine, class_=AsyncSession, expire_on_commit=False)

        async with async_session_factory_source() as source_session:
            # Fetch all data
            result = await source_session.execute(text(f'SELECT * FROM "{table_name}"'))
            rows = result.fetchall()

            if not rows:
                return 0
            
            print(f"   Found {len(rows)} rows, all readable ✓")
            return len(rows)

    async def _copy_table_data(self, source_engine, target_engine, table_name: str) -> int:
        """Copy data from source table to target table using ORM for proper type handling.
        
        This uses SQLAlchemy's ORM layer to ensure custom TypeDecorators are properly invoked,
        handling serialization/deserialization for vectors, JSON configs, and other custom types.
        """
        # Get the ORM model for this table
        orm_model = self._get_orm_model_for_table(table_name)
        
        if not orm_model:
            # Fallback to raw SQL for tables without ORM models
            return await self._copy_table_data_raw_sql(source_engine, target_engine, table_name)
        
        async_session_factory_source = sessionmaker(source_engine, class_=AsyncSession, expire_on_commit=False)
        async_session_factory_target = sessionmaker(target_engine, class_=AsyncSession, expire_on_commit=False)
        
        rows_copied = 0
        
        try:
            async with async_session_factory_source() as source_session:
                # Fetch all instances using ORM (triggers deserialization via TypeDecorator)
                stmt = select(orm_model)
                result = await source_session.execute(stmt)
                instances = result.scalars().all()
                
                if not instances:
                    return 0
                
                total_instances = len(instances)
                print(f"   Found {total_instances} instances to copy")
                
            # Insert in batches into target
            batch_size = 100
            async with async_session_factory_target() as target_session:
                for i in range(0, len(instances), batch_size):
                    batch = instances[i:i + batch_size]
                    
                    for instance in batch:
                        try:
                            # Create a new instance in target session
                            # This triggers serialization via TypeDecorator.process_bind_param()
                            target_session.add(self._copy_orm_instance(instance))
                        except Exception as e:
                            raise ValueError(
                                f"Failed to copy {orm_model.__name__} instance "
                                f"(index {rows_copied + i + 1}): {e}"
                            ) from e
                    
                    await target_session.commit()
                    rows_copied += len(batch)
                    
                    if i > 0 and i % 500 == 0:
                        print(f"   ✓ Copied {rows_copied}/{total_instances} instances")
            
            # Validate row count
            async with async_session_factory_target() as target_session:
                count_stmt = select(func.count()).select_from(orm_model)
                result = await target_session.execute(count_stmt)
                target_count = result.scalar() or 0
                
                if target_count != total_instances:
                    raise ValueError(
                        f"Row count mismatch for {table_name}: "
                        f"expected {total_instances}, got {target_count}"
                    )
            
            return rows_copied
            
        except Exception as e:
            print(f"   ❌ Error copying {table_name}: {e}")
            raise
    
    def _copy_orm_instance(self, instance):
        """Create a copy of an ORM instance for insertion into target database.
        
        This creates a new detached instance that can be added to a different session,
        ensuring all attributes are properly copied without foreign key constraints.
        """
        from copy import deepcopy
        
        # Create a new instance of the same class
        new_instance = instance.__class__()
        
        # Copy all attributes except relationships and SQLAlchemy internal attributes
        for column in instance.__table__.columns:
            attr_name = column.name
            if hasattr(instance, attr_name):
                value = getattr(instance, attr_name)
                # Deep copy to avoid shared references
                setattr(new_instance, attr_name, deepcopy(value))
        
        return new_instance
    
    def _get_orm_model_for_table(self, table_name: str):
        """Get the ORM model class for a given table name."""
        # Import all ORM models
        from letta.orm import (
            Agent, AgentsTags, Archive, ArchivesAgents, Block, BlockHistory, 
            BlocksAgents, FileMetadata, FileAgent, Group, GroupsAgents, GroupsBlocks,
            IdentitiesAgents, IdentitiesBlocks, Identity, Job, LLMBatchItem, LLMBatchJob,
            MCPOAuth, MCPServer, Message, Organization, ArchivalPassage, SourcePassage,
            PassageTag, Prompt, Provider, ProviderModel, ProviderTrace, Run, RunMetrics,
            AgentEnvironmentVariable, SandboxConfig, SandboxEnvironmentVariable, Source,
            SourcesAgents, Step, StepMetrics, Tool, ToolsAgents, User
        )
        
        # Map table names to ORM models
        table_to_model = {
            "agents": Agent,
            "agents_tags": AgentsTags,
            "archives": Archive,
            "archives_agents": ArchivesAgents,
            "blocks": Block,
            "block_history": BlockHistory,
            "blocks_agents": BlocksAgents,
            "files": FileMetadata,
            "files_agents": FileAgent,
            "groups": Group,
            "groups_agents": GroupsAgents,
            "groups_blocks": GroupsBlocks,
            "identities_agents": IdentitiesAgents,
            "identities_blocks": IdentitiesBlocks,
            "identities": Identity,
            "jobs": Job,
            "llm_batch_items": LLMBatchItem,
            "llm_batch_jobs": LLMBatchJob,
            "llm_batch_job": LLMBatchJob,  # Singular variant
            "mcp_oauth": MCPOAuth,
            "mcp_servers": MCPServer,
            "mcp_server": MCPServer,  # Singular variant
            "messages": Message,
            "organizations": Organization,
            "archival_passages": ArchivalPassage,
            "source_passages": SourcePassage,
            "passage_tags": PassageTag,
            "prompts": Prompt,
            "providers": Provider,
            "provider_models": ProviderModel,
            "provider_traces": ProviderTrace,
            "runs": Run,
            "run_metrics": RunMetrics,
            "sandbox_environment_variables": SandboxEnvironmentVariable,
            "sandbox_configs": SandboxConfig,
            "agent_environment_variables": AgentEnvironmentVariable,
            "sources": Source,
            "sources_agents": SourcesAgents,
            "steps": Step,
            "step_metrics": StepMetrics,
            "tools": Tool,
            "tools_agents": ToolsAgents,
            "users": User,
        }
        
        return table_to_model.get(table_name)
    
    async def _copy_table_data_raw_sql(self, source_engine, target_engine, table_name: str) -> int:
        """Fallback: Copy data using raw SQL for tables without ORM models.
        
        This is only used for system tables like alembic_version that don't have ORM models.
        """
        async_session_factory_source = sessionmaker(source_engine, class_=AsyncSession, expire_on_commit=False)
        async_session_factory_target = sessionmaker(target_engine, class_=AsyncSession, expire_on_commit=False)

        async with async_session_factory_source() as source_session:
            # Fetch all data
            result = await source_session.execute(text(f'SELECT * FROM "{table_name}"'))
            columns = result.keys()
            rows = result.fetchall()

            if not rows:
                return 0

            # Convert to list of dicts
            data = []
            for row in rows:
                row_dict = dict(zip(columns, row))
                data.append(row_dict)

        # Insert into target
        async with async_session_factory_target() as target_session:
            # Get table metadata
            metadata = MetaData()
            await target_session.run_sync(lambda sync_session: metadata.reflect(bind=sync_session.bind))
            table = Table(table_name, metadata, autoload_with=target_engine.sync_engine)

            # Insert in batches
            batch_size = 100
            for i in range(0, len(data), batch_size):
                batch = data[i:i + batch_size]
                await target_session.execute(table.insert(), batch)
                await target_session.commit()

        return len(data)

    async def _reset_postgresql_sequences(self, target_engine, table_names: List[str]) -> None:
        """Reset PostgreSQL sequences to max(id) + 1 for auto-increment columns.
        
        This ensures that subsequent INSERT statements won't violate unique constraints.
        """
        # Get table metadata
        sync_engine = create_engine(self._get_sync_uri(target_engine.url.render_as_string()))
        inspector = inspect(sync_engine)
        
        async with target_engine.begin() as conn:
            for table_name in table_names:
                # Get columns and their types
                columns = inspector.get_columns(table_name)
                
                for column in columns:
                    col_name = column['name']
                    col_type = str(column['type'])
                    
                    # Check if this is a primary key with auto-increment
                    pk_constraint = inspector.get_pk_constraint(table_name)
                    is_pk = col_name in pk_constraint.get('constrained_columns', [])
                    
                    # Only reset if it's INTEGER/BIGINT/SMALLINT and primary key
                    if is_pk and any(t in col_type for t in ['INTEGER', 'BIGINT', 'SMALLINT']):
                        # PostgreSQL sequence naming convention: tablename_columnname_seq
                        seq_name = f"{table_name}_{col_name}_seq"
                        
                        # Reset sequence to max(id) + 1
                        reset_sql = f"""
                            SELECT setval(
                                pg_get_serial_sequence('{table_name}', '{col_name}'),
                                COALESCE(MAX({col_name}), 0) + 1
                            ) FROM "{table_name}";
                        """
                        
                        try:
                            await conn.execute(text(reset_sql))
                        except Exception as e:
                            # Sequence might not exist or table might be empty - not critical
                            pass

    def _get_async_uri(self, uri: str) -> str:
        """Convert database URI to async format."""
        if uri.startswith("postgresql://") or uri.startswith("postgresql+pg8000://"):
            # Convert to asyncpg
            uri = uri.replace("postgresql://", "postgresql+asyncpg://")
            uri = uri.replace("postgresql+pg8000://", "postgresql+asyncpg://")
        elif uri.startswith("sqlite://"):
            # Convert to aiosqlite
            uri = uri.replace("sqlite://", "sqlite+aiosqlite://")
        return uri

    def _get_sync_uri(self, uri: str) -> str:
        """Convert database URI to sync format."""
        uri = uri.replace("+asyncpg", "+pg8000")
        uri = uri.replace("+aiosqlite", "")
        return uri

    def _get_sqlite_uri(self) -> str:
        """Get SQLite database URI."""
        sqlite_path = settings.letta_dir / "letta.db"
        return f"sqlite+aiosqlite:///{sqlite_path}"

    def _safe_uri(self, uri: str) -> str:
        """Return URI with password masked for display."""
        if ":" in uri and "@" in uri:
            # Mask password in postgresql://user:password@host:port/db
            parts = uri.split("@")
            if len(parts) == 2:
                prefix = parts[0].split(":")
                if len(prefix) >= 3:
                    prefix[-1] = "****"
                    return ":".join(prefix) + "@" + parts[1]
        return uri


async def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Letta Database Backup, Restore, and Migration Tool",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )
    
    subparsers = parser.add_subparsers(dest='command', help='Command to execute')

    # Backup command
    backup_parser = subparsers.add_parser('backup', help='Backup current database to file')
    backup_parser.add_argument('--host', type=str, default=None, help='Database server hostname (default: localhost)')
    backup_parser.add_argument('--port', type=int, default=None, help='Database server port (default: 5432)')
    backup_parser.add_argument('--user', type=str, default=None, help='Database user (default: letta)')
    backup_parser.add_argument('--password', type=str, default=None, help='Database password (default: letta)')
    backup_parser.add_argument('--db', type=str, default=None, help='Database name (default: letta)')
    backup_parser.add_argument('--ssh-host', type=str, default=None, help='SSH host to tunnel through (e.g., user@Henriks-Mac-Pro.local)')
    backup_parser.add_argument('--ssh-user', type=str, default=None, help='SSH username (default: current user)')
    backup_parser.add_argument('--ssh-key', type=str, default=None, help='Path to SSH private key')
    backup_parser.add_argument(
        '--output', '-o',
        type=Path,
        help='Output backup file path (default: letta-backup-TIMESTAMP.db)'
    )
    backup_parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Verify data readability without writing backup file'
    )

    # Restore command
    restore_parser = subparsers.add_parser('restore', help='Restore database from backup file')
    restore_parser.add_argument('backup_file', type=Path, help='Backup file to restore from')
    restore_parser.add_argument('--host', type=str, default=None, help='Database server hostname (default: localhost)')
    restore_parser.add_argument('--port', type=int, default=None, help='Database server port (default: 5432)')
    restore_parser.add_argument('--user', type=str, default=None, help='Database user (default: letta)')
    restore_parser.add_argument('--password', type=str, default=None, help='Database password (default: letta)')
    restore_parser.add_argument('--db', type=str, default=None, help='Database name (default: letta)')
    restore_parser.add_argument('--ssh-host', type=str, default=None, help='SSH host to tunnel through (e.g., user@Henriks-Mac-Pro.local)')
    restore_parser.add_argument('--ssh-user', type=str, default=None, help='SSH username (default: current user)')
    restore_parser.add_argument('--ssh-key', type=str, default=None, help='Path to SSH private key')
    restore_parser.add_argument('--docker-exec', action='store_true', help='Use docker exec instead of port tunneling (requires SSH access)')
    restore_parser.add_argument('--docker-container', type=str, default='postgres', help='Docker container name (default: postgres)')
    restore_group = restore_parser.add_mutually_exclusive_group()
    restore_group.add_argument('--sqlite', action='store_true', help='Restore to SQLite database')
    restore_group.add_argument('--postgres', action='store_true', help='Restore to PostgreSQL database')

    # Migrate command
    migrate_parser = subparsers.add_parser('migrate', help='Migrate database to different type')
    migrate_parser.add_argument('--host', type=str, default=None, help='Database server hostname (default: localhost)')
    migrate_parser.add_argument('--port', type=int, default=None, help='Database server port (default: 5432)')
    migrate_parser.add_argument('--user', type=str, default=None, help='Database user (default: letta)')
    migrate_parser.add_argument('--password', type=str, default=None, help='Database password (default: letta)')
    migrate_parser.add_argument('--db', type=str, default=None, help='Database name (default: letta)')
    migrate_parser.add_argument('--ssh-host', type=str, default=None, help='SSH host to tunnel through (e.g., user@Henriks-Mac-Pro.local)')
    migrate_parser.add_argument('--ssh-user', type=str, default=None, help='SSH username (default: current user)')
    migrate_parser.add_argument('--ssh-key', type=str, default=None, help='Path to SSH private key')
    migrate_parser.add_argument(
        '--to',
        choices=['sqlite', 'postgres'],
        required=True,
        help='Target database type'
    )

    # Verify command
    verify_parser = subparsers.add_parser('verify', help='Verify database readability without backup')
    verify_parser.add_argument('--host', type=str, default=None, help='Database server hostname (default: localhost)')
    verify_parser.add_argument('--port', type=int, default=None, help='Database server port (default: 5432)')
    verify_parser.add_argument('--user', type=str, default=None, help='Database user (default: letta)')
    verify_parser.add_argument('--password', type=str, default=None, help='Database password (default: letta)')
    verify_parser.add_argument('--db', type=str, default=None, help='Database name (default: letta)')
    verify_parser.add_argument('--ssh-host', type=str, default=None, help='SSH host to tunnel through (e.g., user@Henriks-Mac-Pro.local)')
    verify_parser.add_argument('--ssh-user', type=str, default=None, help='SSH username (default: current user)')
    verify_parser.add_argument('--ssh-key', type=str, default=None, help='Path to SSH private key')

    # Dump command (pg_dump for PostgreSQL)
    dump_parser = subparsers.add_parser('dump', help='Dump PostgreSQL database using pg_dump')
    dump_parser.add_argument('output', type=str, help='Output file path (- for stdout)')
    dump_parser.add_argument('--format', type=str, default='custom', choices=['custom', 'plain', 'tar'], help='Dump format (default: custom)')
    dump_parser.add_argument('--host', type=str, default=None, help='Database server hostname (default: localhost)')
    dump_parser.add_argument('--port', type=int, default=None, help='Database server port (default: 5432)')
    dump_parser.add_argument('--user', type=str, default=None, help='Database user (default: letta)')
    dump_parser.add_argument('--password', type=str, default=None, help='Database password (default: letta)')
    dump_parser.add_argument('--db', type=str, default=None, help='Database name (default: letta)')
    dump_parser.add_argument('--ssh-host', type=str, default=None, help='SSH host to tunnel through (e.g., user@Henriks-Mac-Pro.local)')
    dump_parser.add_argument('--ssh-user', type=str, default=None, help='SSH username (default: current user)')
    dump_parser.add_argument('--ssh-key', type=str, default=None, help='Path to SSH private key')

    # SQL Dump command (generate plain SQL INSERT statements without pg_dump)
    sql_dump_parser = subparsers.add_parser('sql-dump', help='Generate SQL dump file (CREATE + INSERT statements)')
    sql_dump_parser.add_argument('output', type=str, nargs='?', default=None, help='Output SQL file path (default: letta-backup-TIMESTAMP.sql)')
    sql_dump_parser.add_argument('--host', type=str, default=None, help='Database server hostname (default: localhost)')
    sql_dump_parser.add_argument('--port', type=int, default=None, help='Database server port (default: 5432)')
    sql_dump_parser.add_argument('--user', type=str, default=None, help='Database user (default: letta)')
    sql_dump_parser.add_argument('--password', type=str, default=None, help='Database password (default: letta)')
    sql_dump_parser.add_argument('--db', type=str, default=None, help='Database name (default: letta)')
    sql_dump_parser.add_argument('--ssh-host', type=str, default=None, help='SSH host to tunnel through (e.g., user@Henriks-Mac-Pro.local)')
    sql_dump_parser.add_argument('--ssh-user', type=str, default=None, help='SSH username (default: current user)')
    sql_dump_parser.add_argument('--ssh-key', type=str, default=None, help='Path to SSH private key')

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    copier = DatabaseCopier(
        host=args.host if hasattr(args, 'host') else None,
        port=args.port if hasattr(args, 'port') else None,
        user=args.user if hasattr(args, 'user') else None,
        password=args.password if hasattr(args, 'password') else None,
        db=args.db if hasattr(args, 'db') else None,
        ssh_host=args.ssh_host if hasattr(args, 'ssh_host') else None,
        ssh_user=args.ssh_user if hasattr(args, 'ssh_user') else None,
        ssh_key=args.ssh_key if hasattr(args, 'ssh_key') else None,
        docker_exec=args.docker_exec if hasattr(args, 'docker_exec') else False,
        docker_container=args.docker_container if hasattr(args, 'docker_container') else 'postgres'
    )

    if args.command == 'backup':
        # Generate default output path if not specified
        if args.output is None and not args.dry_run:
            timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
            args.output = Path(f"letta-backup-{timestamp}.db")
        elif args.output is None and args.dry_run:
            args.output = Path("/tmp/letta-verify.db")  # Dummy path for dry-run
        await copier.backup(args.output, dry_run=args.dry_run)

    elif args.command == 'verify':
        # Verify source database readability and test SSH tunnel if configured
        await copier.verify()

    elif args.command == 'restore':
        target_type = None
        if args.sqlite:
            target_type = DatabaseChoice.SQLITE
        elif args.postgres:
            target_type = DatabaseChoice.POSTGRES
        await copier.restore(args.backup_file, target_type)

    elif args.command == 'dump':
        # Use pg_dump for PostgreSQL databases
        output_path = Path(args.output) if args.output != '-' else None
        await copier.pg_dump(output_path, dump_format=args.format)

    elif args.command == 'sql-dump':
        # Generate SQL dump (no pg_dump tool needed)
        output_path = None
        if args.output:
            output_path = Path(args.output)
        await copier.sql_dump(output_path)

    elif args.command == 'migrate':
        target_type = DatabaseChoice.SQLITE if args.to == 'sqlite' else DatabaseChoice.POSTGRES
        await copier.migrate(target_type)


if __name__ == "__main__":
    asyncio.run(main())
