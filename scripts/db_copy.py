#!/usr/bin/env python3
"""
Database backup, restore, and migration tool for Letta db.

This script provides flexible database operations:
- Backup: Export current database to SQLite file
- Restore: Import from SQLite backup to target database
- Migrate: One-step migration between database types

Usage:
    # Backup current database (use postfix backup.db to avoid git checkin)
    python scripts/db_copy.py backup [--output backup.db]

    # Restore from backup
    python scripts/db_copy.py restore backup.db [--sqlite|--postgres]

    # Direct migration
    python scripts/db_copy.py migrate --to [sqlite|postgres]

For Docker workflows:
    # Backup from container
    docker exec letta_server python scripts/db_copy.py backup --output /app/backup.db
    docker cp letta_server:/app/backup.db ./backup.db

    # Restore to container
    docker cp ./backup.db letta_server:/app/backup.db
    docker exec letta_server python scripts/db_copy.py restore /app/backup.db --sqlite
"""

import argparse
import asyncio
import sys
from datetime import datetime
from pathlib import Path
from typing import List, Optional

from sqlalchemy import MetaData, Table, create_engine, inspect, text
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker

# Letta imports
from letta.orm.base import Base
from letta.settings import DatabaseChoice, settings


class DatabaseCopier:
    """Handles database backup, restore, and migration operations."""

    def __init__(self):
        self.current_db_uri = settings.letta_db_uri
        self.current_db_type = settings.database_engine

    async def backup(self, output_path: Path) -> None:
        """Backup current database to SQLite file."""
        print("=" * 80)
        print("Letta Database Backup")
        print("=" * 80)
        print(f"\nSource database type: {self.current_db_type.value}")
        print(f"Source URI: {self._safe_uri(self.current_db_uri)}")
        print(f"Output file: {output_path}")

        # Ensure output directory exists
        output_path.parent.mkdir(parents=True, exist_ok=True)

        # Warn if output file exists
        if output_path.exists():
            response = input(f"\n⚠️  File {output_path} already exists. Overwrite? (yes/no): ")
            if response.lower() not in ['yes', 'y']:
                print("Backup cancelled.")
                return

        # Create engines
        print("\nConnecting to source database...")
        source_engine = create_async_engine(self._get_async_uri(self.current_db_uri), echo=False)

        print("Creating backup database...")
        backup_uri = f"sqlite+aiosqlite:///{output_path}"
        backup_engine = create_async_engine(backup_uri, echo=False)

        try:
            # Create schema in backup database
            print("Creating schema in backup database...")
            async with backup_engine.begin() as conn:
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
                print(f"\n📦 Backing up table: {table_name}")
                rows_copied = await self._copy_table_data(source_engine, backup_engine, table_name)
                print(f"   ✓ Copied {rows_copied} rows")
                total_rows += rows_copied

            print("\n" + "=" * 80)
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
            await backup_engine.dispose()

    async def restore(self, backup_path: Path, target_type: Optional[DatabaseChoice] = None) -> None:
        """Restore database from SQLite backup file."""
        print("=" * 80)
        print("Letta Database Restore")
        print("=" * 80)

        if not backup_path.exists():
            print(f"❌ Backup file not found: {backup_path}", file=sys.stderr)
            sys.exit(1)

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

    async def _copy_table_data(self, source_engine, target_engine, table_name: str) -> int:
        """Copy data from source table to target table."""
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
    backup_parser.add_argument(
        '--output', '-o',
        type=Path,
        help='Output backup file path (default: letta-backup-TIMESTAMP.db)'
    )

    # Restore command
    restore_parser = subparsers.add_parser('restore', help='Restore database from backup file')
    restore_parser.add_argument('backup_file', type=Path, help='Backup file to restore from')
    restore_group = restore_parser.add_mutually_exclusive_group()
    restore_group.add_argument('--sqlite', action='store_true', help='Restore to SQLite database')
    restore_group.add_argument('--postgres', action='store_true', help='Restore to PostgreSQL database')

    # Migrate command
    migrate_parser = subparsers.add_parser('migrate', help='Migrate database to different type')
    migrate_parser.add_argument(
        '--to',
        choices=['sqlite', 'postgres'],
        required=True,
        help='Target database type'
    )

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    copier = DatabaseCopier()

    if args.command == 'backup':
        # Generate default output path if not specified
        if args.output is None:
            timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
            args.output = Path(f"letta-backup-{timestamp}.db")
        await copier.backup(args.output)

    elif args.command == 'restore':
        target_type = None
        if args.sqlite:
            target_type = DatabaseChoice.SQLITE
        elif args.postgres:
            target_type = DatabaseChoice.POSTGRES
        await copier.restore(args.backup_file, target_type)

    elif args.command == 'migrate':
        target_type = DatabaseChoice.SQLITE if args.to == 'sqlite' else DatabaseChoice.POSTGRES
        await copier.migrate(target_type)


if __name__ == "__main__":
    asyncio.run(main())
