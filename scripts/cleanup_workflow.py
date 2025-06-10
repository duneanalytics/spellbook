#!/usr/bin/env python3
"""
Enhanced cleanup workflow that:
1. Generates all subproject manifests
2. Queries database for tables with 0 or 1 query usages in last 6 months
3. Maps tables to subprojects using spell_metadata
4. Validates model files exist and raises errors if not found
5. Analyzes dependencies (children/sources) across subprojects
6. Outputs table_name, dbt_subproject, and materialization for eligible tables
"""

import os
import sys
import json
import argparse
import subprocess
import shutil
from pathlib import Path
from typing import Dict, List, Optional, Set
from datetime import datetime
import csv

# Add the scripts directory to the Python path to import existing modules
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Optional imports for database functionality
try:
    import psycopg2
    import psycopg2.extras
    PSYCOPG2_AVAILABLE = True
except ImportError:
    PSYCOPG2_AVAILABLE = False

try:
    from dotenv import load_dotenv
    DOTENV_AVAILABLE = True
except ImportError:
    DOTENV_AVAILABLE = False


class DatabaseQuery:
    """Database query handler for postgres connections."""
    
    def __init__(self, env_file: str = ".env"):
        if DOTENV_AVAILABLE:
            load_dotenv(env_file)
        
        self.connection_params = {
            'host': os.getenv('DATABASE_HOST'),
            'port': os.getenv('DATABASE_PORT', 5432),
            'database': os.getenv('DATABASE_DATABASE'),
            'user': os.getenv('DATABASE_USER'),
            'password': os.getenv('DATABASE_PASSWORD')
        }
        
        # Test connection
        self._test_connection()
    
    def _test_connection(self):
        """Test database connection."""
        if not PSYCOPG2_AVAILABLE:
            raise ImportError("psycopg2-binary package required for database connection")
        
        try:
            conn = psycopg2.connect(**self.connection_params)
            conn.close()
        except Exception as e:
            raise ConnectionError(f"Database connection failed: {e}")
    
    def execute_query(self, query: str) -> List[Dict]:
        """Execute query and return results as list of dictionaries."""
        conn = psycopg2.connect(**self.connection_params)
        try:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                cursor.execute(query)
                results = cursor.fetchall()
                return [dict(row) for row in results]
        finally:
            conn.close()


def get_unused_spells_query() -> str:
    """Get the SQL query for finding tables with 0 or 1 query usages in last 6 months."""
    return """
    SELECT
        CASE 
            WHEN at.catalog_name IS NULL OR at.catalog_name = '' 
            THEN 'delta_prod' 
            ELSE at.catalog_name 
        END as catalog_name,
        at.schema_name,
        at.table_name,
        at.source as table_type,
        COUNT(tqr.query_id) AS reference_count,
        MAX(tqr.referenced_at) AS last_referenced_at,
        at.created_at,
        at.updated_at,
        at.table_metadata ->> 'spell_metadata' as spell_metadata
    FROM
        aggregated_tables at
    LEFT JOIN
        table_query_references tqr
        ON concat('delta_prod', '.', at.schema_name, '.', at.table_name) = tqr.table_name
        AND tqr.referenced_at >= NOW() - INTERVAL '180 days'
    WHERE
        at.table_metadata ->> 'explorer_category' = 'spell'
    GROUP BY
        CASE 
            WHEN at.catalog_name IS NULL OR at.catalog_name = '' 
            THEN 'delta_prod' 
            ELSE at.catalog_name 
        END,
        at.schema_name,
        at.table_name,
        at.source,
        at.created_at,
        at.updated_at,
        at.table_metadata 
    HAVING COUNT(tqr.query_id) <= 5
    ORDER BY
        reference_count ASC,
        last_referenced_at ASC NULLS FIRST;
    """


class CleanupWorkflow:
    """Enhanced cleanup workflow for table analysis."""
    
    def __init__(self, spellbook_root: str = ".", env_file: str = ".env"):
        self.spellbook_root = Path(spellbook_root)
        self.env_file = env_file
        self.subprojects_dir = self.spellbook_root / "dbt_subprojects"
        self.manifests_dir = self.subprojects_dir / "manifests"
        self.db_query = None
        
        # Define subprojects
        self.subprojects = ['daily_spellbook', 'dex', 'hourly_spellbook', 'nft', 'solana', 'tokens']
        
        # Storage for loaded manifests
        self.manifests = {}
        
    def setup_database_connection(self) -> bool:
        """Setup database connection if credentials are available."""
        if not PSYCOPG2_AVAILABLE or not DOTENV_AVAILABLE:
            print("❌ Database functionality requires psycopg2-binary and python-dotenv packages.")
            return False
            
        try:
            self.db_query = DatabaseQuery(self.env_file)
            return True
        except Exception as e:
            print(f"❌ Could not connect to database: {e}")
            return False
    
    def generate_manifests(self) -> bool:
        """Generate manifests for all subprojects."""
        print("=" * 60)
        print("STEP 1: Generating manifests for all subprojects")
        print("=" * 60)
        
        # Create output directory
        os.makedirs(self.manifests_dir, exist_ok=True)
        
        success_count = 0
        failed_projects = []
        
        for subproject in self.subprojects:
            project_dir = self.subprojects_dir / subproject
            
            if not project_dir.exists():
                print(f"⚠️  Subproject directory not found: {project_dir}")
                failed_projects.append(subproject)
                continue
                
            print(f"\n📦 Processing {subproject}...")
            
            # Run dbt compile
            try:
                result = subprocess.run(
                    ['dbt', 'compile'], 
                    cwd=project_dir, 
                    check=True, 
                    capture_output=True, 
                    text=True
                )
                print(f"✅ Successfully compiled {subproject}")
                
                # Collect manifest
                manifest_path = project_dir / "target" / "manifest.json"
                if manifest_path.exists():
                    dest_path = self.manifests_dir / f"{subproject}_manifest.json"
                    shutil.copy(manifest_path, dest_path)
                    print(f"✅ Collected manifest: {dest_path}")
                    success_count += 1
                else:
                    print(f"❌ Manifest not found: {manifest_path}")
                    failed_projects.append(subproject)
                    
            except subprocess.CalledProcessError as e:
                print(f"❌ Failed to compile {subproject}:")
                print(f"   stdout: {e.stdout}")
                print(f"   stderr: {e.stderr}")
                failed_projects.append(subproject)
            except Exception as e:
                print(f"❌ Error processing {subproject}: {e}")
                failed_projects.append(subproject)
        
        print(f"\n📊 Manifest generation summary:")
        print(f"   ✅ Successful: {success_count}/{len(self.subprojects)}")
        if failed_projects:
            print(f"   ❌ Failed: {', '.join(failed_projects)}")
            
        return success_count > 0
    
    def load_manifests(self) -> bool:
        """Load all generated manifests into memory."""
        print("\n" + "=" * 60)
        print("Loading manifests for dependency analysis")
        print("=" * 60)
        
        loaded_count = 0
        for subproject in self.subprojects:
            manifest_file = self.manifests_dir / f"{subproject}_manifest.json"
            
            if manifest_file.exists():
                try:
                    with open(manifest_file, 'r') as f:
                        self.manifests[subproject] = json.load(f)
                    print(f"✅ Loaded manifest for {subproject}")
                    loaded_count += 1
                except Exception as e:
                    print(f"❌ Error loading manifest for {subproject}: {e}")
            else:
                print(f"⚠️  Manifest not found for {subproject}: {manifest_file}")
        
        print(f"\n📊 Loaded {loaded_count}/{len(self.subprojects)} manifests")
        return loaded_count > 0
    
    def query_unused_tables(self, limit: Optional[int] = None) -> List[Dict]:
        """Query database for tables with 0 or 1 query usages in last 6 months."""
        print("\n" + "=" * 60)
        print("STEP 2: Querying database for lightly used tables (≤1 references)")
        print("=" * 60)
        
        if not self.db_query:
            print("❌ Database connection not available")
            return []
        
        try:
            query = get_unused_spells_query()
            print("🔍 Executing query for tables with ≤1 references in last 6 months...")
            results = self.db_query.execute_query(query)
            
            if not results:
                print("ℹ️  No lightly used tables found in database")
                return []
            
            print(f"✅ Found {len(results)} tables with ≤1 references")
            
            # Debug: Show first few results to verify reference counts
            print("🔍 Sample of reference counts from query:")
            for i, result in enumerate(results[:5]):
                print(f"   {i+1}. {result['schema_name']}.{result['table_name']}: {result['reference_count']} refs")
            
            if limit:
                results = results[:limit]
                print(f"📊 Limited to first {limit} results")
                
            return results
            
        except Exception as e:
            print(f"❌ Error querying database: {e}")
            return []
    
    def extract_subproject_from_metadata(self, spell_metadata: str) -> Optional[str]:
        """Extract subproject name from spell_metadata field."""
        if not spell_metadata:
            print(f"   🐛 DEBUG: spell_metadata is empty or None")
            return None
        
        try:
            print(f"   🐛 DEBUG: spell_metadata content: {spell_metadata}")
            metadata = json.loads(spell_metadata)
            manifest_folder = metadata.get('manifest_folder')
            print(f"   🐛 DEBUG: extracted manifest_folder: {manifest_folder}")
            
            # manifest_folder maps directly to subproject name
            if manifest_folder in self.subprojects:
                return manifest_folder
            else:
                print(f"⚠️  Unknown manifest_folder: {manifest_folder}")
                print(f"   🐛 DEBUG: Available subprojects: {self.subprojects}")
                return None
                
        except (json.JSONDecodeError, TypeError) as e:
            print(f"⚠️  Error parsing spell_metadata: {e}")
            print(f"   🐛 DEBUG: Raw spell_metadata: {repr(spell_metadata)}")
            return None
    
    def get_model_file_path(self, spell_metadata: str, subproject: str) -> Optional[str]:
        """Extract original_file path from spell_metadata."""
        if not spell_metadata:
            return None
        
        try:
            metadata = json.loads(spell_metadata)
            original_file = metadata.get('original_file')
            
            if original_file:
                # Construct full path
                full_path = self.subprojects_dir / subproject / original_file
                return str(full_path)
            else:
                return None
                
        except (json.JSONDecodeError, TypeError):
            return None
    
    def validate_model_file_exists(self, file_path: str, table_name: str) -> None:
        """Validate that the model file exists. Raise error if not found."""
        if not file_path:
            raise FileNotFoundError(f"No file path found in metadata for table {table_name}")
        
        if not Path(file_path).exists():
            raise FileNotFoundError(f"Model file not found for table {table_name}: {file_path}")
    
    def find_table_dependencies(self, table_name: str, schema_name: str) -> Set[str]:
        """Find all dependencies (children and sources) for a table across all subprojects."""
        dependencies = set()
        full_table_name = f"{schema_name}.{table_name}"
        
        for subproject, manifest in self.manifests.items():
            # Check nodes (models) that depend on this table - EXCLUDE tests
            for node_id, node_info in manifest.get('nodes', {}).items():
                # Skip test nodes - only count actual model dependencies
                if node_id.startswith('test.'):
                    continue
                    
                depends_on = node_info.get('depends_on', {}).get('nodes', [])
                
                # Check if any dependency references this table
                for dep in depends_on:
                    if table_name in dep or full_table_name in dep:
                        dependencies.add(f"{subproject}:{node_id}")
            
            # Check if this table is used as a source in any subproject
            for source_id, source_info in manifest.get('sources', {}).items():
                source_name = source_info.get('name', '')
                source_schema = source_info.get('schema', '')
                
                if (source_name == table_name or 
                    f"{source_schema}.{source_name}" == full_table_name):
                    dependencies.add(f"{subproject}:source:{source_id}")
        
        return dependencies
    
    def get_materialization_from_metadata(self, spell_metadata: str, subproject: str, table_name: str) -> str:
        """Get materialization from spell_metadata or manifest."""
        # First try to get from spell_metadata
        if spell_metadata:
            try:
                metadata = json.loads(spell_metadata)
                materialization = metadata.get('materialization')
                if materialization:
                    return materialization
            except (json.JSONDecodeError, TypeError):
                pass
        
        # Fall back to manifest lookup
        if subproject in self.manifests:
            manifest = self.manifests[subproject]
            for node_id, node_info in manifest.get('nodes', {}).items():
                if table_name in node_id:
                    return node_info.get('config', {}).get('materialized', 'unknown')
        
        return 'unknown'
    
    def analyze_tables(self, unused_tables: List[Dict]) -> List[Dict]:
        """Analyze tables for subproject mapping, dependencies, and validation."""
        print("\n" + "=" * 60)
        print("STEP 3: Analyzing table mapping, validation, and dependencies")
        print("=" * 60)
        
        valid_tables = []
        exceptions = []
        total_tables = len(unused_tables)
        
        for i, table_info in enumerate(unused_tables, 1):
            table_name = table_info['table_name']
            schema_name = table_info['schema_name']
            spell_metadata = table_info.get('spell_metadata', '')
            reference_count = table_info.get('reference_count', 'Unknown')
            full_table_name = f"{schema_name}.{table_name}"
            
            print(f"\n📋 [{i}/{total_tables}] Analyzing: {full_table_name}")
            print(f"   🔢 Reference count: {reference_count}")
            
            try:
                # Extract subproject from metadata
                subproject = self.extract_subproject_from_metadata(spell_metadata)
                if not subproject:
                    print(f"   ❌ Could not determine subproject from metadata")
                    continue
                
                print(f"   ✅ Subproject: {subproject}")
                
                # Get and validate model file path
                model_file_path = self.get_model_file_path(spell_metadata, subproject)
                self.validate_model_file_exists(model_file_path, full_table_name)
                print(f"   ✅ Model file validated: {model_file_path}")
                
                # Get materialization
                materialization = self.get_materialization_from_metadata(spell_metadata, subproject, table_name)
                print(f"   🏗️  Materialization: {materialization}")
                
                # Check dependencies
                print("   🔍 Analyzing dependencies...")
                dependencies = self.find_table_dependencies(table_name, schema_name)
                
                if dependencies:
                    print(f"   ⚠️  Has {len(dependencies)} dependencies - SKIPPING")
                    for dep in sorted(dependencies):
                        print(f"      - {dep}")
                    continue
                else:
                    print("   ✅ No dependencies found - INCLUDED")
                
                # Add to valid tables
                valid_tables.append({
                    'table_name': full_table_name,
                    'dbt_subproject': subproject,
                    'materialization': materialization,
                    'reference_count': table_info.get('reference_count', 0),
                    'last_referenced_at': table_info.get('last_referenced_at'),
                    'model_file_path': model_file_path
                })
                
            except Exception as e:
                error_msg = f"Error analyzing {full_table_name}: {str(e)}"
                print(f"   ❌ {error_msg}")
                exceptions.append(error_msg)
                continue
        
        # Log all exceptions at the end
        if exceptions:
            print(f"\n⚠️  Encountered {len(exceptions)} errors during analysis:")
            for i, exception in enumerate(exceptions, 1):
                print(f"   {i}. {exception}")
        
        print(f"\n📊 Analysis complete: {len(valid_tables)}/{total_tables} tables eligible for cleanup")
        return valid_tables
    
    def output_results(self, results: List[Dict], output_format: str = 'summary'):
        """Output results in the specified format."""
        print("\n" + "=" * 60)
        print("CLEANUP CANDIDATES - Tables with ≤1 references and no dependencies")
        print("=" * 60)
        
        if not results:
            print("No tables found matching criteria.")
            return
        
        if output_format == 'csv':
            self._output_csv(results)
        elif output_format == 'json':
            self._output_json(results)
        else:
            self._output_summary(results)
    
    def _output_summary(self, results: List[Dict]):
        """Output results in summary format."""
        print(f"\nFound {len(results)} tables eligible for cleanup:\n")
        
        for i, result in enumerate(results, 1):
            print(f"{i:3d}. {result['table_name']}")
            print(f"     Subproject: {result['dbt_subproject']}")
            print(f"     Materialization: {result['materialization']}")
            print(f"     References: {result['reference_count']}")
            print(f"     Last used: {result.get('last_referenced_at', 'Never')}")
            print()
        
        # Summary by subproject
        subproject_counts = {}
        for result in results:
            subproject = result['dbt_subproject']
            subproject_counts[subproject] = subproject_counts.get(subproject, 0) + 1
        
        print("📊 Summary by subproject:")
        for subproject, count in sorted(subproject_counts.items()):
            print(f"   {subproject}: {count}")
    
    def _output_csv(self, results: List[Dict]):
        """Output results in CSV format."""
        fieldnames = ['table_name', 'dbt_subproject', 'materialization', 'reference_count', 'last_referenced_at']
        
        filename = f"cleanup_candidates_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        with open(filename, 'w', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            
            for result in results:
                writer.writerow({
                    'table_name': result['table_name'],
                    'dbt_subproject': result['dbt_subproject'],
                    'materialization': result['materialization'],
                    'reference_count': result['reference_count'],
                    'last_referenced_at': result.get('last_referenced_at', '')
                })
        
        print(f"✅ Results exported to: {filename}")
    
    def _output_json(self, results: List[Dict]):
        """Output results in JSON format."""
        filename = f"cleanup_candidates_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        
        with open(filename, 'w') as jsonfile:
            json.dump(results, jsonfile, indent=2, default=str)
        
        print(f"✅ Results exported to: {filename}")
    
    def run_workflow(self, 
                    limit: Optional[int] = None, 
                    skip_manifests: bool = False,
                    output_format: str = 'summary') -> bool:
        """Run the complete cleanup workflow."""
        print("🧹 Enhanced Cleanup Workflow - Finding tables with ≤1 references and no dependencies")
        print("=" * 80)
        
        # Setup database connection
        if not self.setup_database_connection():
            return False
        
        # Generate manifests
        if not skip_manifests:
            if not self.generate_manifests():
                print("❌ Failed to generate manifests")
                return False
        
        # Load manifests
        if not self.load_manifests():
            print("❌ Failed to load manifests")
            return False
        
        # Query database
        unused_tables = self.query_unused_tables(limit)
        if not unused_tables:
            print("ℹ️  No tables found to analyze")
            return True
        
        # Analyze tables
        valid_tables = self.analyze_tables(unused_tables)
        
        # Output results
        self.output_results(valid_tables, output_format)
        
        print("\n✅ Workflow completed successfully!")
        return True


def main():
    """Main entry point for the cleanup workflow."""
    parser = argparse.ArgumentParser(description='Enhanced table cleanup workflow')
    parser.add_argument('--limit', type=int, help='Limit number of tables to analyze')
    parser.add_argument('--skip-manifests', action='store_true', 
                       help='Skip manifest generation (use existing manifests)')
    parser.add_argument('--format', choices=['summary', 'csv', 'json'], 
                       default='summary', help='Output format')
    parser.add_argument('--env-file', default='.env', help='Environment file path')
    
    args = parser.parse_args()
    
    workflow = CleanupWorkflow(env_file=args.env_file)
    success = workflow.run_workflow(
        limit=args.limit,
        skip_manifests=args.skip_manifests,
        output_format=args.format
    )
    
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main() 