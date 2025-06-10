#!/usr/bin/env python3
"""
Spell Usage Analytics Script

Analyzes comprehensive usage statistics for all spells including:
1. Model dependency counts and lists (from dbt manifests)
2. Query usage counts (unique queries that reference each spell)
3. Execution analytics (total executions of queries that use each spell)
4. Comprehensive reporting in multiple formats
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

# Add the scripts directory to the Python path
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

# Configuration constants
DEFAULT_DAYS_BACK = 180


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


def get_spell_usage_query(days_back: int = DEFAULT_DAYS_BACK) -> str:
    """Get SQL query for comprehensive spell usage analytics."""
    return f"""
    WITH spell_query_usage AS (
        SELECT 
            at.schema_name,
            at.table_name,
            CASE 
                WHEN at.catalog_name IS NULL OR at.catalog_name = '' 
                THEN 'delta_prod' 
                ELSE at.catalog_name 
            END as catalog_name,
            COUNT(DISTINCT tqr.query_id) as unique_query_count,
            STRING_AGG(DISTINCT tqr.query_id::text, ',') as query_ids,
            COALESCE(SUM(qe.execution_count), 0) as total_executions
        FROM 
            aggregated_tables at
        LEFT JOIN 
            table_query_references tqr 
            ON concat('delta_prod', '.', at.schema_name, '.', at.table_name) = tqr.table_name
            AND tqr.referenced_at >= NOW() - INTERVAL '{days_back} days'
        LEFT JOIN (
            SELECT 
                query_id,
                COUNT(*) as execution_count
            FROM query_executions 
            WHERE created_at >= NOW() - INTERVAL '{days_back} days'
            GROUP BY query_id
        ) qe ON tqr.query_id = qe.query_id
        WHERE 
            at.table_metadata ->> 'explorer_category' = 'spell'
        GROUP BY 
            at.schema_name, 
            at.table_name,
            CASE 
                WHEN at.catalog_name IS NULL OR at.catalog_name = '' 
                THEN 'delta_prod' 
                ELSE at.catalog_name 
            END
    )
    SELECT 
        squ.catalog_name,
        squ.schema_name,
        squ.table_name,
        squ.unique_query_count,
        squ.query_ids,
        squ.total_executions,
        at.table_metadata ->> 'spell_metadata' as spell_metadata,
        at.created_at,
        at.updated_at
    FROM 
        spell_query_usage squ
    JOIN 
        aggregated_tables at 
        ON squ.schema_name = at.schema_name 
        AND squ.table_name = at.table_name
    ORDER BY 
        squ.total_executions DESC,
        squ.unique_query_count DESC,
        squ.schema_name,
        squ.table_name;
    """


class SpellUsageAnalytics:
    """Comprehensive spell usage analytics."""
    
    def __init__(self, spellbook_root: str = ".", env_file: str = ".env", days_back: int = DEFAULT_DAYS_BACK):
        self.spellbook_root = Path(spellbook_root)
        self.env_file = env_file
        self.days_back = days_back
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
    
    def load_manifests(self) -> bool:
        """Load all dbt manifests for dependency analysis."""
        print("=" * 60)
        print("Loading dbt manifests for model dependency analysis")
        print("=" * 60)
        
        # Check if manifests exist, if not try to generate them
        if not self.manifests_dir.exists():
            print("📁 Manifests directory not found, attempting to generate...")
            if not self._generate_manifests():
                return False
        
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
    
    def _generate_manifests(self) -> bool:
        """Generate manifests if they don't exist."""
        print("🔄 Generating dbt manifests...")
        os.makedirs(self.manifests_dir, exist_ok=True)
        
        success_count = 0
        for subproject in self.subprojects:
            project_dir = self.subprojects_dir / subproject
            
            if not project_dir.exists():
                print(f"⚠️  Subproject directory not found: {project_dir}")
                continue
                
            try:
                result = subprocess.run(
                    ['dbt', 'compile'], 
                    cwd=project_dir, 
                    check=True, 
                    capture_output=True, 
                    text=True
                )
                
                # Collect manifest
                manifest_path = project_dir / "target" / "manifest.json"
                if manifest_path.exists():
                    dest_path = self.manifests_dir / f"{subproject}_manifest.json"
                    shutil.copy(manifest_path, dest_path)
                    print(f"✅ Generated manifest for {subproject}")
                    success_count += 1
                    
            except subprocess.CalledProcessError as e:
                print(f"❌ Failed to generate manifest for {subproject}: {e.stderr}")
            except Exception as e:
                print(f"❌ Error processing {subproject}: {e}")
        
        return success_count > 0
    
    def query_spell_usage(self, limit: Optional[int] = None) -> List[Dict]:
        """Query database for spell usage statistics."""
        print("\n" + "=" * 60)
        print(f"STEP 1: Querying spell usage analytics (last {self.days_back} days)")
        print("=" * 60)
        
        if not self.db_query:
            print("❌ Database connection not available")
            return []
        
        try:
            query = get_spell_usage_query(self.days_back)
            print(f"🔍 Executing query for spell usage in last {self.days_back} days...")
            results = self.db_query.execute_query(query)
            
            if not results:
                print("ℹ️  No spells found in database")
                return []
            
            print(f"✅ Found {len(results)} spells")
            
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
            return None
        
        try:
            metadata = json.loads(spell_metadata)
            manifest_folder = metadata.get('manifest_folder')
            
            if manifest_folder in self.subprojects:
                return manifest_folder
            else:
                return None
                
        except (json.JSONDecodeError, TypeError):
            return None
    
    def find_models_referencing_table(self, table_name: str, schema_name: str) -> List[str]:
        """Find all dbt models that reference this table across all subprojects."""
        referencing_models = []
        full_table_name = f"{schema_name}.{table_name}"
        
        for subproject, manifest in self.manifests.items():
            # Check nodes (models) that depend on this table
            for node_id, node_info in manifest.get('nodes', {}).items():
                # Skip test nodes - only count actual model dependencies
                if node_id.startswith('test.'):
                    continue
                
                # Check if this model depends on our table
                depends_on = node_info.get('depends_on', {}).get('nodes', [])
                
                # Also check refs and sources
                refs = node_info.get('refs', [])
                sources = node_info.get('sources', [])
                
                # Check direct dependencies
                for dep in depends_on:
                    if table_name in dep or full_table_name in dep:
                        model_name = node_info.get('name', node_id)
                        referencing_models.append(f"{subproject}.{model_name}")
                        break
                
                # Check refs
                for ref in refs:
                    if isinstance(ref, list) and len(ref) > 0:
                        ref_name = ref[0] if isinstance(ref[0], str) else str(ref[0])
                        if table_name in ref_name or ref_name == table_name:
                            model_name = node_info.get('name', node_id)
                            referencing_models.append(f"{subproject}.{model_name}")
                            break
                
                # Check sources
                for source in sources:
                    if isinstance(source, list) and len(source) >= 2:
                        source_table = source[1] if isinstance(source[1], str) else str(source[1])
                        if table_name in source_table or source_table == table_name:
                            model_name = node_info.get('name', node_id)
                            referencing_models.append(f"{subproject}.{model_name}")
                            break
        
        return list(set(referencing_models))  # Remove duplicates
    
    def find_models_this_table_relies_on(self, table_name: str, schema_name: str, subproject: str) -> tuple[List[str], List[str]]:
        """Find all models/sources that this spell depends on (upstream dependencies).
        Returns tuple of (refs, sources)."""
        upstream_refs = []
        upstream_sources = []
        
        if subproject not in self.manifests:
            return upstream_refs, upstream_sources
        
        manifest = self.manifests[subproject]
        
        # Find this spell's model definition
        for node_id, node_info in manifest.get('nodes', {}).items():
            if node_info.get('name') == table_name or table_name in node_id:
                # Get what this model depends on
                depends_on = node_info.get('depends_on', {}).get('nodes', [])
                refs = node_info.get('refs', [])
                sources = node_info.get('sources', [])
                
                # Add dependencies from depends_on (these are usually refs)
                for dep in depends_on:
                    if not dep.startswith('test.'):  # Skip test nodes
                        # Try to find the model name from the dependency
                        dep_parts = dep.split('.')
                        if len(dep_parts) >= 2:
                            dep_name = dep_parts[-1]  # Get the model name
                            upstream_refs.append(dep_name)
                
                # Add refs
                for ref in refs:
                    if isinstance(ref, list) and len(ref) > 0:
                        ref_name = ref[0] if isinstance(ref[0], str) else str(ref[0])
                        upstream_refs.append(ref_name)
                
                # Add sources
                for source in sources:
                    if isinstance(source, list) and len(source) >= 2:
                        source_schema = source[0] if isinstance(source[0], str) else str(source[0])
                        source_table = source[1] if isinstance(source[1], str) else str(source[1])
                        upstream_sources.append(f"{source_schema}.{source_table}")
                
                break
        
        return list(set(upstream_refs)), list(set(upstream_sources))  # Remove duplicates
    
    def find_cross_project_source_usage(self, table_name: str, schema_name: str, subproject: str) -> List[str]:
        """Find if this spell is used as a source in other projects and which models use it."""
        cross_project_usage = []
        full_table_name = f"{schema_name}.{table_name}"
        
        for other_subproject, manifest in self.manifests.items():
            # Skip the same subproject
            if other_subproject == subproject:
                continue
                
            # Check sources in other projects
            for source_id, source_info in manifest.get('sources', {}).items():
                source_name = source_info.get('name', '')
                source_schema = source_info.get('schema', '')
                source_table_name = source_info.get('identifier', source_name)
                
                # Check if this source matches our spell
                if (source_name == table_name or 
                    source_table_name == table_name or
                    f"{source_schema}.{source_name}" == full_table_name or
                    f"{source_schema}.{source_table_name}" == full_table_name):
                    
                    # Find models in this project that use this source
                    source_key = f"{source_schema}.{source_name}"
                    models_using_source = []
                    
                    for node_id, node_info in manifest.get('nodes', {}).items():
                        if node_id.startswith('test.'):
                            continue
                            
                        node_sources = node_info.get('sources', [])
                        for src in node_sources:
                            if isinstance(src, list) and len(src) >= 2:
                                src_schema = src[0] if isinstance(src[0], str) else str(src[0])
                                src_table = src[1] if isinstance(src[1], str) else str(src[1])
                                if (src_schema == source_schema and 
                                    (src_table == source_name or src_table == source_table_name)):
                                    model_name = node_info.get('name', node_id.split('.')[-1])
                                    models_using_source.append(f"{other_subproject}.{model_name}")
                    
                    if models_using_source:
                        cross_project_usage.extend(models_using_source)
        
        return list(set(cross_project_usage))  # Remove duplicates
    
    def analyze_spell_usage(self, spell_data: List[Dict]) -> List[Dict]:
        """Analyze comprehensive usage statistics for each spell."""
        print("\n" + "=" * 60)
        print("STEP 2: Analyzing model dependencies and usage statistics")
        print("=" * 60)
        
        analyzed_spells = []
        skipped_unknown = 0
        total_spells = len(spell_data)
        
        for i, spell_info in enumerate(spell_data, 1):
            table_name = spell_info['table_name']
            schema_name = spell_info['schema_name']
            catalog_name = spell_info['catalog_name']
            spell_metadata = spell_info.get('spell_metadata', '')
            full_table_name = f"{schema_name}.{table_name}"
            
            print(f"\n📋 [{i}/{total_spells}] Analyzing: {full_table_name}")
            
            # Extract subproject
            subproject = self.extract_subproject_from_metadata(spell_metadata)
            
            # Skip spells with unknown subprojects
            if not subproject:
                print(f"   ⚠️  Skipping - Unknown subproject")
                skipped_unknown += 1
                continue
            
            # Find downstream models that reference this spell
            downstream_models = self.find_models_referencing_table(table_name, schema_name)
            downstream_count = len(downstream_models)
            
            # Find upstream models this spell relies on
            upstream_refs, upstream_sources = self.find_models_this_table_relies_on(table_name, schema_name, subproject)
            upstream_count = len(upstream_refs) + len(upstream_sources)
            
            # Find cross-project source usage
            cross_project_sources = self.find_cross_project_source_usage(table_name, schema_name, subproject)
            cross_project_count = len(cross_project_sources)
            
            # Get query and execution statistics
            unique_query_count = spell_info.get('unique_query_count', 0)
            total_executions = spell_info.get('total_executions', 0)
            query_ids = spell_info.get('query_ids', '')
            
            print(f"   📊 Subproject: {subproject}")
            print(f"   ⬇️  Downstream: {downstream_count} models depend on this")
            print(f"   ⬆️  Upstream refs: {len(upstream_refs)} models this relies on")
            print(f"   📚 Upstream sources: {len(upstream_sources)} sources this relies on")
            print(f"   🔗 Cross-project sources: {cross_project_count} models")
            print(f"   📝 Used in {unique_query_count:,} unique queries")
            print(f"   🚀 Total query executions: {total_executions:,}")
            
            analyzed_spells.append({
                'full_table_name': full_table_name,
                'schema_name': schema_name,
                'table_name': table_name,
                'catalog_name': catalog_name,
                'subproject': subproject,
                'downstream_models_count': downstream_count,
                'downstream_models': downstream_models,
                'upstream_models_count': upstream_count,
                'upstream_refs': upstream_refs,
                'upstream_sources': upstream_sources,
                'cross_project_sources_count': cross_project_count,
                'cross_project_sources': cross_project_sources,
                'unique_query_count': unique_query_count,
                'total_executions': total_executions,
                'query_ids': query_ids,
                'created_at': spell_info.get('created_at'),
                'updated_at': spell_info.get('updated_at')
            })
        
        print(f"\n📊 Analysis complete for {len(analyzed_spells)} spells")
        if skipped_unknown > 0:
            print(f"⚠️  Skipped {skipped_unknown} spells with unknown subprojects")
        return analyzed_spells
    
    def output_results(self, results: List[Dict], output_format: str = 'summary'):
        """Output results in the specified format."""
        print("\n" + "=" * 60)
        print(f"SPELL USAGE ANALYTICS - {len(results)} spells analyzed")
        print("=" * 60)
        
        if not results:
            print("No spells found.")
            return
        
        if output_format == 'csv':
            self._output_csv(results)
        elif output_format == 'json':
            self._output_json(results)
        elif output_format == 'markdown':
            self._output_markdown(results)
        else:
            self._output_summary(results)
    
    def _output_summary(self, results: List[Dict]):
        """Output results in summary format."""
        print(f"\nTop spells by usage:\n")
        
        # Sort by total executions descending
        sorted_results = sorted(results, key=lambda x: x['total_executions'], reverse=True)
        
        for i, result in enumerate(sorted_results[:20], 1):  # Show top 20
            print(f"{i:3d}. {result['full_table_name']}")
            print(f"     Subproject: {result['subproject']}")
            print(f"     Downstream models (depend on this): {result['downstream_models_count']}")
            print(f"     Upstream refs (models this relies on): {len(result['upstream_refs'])}")
            print(f"     Upstream sources (sources this relies on): {len(result['upstream_sources'])}")
            print(f"     Cross-project sources: {result['cross_project_sources_count']}")
            print(f"     Used in {result['unique_query_count']:,} queries")
            print(f"     Total executions: {result['total_executions']:,}")
            
            if result['downstream_models']:
                print(f"     Downstream: {', '.join(result['downstream_models'][:3])}")
                if len(result['downstream_models']) > 3:
                    print(f"                 ... and {len(result['downstream_models']) - 3} more")
            
            if result['upstream_refs']:
                print(f"     Upstream refs: {', '.join(result['upstream_refs'][:3])}")
                if len(result['upstream_refs']) > 3:
                    print(f"               ... and {len(result['upstream_refs']) - 3} more")
            
            if result['upstream_sources']:
                print(f"     Upstream sources: {', '.join(result['upstream_sources'][:3])}")
                if len(result['upstream_sources']) > 3:
                    print(f"               ... and {len(result['upstream_sources']) - 3} more")
            
            if result['cross_project_sources']:
                print(f"     Cross-project: {', '.join(result['cross_project_sources'][:3])}")
                if len(result['cross_project_sources']) > 3:
                    print(f"                    ... and {len(result['cross_project_sources']) - 3} more")
            print()
        
        # Summary statistics
        total_executions = sum(r['total_executions'] for r in results)
        total_queries = sum(r['unique_query_count'] for r in results)
        total_models = sum(r['downstream_models_count'] for r in results)
        
        print("📊 Summary Statistics:")
        print(f"   Total spells analyzed: {len(results)}")
        print(f"   Total query executions: {total_executions:,}")
        print(f"   Total unique queries: {total_queries:,}")
        print(f"   Total model references: {total_models:,}")
        
        # Summary by subproject
        subproject_stats = {}
        for result in results:
            subproject = result['subproject']
            if subproject not in subproject_stats:
                subproject_stats[subproject] = {'count': 0, 'executions': 0}
            subproject_stats[subproject]['count'] += 1
            subproject_stats[subproject]['executions'] += result['total_executions']
        
        print(f"\n📊 By subproject:")
        for subproject, stats in sorted(subproject_stats.items()):
            print(f"   {subproject}: {stats['count']} spells, {stats['executions']:,} executions")
    
    def _output_csv(self, results: List[Dict]):
        """Output results in CSV format."""
        fieldnames = [
            'full_table_name', 'subproject', 'downstream_models_count', 'downstream_models',
            'upstream_models_count', 'upstream_refs', 'upstream_sources', 'cross_project_sources_count', 'cross_project_sources',
            'unique_query_count', 'total_executions', 'query_ids', 'created_at', 'updated_at'
        ]
        
        filename = f"spell_usage_analytics_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        with open(filename, 'w', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            
            for result in results:
                writer.writerow({
                    'full_table_name': result['full_table_name'],
                    'subproject': result['subproject'],
                    'downstream_models_count': result['downstream_models_count'],
                    'downstream_models': ','.join(result['downstream_models']),
                    'upstream_models_count': result['upstream_models_count'],
                    'upstream_refs': ','.join(result['upstream_refs']),
                    'upstream_sources': ','.join(result['upstream_sources']),
                    'cross_project_sources_count': result['cross_project_sources_count'],
                    'cross_project_sources': ','.join(result['cross_project_sources']),
                    'unique_query_count': result['unique_query_count'],
                    'total_executions': result['total_executions'],
                    'query_ids': result.get('query_ids', ''),
                    'created_at': result.get('created_at', ''),
                    'updated_at': result.get('updated_at', '')
                })
        
        print(f"✅ Results exported to: {filename}")
    
    def _output_json(self, results: List[Dict]):
        """Output results in JSON format."""
        filename = f"spell_usage_analytics_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        
        # Process results to convert string arrays properly
        processed_results = []
        for result in results:
            processed_result = result.copy()
            
            # Ensure downstream_models is properly formatted
            if isinstance(result['downstream_models'], list):
                processed_result['downstream_models'] = result['downstream_models']
            else:
                processed_result['downstream_models'] = []
            
            # Ensure upstream_refs is properly formatted
            if isinstance(result['upstream_refs'], list):
                processed_result['upstream_refs'] = result['upstream_refs']
            else:
                processed_result['upstream_refs'] = []
            
            # Ensure upstream_sources is properly formatted
            if isinstance(result['upstream_sources'], list):
                processed_result['upstream_sources'] = result['upstream_sources']
            else:
                processed_result['upstream_sources'] = []
            
            # Ensure cross_project_sources is properly formatted
            if isinstance(result['cross_project_sources'], list):
                processed_result['cross_project_sources'] = result['cross_project_sources']
            else:
                processed_result['cross_project_sources'] = []
            
            # Split query_ids into array
            if result.get('query_ids'):
                processed_result['query_ids'] = [qid.strip() for qid in result['query_ids'].split(',') if qid.strip()]
            else:
                processed_result['query_ids'] = []
                
            processed_results.append(processed_result)
        
        with open(filename, 'w') as jsonfile:
            json.dump(processed_results, jsonfile, indent=2, default=str)
        
        print(f"✅ Results exported to: {filename}")
    
    def _output_markdown(self, results: List[Dict]):
        """Output results in Markdown format."""
        filename = f"spell_usage_analytics_{datetime.now().strftime('%Y%m%d_%H%M%S')}.md"
        
        # Sort by total executions descending
        sorted_results = sorted(results, key=lambda x: x['total_executions'], reverse=True)
        
        with open(filename, 'w') as mdfile:
            mdfile.write(f"# Spell Usage Analytics\n\n")
            mdfile.write(f"Analysis of {len(results)} spells over the last {self.days_back} days\n\n")
            mdfile.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
            
            # Summary statistics
            total_executions = sum(r['total_executions'] for r in results)
            total_queries = sum(r['unique_query_count'] for r in results)
            total_models = sum(r['downstream_models_count'] for r in results)
            
            mdfile.write("## Summary Statistics\n\n")
            mdfile.write(f"- **Total spells analyzed**: {len(results):,}\n")
            mdfile.write(f"- **Total query executions**: {total_executions:,}\n")
            mdfile.write(f"- **Total unique queries**: {total_queries:,}\n")
            mdfile.write(f"- **Total model references**: {total_models:,}\n\n")
            
            # Top spells table
            mdfile.write("## Top Spells by Usage\n\n")
            mdfile.write("| Rank | Spell | Subproject | Downstream Models | Queries | Executions |\n")
            mdfile.write("|------|-------|------------|-------------------|---------|------------|\n")
            
            for i, result in enumerate(sorted_results[:50], 1):  # Top 50
                mdfile.write(f"| {i} | `{result['full_table_name']}` | {result['subproject']} | {result['downstream_models_count']} | {result['unique_query_count']:,} | {result['total_executions']:,} |\n")
            
            mdfile.write("\n## Detailed Analysis\n\n")
            
            # Detailed breakdown for top spells
            for i, result in enumerate(sorted_results, 1):  # Show ALL spells, not just top 20
                mdfile.write(f"### {i}. {result['full_table_name']}\n\n")
                mdfile.write(f"- **Subproject**: {result['subproject']}\n")
                mdfile.write(f"- **Downstream models** (depend on this): {result['downstream_models_count']}\n")
                mdfile.write(f"- **Upstream refs** (models this relies on): {len(result['upstream_refs'])}\n")
                mdfile.write(f"- **Upstream sources** (sources this relies on): {len(result['upstream_sources'])}\n")
                mdfile.write(f"- **Cross-project sources**: {result['cross_project_sources_count']}\n")
                mdfile.write(f"- **Used in queries**: {result['unique_query_count']:,}\n")
                mdfile.write(f"- **Total executions**: {result['total_executions']:,}\n")
                
                if result['downstream_models']:
                    mdfile.write(f"\n**Downstream Models** (models that depend on this spell):\n")
                    for model in result['downstream_models'][:10]:  # Show up to 10
                        mdfile.write(f"- `{model}`\n")
                    if len(result['downstream_models']) > 10:
                        mdfile.write(f"- ... and {len(result['downstream_models']) - 10} more\n")
                
                if result['upstream_refs']:
                    mdfile.write(f"\n**Upstream refs** (models this spell relies on):\n")
                    for model in result['upstream_refs'][:10]:  # Show up to 10
                        mdfile.write(f"- `{model}`\n")
                    if len(result['upstream_refs']) > 10:
                        mdfile.write(f"- ... and {len(result['upstream_refs']) - 10} more\n")
                
                if result['upstream_sources']:
                    mdfile.write(f"\n**Upstream sources** (models this spell relies on):\n")
                    for model in result['upstream_sources'][:10]:  # Show up to 10
                        mdfile.write(f"- `{model}`\n")
                    if len(result['upstream_sources']) > 10:
                        mdfile.write(f"- ... and {len(result['upstream_sources']) - 10} more\n")
                
                if result['cross_project_sources']:
                    mdfile.write(f"\n**Cross-Project Sources** (used as source in other projects):\n")
                    for model in result['cross_project_sources'][:10]:  # Show up to 10
                        mdfile.write(f"- `{model}`\n")
                    if len(result['cross_project_sources']) > 10:
                        mdfile.write(f"- ... and {len(result['cross_project_sources']) - 10} more\n")
                
                mdfile.write("\n---\n\n")
        
        print(f"✅ Results exported to: {filename}")
    
    def run_analytics(self, limit: Optional[int] = None, output_format: str = 'summary') -> bool:
        """Run the complete spell usage analytics workflow."""
        print(f"📊 Spell Usage Analytics - Analyzing spell usage over last {self.days_back} days")
        print("=" * 80)
        
        # Setup database connection
        if not self.setup_database_connection():
            return False
        
        # Load manifests for model dependency analysis
        if not self.load_manifests():
            print("❌ Failed to load manifests - model analysis will be limited")
        
        # Query spell usage
        spell_data = self.query_spell_usage(limit)
        if not spell_data:
            print("ℹ️  No spells found to analyze")
            return True
        
        # Analyze usage
        analyzed_spells = self.analyze_spell_usage(spell_data)
        
        # Output results
        self.output_results(analyzed_spells, output_format)
        
        print("\n✅ Analytics completed successfully!")
        return True


def main():
    """Main entry point for the spell usage analytics."""
    parser = argparse.ArgumentParser(description='Comprehensive spell usage analytics')
    parser.add_argument('--limit', type=int, help='Limit number of spells to analyze')
    parser.add_argument('--format', choices=['summary', 'csv', 'json', 'markdown'], 
                       default='summary', help='Output format')
    parser.add_argument('--env-file', default='.env', help='Environment file path')
    parser.add_argument('--days-back', type=int, default=DEFAULT_DAYS_BACK,
                       help=f'Number of days back to analyze (default: {DEFAULT_DAYS_BACK})')
    
    args = parser.parse_args()
    
    analytics = SpellUsageAnalytics(
        env_file=args.env_file,
        days_back=args.days_back
    )
    success = analytics.run_analytics(
        limit=args.limit,
        output_format=args.format
    )
    
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main() 