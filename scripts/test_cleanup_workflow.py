#!/usr/bin/env python3
"""
Simple test script to find models safe to remove.
"""

import json
import os
import subprocess
from pathlib import Path

def test_database_connection():
    """Test if we can connect to the database."""
    print("🔧 Testing database connection...")
    
    # Check .env file
    if not os.path.exists('.env'):
        print("❌ .env file not found")
        return False
    
    # Load environment variables
    from dotenv import load_dotenv
    load_dotenv()
    
    required_vars = ['DATABASE_HOST', 'DATABASE_PORT', 'DATABASE_DATABASE', 'DATABASE_USER', 'DATABASE_PASSWORD']
    missing = [var for var in required_vars if not os.getenv(var)]
    
    if missing:
        print(f"❌ Missing environment variables: {missing}")
        return False
    
    print("✅ Database configuration looks good")
    return True

def collect_manifests():
    """Collect manifest files from all subprojects."""
    print("\n📦 Collecting manifest files...")
    
    manifests_dir = Path("dbt_subprojects/manifests")
    
    # Check if manifests already exist and are recent
    if manifests_dir.exists():
        manifest_files = list(manifests_dir.glob("*_manifest.json"))
        if manifest_files:
            # Check if manifests are recent (less than 1 hour old)
            import time
            newest = max(f.stat().st_mtime for f in manifest_files)
            if time.time() - newest < 3600:  # 1 hour
                print(f"✅ Found {len(manifest_files)} recent manifest files")
                return True
    
    # Need to collect manifests
    print("Collecting fresh manifest files...")
    
    try:
        result = subprocess.run(
            ['python', 'scripts/collect_manifests.py'],
            capture_output=True,
            text=True,
            check=True
        )
        print("✅ Manifest collection completed")
        print(result.stdout)
        return True
        
    except subprocess.CalledProcessError as e:
        print(f"❌ Failed to collect manifests: {e}")
        print(f"STDOUT: {e.stdout}")
        print(f"STDERR: {e.stderr}")
        return False
    except FileNotFoundError:
        print("❌ scripts/collect_manifests.py not found")
        return False

def test_database_query():
    """Test the database query for unused models."""
    print("\n📊 Testing database query...")
    
    try:
        # Import after we know the dependencies are available
        from find_table_subproject import DatabaseQuery, get_unused_spells_query
        
        db = DatabaseQuery('.env')
        
        # Modify query to include barely used models (≤1 references)
        query = get_unused_spells_query().replace("= 0", "<= 1")
        
        print("Running query (limit 5 for testing)...")
        query_with_limit = query.replace("ORDER BY", "ORDER BY") + " LIMIT 5"
        
        results = db.execute_query(query_with_limit)
        
        print(f"✅ Query successful, found {len(results)} results")
        
        if results:
            print("\nSample results:")
            for i, result in enumerate(results[:3]):
                print(f"  {i+1}. {result.get('schema_name', 'unknown')}.{result.get('table_name', 'unknown')}")
                print(f"     References: {result.get('reference_count', 0)}")
                print(f"     Last used: {result.get('last_referenced_at', 'Never')}")
        
        return results
        
    except Exception as e:
        print(f"❌ Database query failed: {e}")
        return None

def test_dependency_check(sample_table):
    """Test dependency checking using manifest files."""
    print(f"\n🔍 Testing dependency check for sample table...")
    
    try:
        from find_table_subproject import TableSubprojectFinder
        
        finder = TableSubprojectFinder()
        
        # Test with a sample table name
        table_name = f"{sample_table.get('schema_name', 'test')}.{sample_table.get('table_name', 'test')}"
        
        print(f"Checking dependencies for: {table_name}")
        
        # This will use the current (complex) dependency finding
        dependencies = finder.find_dependencies(table_name)
        
        print(f"✅ Found {len(dependencies)} dependencies")
        
        if dependencies:
            print("Sample dependencies:")
            for dep in dependencies[:3]:
                print(f"  - {dep.get('dependent_table', 'unknown')} ({dep.get('match_type', 'unknown')})")
        
        return len(dependencies) == 0  # Safe to remove if no dependencies
        
    except Exception as e:
        print(f"❌ Dependency check failed: {e}")
        return False

def main():
    print("🧪 Testing Cleanup Workflow")
    print("=" * 30)
    
    # Test 1: Database connection
    if not test_database_connection():
        print("\n❌ Database connection test failed")
        return
    
    # Test 2: Collect manifest files
    if not collect_manifests():
        print("\n❌ Manifest collection failed")
        return
    
    # Test 3: Database query
    results = test_database_query()
    if results is None:
        print("\n❌ Database query test failed")
        return
    
    # Test 4: Dependency check (if we have results)
    if results:
        sample_table = results[0]
        is_safe = test_dependency_check(sample_table)
        
        if is_safe:
            print(f"\n✅ Sample table appears safe to remove")
        else:
            print(f"\n⚠️ Sample table has dependencies")
    
    print("\n🎉 All tests completed!")
    print("\nReady to run full cleanup analysis!")

if __name__ == "__main__":
    main() 