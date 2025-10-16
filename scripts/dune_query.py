#!/usr/bin/env python3
"""
🌶️ Generalized Dune query utility using spice

This script provides a simple interface to query Dune and test results.
Can be used as a CLI tool or imported as a Python module.

See scripts/DUNE_TESTING.md for full documentation.

Examples:
    # Run a saved query by ID
    python scripts/dune_query.py 21693
    
    # Run a saved query by URL
    python scripts/dune_query.py https://dune.com/queries/21693
    
    # Run raw SQL
    python scripts/dune_query.py "SELECT * FROM dex_solana.trades LIMIT 5"
    
    # Run SQL from a file
    python scripts/dune_query.py --sql-file path/to/query.sql
    
    # Compile and run a dbt model
    python scripts/dune_query.py "@uniswap_v3_unichain_base_trades"
    
    # Force refresh execution
    python scripts/dune_query.py 21693 --refresh
    
    # Use query parameters
    python scripts/dune_query.py 21693 --params network=ethereum version=5
    
    # Save results to file
    python scripts/dune_query.py 21693 --output results.csv
    
    # Import and use in Python
    from scripts.dune_query import run_query
    df = run_query("SELECT * FROM ethereum.blocks LIMIT 5")
    df = run_query("@uniswap_v3_unichain_base_trades")
"""

import os
import sys
import argparse
import subprocess
import tempfile
from pathlib import Path
from typing import Any, Dict, Optional, Union
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

try:
    import spice
    import polars as pl
except ImportError:
    print("Error: Required packages not installed.")
    print("Install with: pip install dune_spice polars")
    sys.exit(1)


def run_query(
    query: Union[str, int],
    *,
    refresh: bool = False,
    parameters: Optional[Dict[str, Any]] = None,
    api_key: Optional[str] = None,
    performance: str = 'medium',
    verbose: bool = True,
    limit: Optional[int] = None,
    cache: bool = True,
    compile_dbt: bool = False,
    project_dir: Optional[str] = None,
    profiles_dir: Optional[str] = None,
    target: Optional[str] = None,
) -> pl.DataFrame:
    """
    Execute a Dune query and return results as a polars DataFrame.
    
    Args:
        query: Query ID, URL, raw SQL string, or dbt model name (with @ prefix)
        refresh: Force new execution instead of using cached results
        parameters: Dict of query parameters (e.g. {'network': 'ethereum'})
        api_key: Dune API key (uses DUNE_API_KEY env var if not provided)
        performance: Query performance tier ('medium', 'large')
        verbose: Print execution details
        limit: Max number of rows to return
        cache: Use local caching for results
        compile_dbt: Whether to compile dbt model before execution
        project_dir: dbt project directory (for dbt compilation)
        profiles_dir: dbt profiles directory (for dbt compilation)
        target: dbt target (for dbt compilation)
        
    Returns:
        polars DataFrame with query results
    """
    if api_key is None:
        api_key = os.getenv('DUNE_API_KEY')
        if not api_key:
            raise ValueError(
                "No Dune API key found. Set DUNE_API_KEY environment variable "
                "or pass api_key parameter."
            )
    
    # Check if this is a dbt model (starts with @)
    if isinstance(query, str) and query.startswith('@'):
        compile_dbt = True
        query = query[1:]  # Remove @ prefix
    
    # Compile dbt model if requested
    if compile_dbt and isinstance(query, str):
        query = compile_dbt_model(
            query,
            project_dir=project_dir,
            profiles_dir=profiles_dir,
            target=target,
            verbose=verbose,
        )
    
    try:
        df = spice.query(
            query,
            refresh=refresh,
            parameters=parameters,
            api_key=api_key,
            performance=performance,
            verbose=verbose,
            limit=limit,
            cache=cache,
        )
        return df
    except Exception as e:
        print(f"Error executing query: {e}", file=sys.stderr)
        raise


def compare_dataframes(
    df1: pl.DataFrame,
    df2: pl.DataFrame,
    label1: str = "df1",
    label2: str = "df2",
) -> None:
    """
    Compare two dataframes and print differences.
    
    Useful for comparing test results vs production results.
    """
    print(f"\n{'='*60}")
    print(f"COMPARISON: {label1} vs {label2}")
    print(f"{'='*60}")
    
    print(f"\n{label1} shape: {df1.shape}")
    print(f"{label2} shape: {df2.shape}")
    
    # Compare row counts
    diff = df1.shape[0] - df2.shape[0]
    if diff > 0:
        print(f"\n✅ {label1} has {diff} MORE rows than {label2}")
    elif diff < 0:
        print(f"\n⚠️  {label1} has {abs(diff)} FEWER rows than {label2}")
    else:
        print(f"\n✓ Both have same number of rows")
    
    # Compare columns
    cols1 = set(df1.columns)
    cols2 = set(df2.columns)
    
    if cols1 == cols2:
        print(f"✓ Both have same columns")
    else:
        only_in_1 = cols1 - cols2
        only_in_2 = cols2 - cols1
        if only_in_1:
            print(f"\nColumns only in {label1}: {only_in_1}")
        if only_in_2:
            print(f"Columns only in {label2}: {only_in_2}")
    
    # Preview both
    print(f"\n{label1} preview:")
    print(df1.head(5))
    print(f"\n{label2} preview:")
    print(df2.head(5))


def save_results(
    df: pl.DataFrame,
    output_path: str,
    format: Optional[str] = None,
) -> None:
    """
    Save DataFrame to file. Format inferred from extension if not provided.
    
    Supported formats: csv, parquet, json, xlsx
    """
    path = Path(output_path)
    
    if format is None:
        format = path.suffix.lstrip('.')
    
    format = format.lower()
    
    if format == 'csv':
        df.write_csv(output_path)
    elif format == 'parquet':
        df.write_parquet(output_path)
    elif format == 'json':
        df.write_json(output_path)
    elif format in ['xlsx', 'excel']:
        df.write_excel(output_path)
    else:
        raise ValueError(f"Unsupported format: {format}")
    
    print(f"✓ Saved results to {output_path}")


def find_dbt_project_dir(model_name: str, base_dir: Optional[str] = None) -> Optional[str]:
    """
    Find the correct dbt project directory for a given model.
    
    Args:
        model_name: Model name to search for
        base_dir: Base directory to search from (defaults to current directory)
        
    Returns:
        Path to the dbt project directory containing the model, or None if not found
    """
    if base_dir is None:
        base_dir = os.getcwd()
    
    base_path = Path(base_dir)
    
    # Search in common dbt subproject directories
    search_dirs = [
        base_path / 'dbt_subprojects' / 'dex',
        base_path / 'dbt_subprojects' / 'solana', 
        base_path / 'dbt_subprojects' / 'nft',
        base_path / 'dbt_subprojects' / 'tokens',
        base_path / 'dbt_subprojects' / 'daily_spellbook',
        base_path / 'dbt_subprojects' / 'hourly_spellbook',
        base_path,  # Root level as fallback
    ]
    
    for search_dir in search_dirs:
        if search_dir.exists():
            # Look for the model file
            model_files = list(search_dir.rglob(f"{model_name}.sql"))
            if model_files:
                # Check if this directory has a dbt_project.yml
                dbt_project_file = search_dir / 'dbt_project.yml'
                if dbt_project_file.exists():
                    return str(search_dir)
    
    return None


def compile_dbt_model(
    model_name: str,
    *,
    project_dir: Optional[str] = None,
    profiles_dir: Optional[str] = None,
    target: Optional[str] = None,
    verbose: bool = True,
) -> str:
    """
    Compile a dbt model and return the compiled SQL.
    
    Args:
        model_name: Model name (e.g., 'uniswap_v3_unichain_base_trades' or '@uniswap_v3_unichain_base_trades')
        project_dir: dbt project directory (defaults to current directory)
        profiles_dir: dbt profiles directory
        target: dbt target (e.g., 'prod', 'dev')
        verbose: Print compilation details
        
    Returns:
        Compiled SQL as string
    """
    # Remove @ prefix if present
    if model_name.startswith('@'):
        model_name = model_name[1:]
    
    # Auto-detect project directory if not specified
    if project_dir is None:
        detected_dir = find_dbt_project_dir(model_name)
        if detected_dir:
            project_dir = detected_dir
        else:
            project_dir = os.getcwd()
            if verbose:
                print(f"⚠️  Could not auto-detect dbt project directory, using: {project_dir}")
    
    # Build dbt command - use pipenv if available
    cmd = ['pipenv', 'run', 'dbt', 'compile', '--select', model_name]
    
    if project_dir:
        cmd.extend(['--project-dir', project_dir])
    
    if profiles_dir:
        cmd.extend(['--profiles-dir', profiles_dir])
    
    if target:
        cmd.extend(['--target', target])
    
    if verbose:
        print(f"🔧 Compiling dbt model: {model_name}")
        print(f"📁 Project dir: {project_dir}")
        print(f"⚡ Command: {' '.join(cmd)}")
    
    try:
        # Run dbt compile
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            cwd=project_dir,
            check=True
        )
        
        if verbose:
            print(f"✅ dbt compile successful")
            if result.stderr:
                print(f"📝 dbt output: {result.stderr}")
        
        # Find the compiled SQL file
        # dbt compiles to target/compiled/<project_name>/models/<path>/<model_name>.sql
        target_dir = Path(project_dir) / 'target' / 'compiled'
        
        # Find the compiled file by searching for the model name
        compiled_files = list(target_dir.rglob(f"{model_name}.sql"))
        
        if not compiled_files:
            raise FileNotFoundError(f"Could not find compiled SQL file for model '{model_name}'")
        
        if len(compiled_files) > 1:
            if verbose:
                print(f"⚠️  Found multiple compiled files, using: {compiled_files[0]}")
        
        compiled_file = compiled_files[0]
        
        # Read the compiled SQL
        with open(compiled_file, 'r') as f:
            compiled_sql = f.read()
        
        if verbose:
            print(f"📄 Compiled SQL from: {compiled_file}")
            print(f"📊 SQL length: {len(compiled_sql)} characters")
        
        return compiled_sql
        
    except subprocess.CalledProcessError as e:
        error_msg = f"dbt compile failed: {e}"
        if e.stderr:
            error_msg += f"\nError output: {e.stderr}"
        if e.stdout:
            error_msg += f"\nStandard output: {e.stdout}"
        raise RuntimeError(error_msg)
    
    except FileNotFoundError:
        raise RuntimeError("dbt command not found. Make sure dbt is installed and in your PATH.")


def find_model_file(model_name: str, project_dir: Optional[str] = None) -> Optional[Path]:
    """
    Find the dbt model file for a given model name.
    
    Args:
        model_name: Model name (e.g., 'uniswap_v3_unichain_base_trades')
        project_dir: dbt project directory (defaults to current directory)
        
    Returns:
        Path to the model file, or None if not found
    """
    if project_dir is None:
        project_dir = os.getcwd()
    
    project_path = Path(project_dir)
    
    # Search in common dbt model directories
    search_dirs = [
        project_path / 'models',
        project_path / 'dbt_subprojects' / 'dex' / 'models',
        project_path / 'dbt_subprojects' / 'solana' / 'models',
    ]
    
    for search_dir in search_dirs:
        if search_dir.exists():
            # Look for .sql files with the model name
            model_files = list(search_dir.rglob(f"{model_name}.sql"))
            if model_files:
                return model_files[0]
    
    return None


def parse_params(param_list: list[str]) -> Dict[str, Any]:
    """
    Parse command line parameters in format key=value.
    
    Examples:
        ['network=ethereum', 'version=5'] -> {'network': 'ethereum', 'version': 5}
    """
    params = {}
    for param in param_list:
        if '=' not in param:
            raise ValueError(f"Invalid parameter format: {param}. Use key=value")
        
        key, value = param.split('=', 1)
        
        # Try to parse as number
        try:
            value = int(value)
        except ValueError:
            try:
                value = float(value)
            except ValueError:
                pass  # Keep as string
        
        params[key] = value
    
    return params


def main():
    parser = argparse.ArgumentParser(
        description='🌶️ Query Dune using spice - Simple CLI for Dune SQL API',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s 21693
  %(prog)s https://dune.com/queries/21693
  %(prog)s "SELECT * FROM dex_solana.trades LIMIT 5"
  %(prog)s "@uniswap_v3_unichain_base_trades"
  %(prog)s --sql-file compare.sql --refresh
  %(prog)s 21693 --params network=ethereum version=5
  %(prog)s 21693 --output results.csv
        """
    )
    
    parser.add_argument(
        'query',
        nargs='?',
        help='Query ID, URL, or SQL string (or use --sql-file)'
    )
    
    parser.add_argument(
        '--sql-file',
        type=str,
        help='Path to SQL file to execute'
    )
    
    parser.add_argument(
        '--refresh',
        action='store_true',
        help='Force new query execution'
    )
    
    parser.add_argument(
        '--params',
        nargs='+',
        help='Query parameters in format key=value (e.g., network=ethereum version=5)'
    )
    
    parser.add_argument(
        '--api-key',
        type=str,
        help='Dune API key (or set DUNE_API_KEY env var)'
    )
    
    parser.add_argument(
        '--performance',
        choices=['medium', 'large'],
        default='medium',
        help='Query performance tier'
    )
    
    parser.add_argument(
        '--limit',
        type=int,
        help='Max number of rows to return'
    )
    
    parser.add_argument(
        '--output',
        type=str,
        help='Save results to file (csv, parquet, json, xlsx)'
    )
    
    parser.add_argument(
        '--no-cache',
        action='store_true',
        help='Disable local caching'
    )
    
    parser.add_argument(
        '--quiet',
        action='store_true',
        help='Suppress verbose output'
    )
    
    parser.add_argument(
        '--dbt-project-dir',
        type=str,
        help='dbt project directory (for dbt model compilation)'
    )
    
    parser.add_argument(
        '--dbt-profiles-dir',
        type=str,
        help='dbt profiles directory (for dbt model compilation)'
    )
    
    parser.add_argument(
        '--dbt-target',
        type=str,
        help='dbt target (for dbt model compilation)'
    )
    
    args = parser.parse_args()
    
    # Determine query source
    if args.sql_file:
        with open(args.sql_file, 'r') as f:
            query = f.read()
        print(f"📄 Reading SQL from {args.sql_file}")
    elif args.query:
        query = args.query
    else:
        parser.error("Provide a query ID/URL/SQL or use --sql-file")
    
    # Parse parameters
    parameters = None
    if args.params:
        parameters = parse_params(args.params)
        print(f"📊 Using parameters: {parameters}")
    
    # Execute query
    print(f"🌶️  Executing query...")
    
    try:
        df = run_query(
            query,
            refresh=args.refresh,
            parameters=parameters,
            api_key=args.api_key,
            performance=args.performance,
            verbose=not args.quiet,
            limit=args.limit,
            cache=not args.no_cache,
            compile_dbt=isinstance(query, str) and query.startswith('@'),
            project_dir=args.dbt_project_dir,
            profiles_dir=args.dbt_profiles_dir,
            target=args.dbt_target,
        )
        
        print(f"\n✅ Query successful! Shape: {df.shape}")
        print(f"\n📋 Preview:")
        print(df.head(10))
        
        # Print summary stats
        print(f"\n📊 Summary:")
        print(f"  Rows: {df.shape[0]:,}")
        print(f"  Columns: {df.shape[1]}")
        print(f"  Column names: {', '.join(df.columns)}")
        
        # Save if requested
        if args.output:
            save_results(df, args.output)
        
        return df
        
    except Exception as e:
        print(f"\n❌ Query failed: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == '__main__':
    main()

