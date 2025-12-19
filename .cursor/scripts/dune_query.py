#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.12"
# dependencies = ["dune-client", "python-dotenv"]
# ///
"""Minimal script to run Dune queries by query_id or SQL string."""

import argparse
import os
import sys
from pathlib import Path

try:
    from dune_client.client import DuneClient
    from dune_client.models import ResultsResponse
    from dune_client.query import QueryBase, QueryParameter
    from dotenv import load_dotenv
except ImportError:
    print("ERROR: Required packages not installed. Run via: uv run dune_query.py")
    sys.exit(1)


def run_query(
    query_id: int | None = None,
    query: str | None = None,
    parameters: dict[str, str] | None = None,
) -> ResultsResponse:
    """
    Run a Dune query either by query_id or SQL string.

    Args:
        query_id: Dune query ID (integer)
        query: SQL query string
        parameters: Dictionary of parameters for parameterized queries

    Returns:
        Query results
    """
    api_key = os.environ.get("DUNE_API_KEY")
    if not api_key:
        print("ERROR: DUNE_API_KEY not found in environment or .env file")
        sys.exit(1)

    dune = DuneClient(api_key=api_key)

    if query_id is not None:
        # Run saved query by ID
        # Convert parameters dict to QueryParameter list
        params_list: list[QueryParameter] = []
        if parameters:
            for key, value in parameters.items():
                params_list.append(QueryParameter.text_type(key, value))
        query_obj = QueryBase(query_id=query_id, params=params_list if params_list else None)
        results = dune.run_query(query=query_obj)
        return results
    elif query is not None:
        # Run SQL query string
        results = dune.run_sql(query_sql=query)
        return results
    else:
        raise ValueError("Either query_id or query must be provided")


def parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Run Dune queries by query_id or SQL string",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s --query-id 3493826
  %(prog)s --query-id 6293737 --param chain=xlayer
  %(prog)s --query "SELECT * FROM dex.trades LIMIT 10"
        """,
    )

    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--query-id",
        type=int,
        help="Dune query ID to execute",
    )
    group.add_argument(
        "--query",
        type=str,
        help="SQL query string to execute",
    )

    parser.add_argument(
        "--param",
        action="append",
        metavar="KEY=VALUE",
        help="Query parameter (can be repeated). Example: --param chain=xlayer",
    )

    return parser.parse_args()


def main() -> None:
    """Main entry point."""
    # Load .env file from project root
    env_path = Path(__file__).parent.parent.parent / ".env"
    load_dotenv(env_path)

    args = parse_args()

    # Parse parameters
    parameters: dict[str, str] = {}
    if args.param:
        for param_str in args.param:
            if "=" in param_str:
                key, value = param_str.split("=", 1)
                parameters[key] = value
            else:
                print(f"WARNING: Invalid parameter format '{param_str}', expected KEY=VALUE")

    # Run query
    if args.query_id is not None:
        results = run_query(query_id=args.query_id, parameters=parameters if parameters else None)
    else:
        results = run_query(query=args.query)

    # Print results
    print(f"Query executed successfully. Rows: {len(results.result.rows)}")
    if results.result.rows:
        print("\nResults:")
        for i, row in enumerate(results.result.rows):
            print(f"  Row {i + 1}: {row}")


if __name__ == "__main__":
    main()
