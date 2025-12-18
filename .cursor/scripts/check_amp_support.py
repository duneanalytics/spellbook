#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.12"
# dependencies = ["aiohttp", "dune-client", "python-dotenv"]
# ///
"""Check if the metadata API supports a given blockchain."""

import argparse
import asyncio
import os
import sys
from pathlib import Path

try:
    import aiohttp
    from dotenv import load_dotenv
except ImportError:
    print("ERROR: Required packages not installed. Run via: uv run check_amp_support.py")
    sys.exit(1)

# Import run_query from dune_query.py
sys.path.insert(0, str(Path(__file__).parent))
try:
    from dune_query import run_query
except ImportError as e:
    print("ERROR: Failed to import dune_query. Run via: uv run check_amp_support.py")
    sys.exit(1)

API_ENDPOINT = "https://api.sim.dune.com/alpha/evm/token-metadata/amp"


async def test_api_support(chain_id: int, chain_name: str) -> bool:
    """Test if metadata API supports the chain. Returns True if supported, False otherwise."""
    api_key = os.environ.get("SIM_METADATA_API_KEY")
    if not api_key:
        print("ERROR: sim_metadata_api_key not found in environment or .env file")
        sys.exit(1)

    test_address = "0x0000000000000000000000000000000000000000"

    try:
        timeout = aiohttp.ClientTimeout(total=30)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            payload = [{"chain_id": chain_id, "address": test_address}]
            headers = {"X-Sim-Api-Key": api_key, "Content-Type": "application/json"}

            async with session.post(API_ENDPOINT, json=payload, headers=headers) as response:
                return response.status == 200
    except Exception:
        return False


async def test_token_metadata_availability(chain_id: int, token_address: str) -> bool:
    """Test if the API has metadata for a specific token address. Returns True if metadata exists, False otherwise."""
    api_key = os.environ.get("SIM_METADATA_API_KEY")
    if not api_key:
        return False

    try:
        timeout = aiohttp.ClientTimeout(total=30)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            payload = [{"chain_id": chain_id, "address": token_address}]
            headers = {"X-Sim-Api-Key": api_key, "Content-Type": "application/json"}

            async with session.post(API_ENDPOINT, json=payload, headers=headers) as response:
                if response.status == 200:
                    result = await response.json()
                    # Check if we got actual metadata (non-empty array with data)
                    return isinstance(result, list) and len(result) > 0
                return False
    except Exception:
        return False


def get_native_token_address(chain_name: str) -> tuple[int, str | None] | None:
    """Query Dune for the chain_id and native token address of a chain using dune_query.py.

    Returns:
        tuple of (chain_id, token_address) if found, None otherwise. token_address may be None.
    """
    query = f"SELECT chain_id, token_address FROM dune.blockchains WHERE name = '{chain_name}'"

    try:
        results = run_query(query=query)
        if results.result.rows and len(results.result.rows) > 0:
            row = results.result.rows[0]
            chain_id = row.get("chain_id")
            token_address = row.get("token_address")

            if chain_id is None:
                print(f"WARNING: chain_id not found for {chain_name}")
                return None

            if token_address:
                # Normalize address format (remove 0x prefix if needed, ensure lowercase)
                addr = str(token_address).lower()
                if not addr.startswith("0x"):
                    addr = "0x" + addr
                return (int(chain_id), addr)
            else:
                # Return chain_id even if token_address is None
                return (int(chain_id), None)
        return None
    except Exception as e:
        print(f"WARNING: Failed to query chain info: {e}")
        return None


def parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Check if metadata API supports a given blockchain",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s megaeth
  %(prog)s kaia
  %(prog)s viction
        """,
    )

    parser.add_argument(
        "chain_name",
        type=str,
        help="Name of the blockchain to check (e.g., megaeth, kaia)",
    )

    return parser.parse_args()


def main() -> None:
    """Main entry point."""
    # Load .env file from project root
    env_path = Path(__file__).parent.parent.parent / ".env"
    load_dotenv(env_path)

    args = parse_args()
    chain_name = args.chain_name

    # Get chain_id and native token address from Dune
    chain_info = get_native_token_address(chain_name)

    if not chain_info:
        print(f"ERROR: Could not retrieve chain information for {chain_name}")
        print("Make sure the chain exists in dune.blockchains table")
        sys.exit(1)

    chain_id, native_token_address = chain_info

    print(f"✓ Found chain_id: {chain_id} for {chain_name}")

    # Test 1: API endpoint support
    api_supported = asyncio.run(test_api_support(chain_id, chain_name))

    if not api_supported:
        print(f"NO - {chain_name} (chain_id={chain_id}) does NOT have API support")
        sys.exit(1)

    print(f"✓ API endpoint supports {chain_name} (chain_id={chain_id})")

    # Test 2: Token metadata availability (using native token address)
    if not native_token_address:
        print(f"WARNING: Could not retrieve native token address for {chain_name}")
        print(f"YES - {chain_name} (chain_id={chain_id}) has API support (metadata availability unknown)")
        sys.exit(0)

    print(f"✓ Found native token address: {native_token_address}")

    metadata_available = asyncio.run(test_token_metadata_availability(chain_id, native_token_address))

    if metadata_available:
        print("✓ Token metadata is available for native token")
        print(f"YES - {chain_name} (chain_id={chain_id}) has API support and token metadata")
        sys.exit(0)
    else:
        print("⚠ Token metadata NOT available for native token (API supports chain but no metadata)")
        print(f"YES - {chain_name} (chain_id={chain_id}) has API support (but metadata may be incomplete)")
        sys.exit(0)


if __name__ == "__main__":
    main()
