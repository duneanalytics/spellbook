#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.12"
# dependencies = ["aiohttp", "dune-client", "python-dotenv"]
# ///
"""Compare stablecoins balances from Dune with SIM API.

Dune = yesterday's snapshot; SIM = live. Requires DUNE_API_KEY and SIM_METADATA_API_KEY.
Example: uv run .cursor/scripts/compare_stablecoins_balances.py ethereum --limit 50
"""

import argparse
import asyncio
import os
import sys
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any

try:
    import aiohttp
    from dotenv import load_dotenv
except ImportError:
    print("ERROR: Required packages not installed. Run via: uv run compare_stablecoins_balances.py")
    sys.exit(1)

# Import run_query from dune_query.py
sys.path.insert(0, str(Path(__file__).parent))
try:
    from dune_query import run_query
except ImportError:
    print("ERROR: Failed to import dune_query. Run via: uv run compare_stablecoins_balances.py")
    sys.exit(1)


SIM_EVM_BALANCES_API = "https://api.sim.dune.com/v1/evm/balances"
SIM_SVM_BALANCES_API = "https://api.sim.dune.com/beta/svm/balances"

CHAIN_ID_MAP = {
    "arbitrum": 42161,
    "avalanche_c": 43114,
    "base": 8453,
    "bnb": 56,
    "celo": 42220,
    "ethereum": 1,
    "kaia": 8217,
    "linea": 59144,
    "optimism": 10,
    "polygon": 137,
    "scroll": 534352,
    "unichain": 130,
    "worldchain": 480,
    "zksync": 324,
}
SVM_CHAINS = {"solana"}


@dataclass
class BalanceComparison:
    blockchain: str
    address: str
    token_address: str
    dune_balance: Decimal
    sim_balance: Decimal | None
    diff_percent: float | None
    status: str  # "exact_match", "small_diff", "large_diff", "not_found"


def get_dune_balances_query(
    chain: str,
    limit: int = 100,
    include_transfer_check: bool = False,
    include_gas_check: bool = False,
) -> str:
    """Build Dune SQL for stablecoin balances (limit, optional transfer/gas filters)."""
    if chain == "solana":
        return get_solana_balances_query(limit, include_transfer_check)
    test_schema_tables = {
        "unichain": "test_schema.git_dunesql_4f56406_stablecoins_unichain_balances",
        "celo": "test_schema.git_dunesql_4f56406_stablecoins_celo_balances",
        "arbitrum": "test_schema.git_dunesql_4f56406_stablecoins_arbitrum_balances_test",
    }
    table_name = test_schema_tables.get(chain, f"stablecoins_{chain}.balances")
    is_test_table = chain in test_schema_tables
    day_filter = (
        f"day = (SELECT max(day) FROM {table_name})"
        if is_test_table
        else "day = current_date - interval '1' day"
    )

    base_query = f"""
SELECT blockchain, address, token_address, token_symbol, balance
FROM {table_name}
WHERE {day_filter}
-- Exclude wrapped/derivative tokens from lending protocols
AND NOT (
    -- Aave: aTokens (aUSDC, aUSDT, aDAI, etc.)
    regexp_like(token_symbol, '^a[A-Z]')
    -- Compound: cTokens (cUSDC, cUSDT, cDAI) but NOT cUSD, cEUR, cREAL (Celo stables)
    OR (regexp_like(token_symbol, '^c[A-Z]') AND token_symbol NOT IN ('cUSD', 'cEUR', 'cREAL'))
    -- Spark/Morpho: spTokens
    OR token_symbol LIKE 'sp%'
    -- Yearn: yTokens
    OR regexp_like(token_symbol, '^y[A-Z]')
    -- Radiant: rTokens
    OR regexp_like(token_symbol, '^r[A-Z]')
    -- Wrapped/staked versions
    OR token_symbol LIKE 'st%'
    OR regexp_like(token_symbol, '^w[a-z]?[A-Z]')
    -- Synthetic/leveraged: xTokens (xUSD, xDAI, etc.)
    OR regexp_like(token_symbol, '^x[A-Z]')
    -- CDP/synthetic stablecoins ending with 's' (USDs, etc.) but not USDS (Sky)
    OR (regexp_like(token_symbol, '^USD[a-z]$') AND token_symbol != 'USDS')
    -- Interest-bearing: iTokens, mTokens (Morpho)
    OR regexp_like(token_symbol, '^[im][A-Z]')
    -- Gains: gTokens
    OR regexp_like(token_symbol, '^g[A-Z]')
    -- dForce: dTokens
    OR regexp_like(token_symbol, '^d[A-Z]')
)
"""

    if include_transfer_check:
        base_query += f"""
AND address NOT IN (
    SELECT DISTINCT "to" as addr FROM erc20_{chain}.evt_Transfer
    WHERE evt_block_time >= now() - interval '48' hour
    UNION
    SELECT DISTINCT "from" as addr FROM erc20_{chain}.evt_Transfer
    WHERE evt_block_time >= now() - interval '48' hour
)
"""

    if include_gas_check and chain == "celo":
        base_query += """
AND address IN (
    SELECT DISTINCT tx_from FROM gas_celo.fees
    WHERE currency_symbol IN ('USDT', 'USDC', 'cUSD', 'cEUR', 'cREAL')
)
"""

    base_query += f"""
ORDER BY balance DESC
LIMIT {limit}
"""
    return base_query


def get_solana_balances_query(limit: int = 100, include_transfer_check: bool = False) -> str:
    """Build Dune SQL for Solana stablecoin balances."""
    table_name = "stablecoins_solana.balances"
    base_query = f"""
SELECT blockchain, address, token_address, token_symbol, balance
FROM {table_name}
WHERE day = current_date - interval '1' day
"""
    if include_transfer_check:
        base_query += """
AND address NOT IN (
    SELECT DISTINCT from_owner FROM tokens_solana.transfers
    WHERE block_time >= now() - interval '48' hour
    UNION
    SELECT DISTINCT to_owner FROM tokens_solana.transfers
    WHERE block_time >= now() - interval '48' hour
)
"""
    base_query += f"""
ORDER BY balance DESC
LIMIT {limit}
"""
    return base_query


async def fetch_sim_evm_balances(
    session: aiohttp.ClientSession,
    api_key: str,
    chain_id: int,
    address: str,
    delay: float = 0.5,
) -> dict[str, Decimal]:
    """Fetch SIM EVM balances for one address. Returns token_address -> balance."""
    if delay > 0:
        await asyncio.sleep(delay)
    try:
        url = f"{SIM_EVM_BALANCES_API}/{address}"
        headers = {"X-Sim-Api-Key": api_key, "Content-Type": "application/json"}
        params = {"chain_ids": chain_id}

        async with session.get(url, headers=headers, params=params) as response:
            if response.status == 200:
                result = await response.json()
                balances: dict[str, Decimal] = {}
                if isinstance(result, dict) and "balances" in result:
                    for bal in result["balances"]:
                        if bal.get("chain_id") != chain_id:
                            continue
                        token_addr = bal.get("address", "").lower()
                        amount_raw = bal.get("amount", "0")
                        decimals = bal.get("decimals", 18)
                        try:
                            balance = Decimal(str(amount_raw)) / Decimal(10**decimals)
                            balances[token_addr] = balance
                        except (ValueError, TypeError):
                            pass
                elif isinstance(result, list):
                    for item in result:
                        if "balances" in item:
                            for bal in item["balances"]:
                                if bal.get("chain_id") != chain_id:
                                    continue
                                token_addr = bal.get("address", "").lower()
                                amount_raw = bal.get("amount", "0")
                                decimals = bal.get("decimals", 18)
                                try:
                                    balance = Decimal(str(amount_raw)) / Decimal(10**decimals)
                                    balances[token_addr] = balance
                                except (ValueError, TypeError):
                                    pass

                return balances
            elif response.status == 404:
                return {}
            else:
                return {}
    except Exception as e:
        print(f"WARNING: Error fetching SIM EVM balances for {address}: {e}")
        return {}


async def fetch_sim_svm_balances(
    session: aiohttp.ClientSession,
    api_key: str,
    address: str,
    delay: float = 0.5,
) -> dict[str, Decimal]:
    """Fetch SIM Solana balances for one address. Returns token_address -> balance."""
    if delay > 0:
        await asyncio.sleep(delay)
    try:
        url = f"{SIM_SVM_BALANCES_API}/{address}"
        headers = {"X-Sim-Api-Key": api_key, "Content-Type": "application/json"}
        params = {"chains": "solana"}
        async with session.get(url, headers=headers, params=params) as response:
            if response.status == 200:
                result = await response.json()
                balances: dict[str, Decimal] = {}
                if isinstance(result, dict) and "balances" in result:
                    for bal in result["balances"]:
                        if bal.get("chain") != "solana":
                            continue
                        token_addr = bal.get("address", "")
                        balance_str = bal.get("balance")
                        if balance_str is not None:
                            try:
                                balances[token_addr] = Decimal(str(balance_str))
                            except (ValueError, TypeError):
                                pass
                        else:
                            amount_raw = bal.get("amount", "0")
                            decimals = bal.get("decimals", 9)
                            try:
                                balance = Decimal(str(amount_raw)) / Decimal(10**decimals)
                                balances[token_addr] = balance
                            except (ValueError, TypeError):
                                pass
                return balances
            elif response.status == 404:
                return {}
            else:
                return {}
    except Exception as e:
        print(f"WARNING: Error fetching SIM SVM balances for {address}: {e}")
        return {}


def _classify_balance(dune_balance: Decimal, sim_balance: Decimal | None) -> tuple[str, float | None]:
    if sim_balance is None:
        return "not_found", None
    elif dune_balance == 0 and sim_balance == 0:
        return "exact_match", 0.0
    elif dune_balance == 0:
        return "large_diff", 100.0
    elif sim_balance == dune_balance:
        return "exact_match", 0.0
    else:
        diff = abs(dune_balance - sim_balance)
        diff_percent = float(diff / dune_balance * 100)
        if diff_percent < 0.0001:
            return "exact_match", 0.0
        elif diff_percent < 1.0:
            return "small_diff", diff_percent
        else:
            return "large_diff", diff_percent


async def compare_balances(
    dune_rows: list[dict[str, Any]],
    chain: str,
    api_key: str,
    request_delay: float = 0.5,
    retry_not_found: bool = True,
    retry_delay: float = 5.0,
) -> list[BalanceComparison]:
    """Compare Dune balance rows with SIM API; optional retry for not-found entries."""
    is_svm = chain in SVM_CHAINS
    chain_id = None if is_svm else CHAIN_ID_MAP.get(chain)
    if not is_svm and not chain_id:
        print(f"ERROR: Unknown chain '{chain}'. Supported: {list(CHAIN_ID_MAP.keys()) + list(SVM_CHAINS)}")
        return []

    results: list[BalanceComparison] = []
    addresses_tokens: dict[str, list[dict]] = {}
    for row in dune_rows:
        addr = str(row.get("address", ""))
        if not is_svm:
            addr = addr.lower()
        if addr not in addresses_tokens:
            addresses_tokens[addr] = []
        addresses_tokens[addr].append(row)

    timeout = aiohttp.ClientTimeout(total=60)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        addresses = list(addresses_tokens.keys())
        total = len(addresses)
        not_found_entries: list[tuple[int, str, dict]] = []

        for idx, addr in enumerate(addresses):
            if is_svm:
                sim_balances = await fetch_sim_svm_balances(
                    session, api_key, addr, delay=request_delay if idx > 0 else 0
                )
            else:
                sim_balances = await fetch_sim_evm_balances(
                    session, api_key, chain_id, addr, delay=request_delay if idx > 0 else 0
                )

            for row in addresses_tokens[addr]:
                token_addr = str(row.get("token_address", ""))
                if not is_svm:
                    token_addr = token_addr.lower()
                dune_balance = Decimal(str(row.get("balance", 0)))
                sim_balance = sim_balances.get(token_addr)

                status, diff_percent = _classify_balance(dune_balance, sim_balance)

                result_index = len(results)
                results.append(
                    BalanceComparison(
                        blockchain=str(row.get("blockchain", chain)),
                        address=addr,
                        token_address=token_addr,
                        dune_balance=dune_balance,
                        sim_balance=sim_balance,
                        diff_percent=diff_percent,
                        status=status,
                    )
                )

                if status == "not_found":
                    not_found_entries.append((result_index, addr, row))
            if total > 10 and (idx + 1) % 10 == 0:
                print(f"  Progress: {idx + 1}/{total} addresses...")

        if retry_not_found and not_found_entries:
            print(f"\n  {len(not_found_entries)} entries not found. Retrying each with {retry_delay}s delay...")
            retry_addresses: dict[str, list[tuple[int, dict]]] = {}
            for result_idx, addr, row in not_found_entries:
                if addr not in retry_addresses:
                    retry_addresses[addr] = []
                retry_addresses[addr].append((result_idx, row))

            retry_addr_list = list(retry_addresses.keys())
            resolved_count = 0

            for idx, addr in enumerate(retry_addr_list):
                await asyncio.sleep(retry_delay)
                if is_svm:
                    sim_balances = await fetch_sim_svm_balances(
                        session, api_key, addr, delay=0
                    )
                else:
                    sim_balances = await fetch_sim_evm_balances(
                        session, api_key, chain_id, addr, delay=0
                    )

                for result_idx, row in retry_addresses[addr]:
                    token_addr = str(row.get("token_address", ""))
                    if not is_svm:
                        token_addr = token_addr.lower()
                    dune_balance = Decimal(str(row.get("balance", 0)))
                    sim_balance = sim_balances.get(token_addr)

                    if sim_balance is not None:
                        status, diff_percent = _classify_balance(dune_balance, sim_balance)
                        results[result_idx] = BalanceComparison(
                            blockchain=str(row.get("blockchain", chain)),
                            address=addr,
                            token_address=token_addr,
                            dune_balance=dune_balance,
                            sim_balance=sim_balance,
                            diff_percent=diff_percent,
                            status=status,
                        )
                        resolved_count += 1
                if len(retry_addr_list) > 5:
                    print(f"    Retry progress: {idx + 1}/{len(retry_addr_list)} addresses...")

            print(f"  Retry resolved {resolved_count}/{len(not_found_entries)} entries")

    return results


def print_summary(comparisons: list[BalanceComparison], show_not_found: bool = False) -> None:
    exact_matches = sum(1 for c in comparisons if c.status == "exact_match")
    small_diffs = sum(1 for c in comparisons if c.status == "small_diff")
    large_diffs = sum(1 for c in comparisons if c.status == "large_diff")
    not_found = sum(1 for c in comparisons if c.status == "not_found")
    total = len(comparisons)

    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print("Dune = yesterday's snapshot; SIM = live.")
    print(f"Total entries compared: {total}")
    print(f"  ✓ Exact matches (100%):  {exact_matches}")
    print(f"  ~ Small diff (<1%):       {small_diffs}")
    print(f"  ✗ Large diff (>=1%):      {large_diffs}")
    not_found_note = "" if show_not_found else " (hidden)"
    print(f"  ? Not found in SIM:       {not_found}{not_found_note}")
    print("=" * 60)


def format_balance(value: Decimal | float | None) -> str:
    if value is None:
        return "N/A"
    return f"{float(value):,.2f}"


def print_differences(
    comparisons: list[BalanceComparison],
    show_small: bool = True,
    show_large: bool = True,
    show_not_found: bool = False,
) -> None:
    diffs = [
        c
        for c in comparisons
        if (show_small and c.status == "small_diff")
        or (show_large and c.status == "large_diff")
        or (show_not_found and c.status == "not_found")
    ]

    if not diffs:
        print("\nNo differences to show.")
        return
    status_order = {"small_diff": 0, "large_diff": 1, "not_found": 2}
    diffs.sort(key=lambda c: (status_order.get(c.status, 99), -(c.diff_percent or 0)))

    print("\n" + "-" * 120)
    print("DIFFERENCES")
    print("-" * 120)
    print(
        f"{'Status':<12} {'Address':<44} {'Token':<44} {'Dune Balance':>18} {'SIM Balance':>18} {'Diff %':>8}"
    )
    print("-" * 120)

    for c in diffs:
        dune_bal_str = format_balance(c.dune_balance)
        sim_bal_str = format_balance(c.sim_balance)
        diff_str = f"{c.diff_percent:.2f}%" if c.diff_percent is not None else "N/A"

        status_icon = {
            "small_diff": "~",
            "large_diff": "✗",
            "not_found": "?",
        }.get(c.status, " ")

        print(
            f"{status_icon} {c.status:<10} {c.address:<44} {c.token_address:<44} {dune_bal_str:>18} {sim_bal_str:>18} {diff_str:>8}"
        )


def get_all_supported_chains() -> list[str]:
    return sorted(list(CHAIN_ID_MAP.keys()) + list(SVM_CHAINS))


def parse_args() -> argparse.Namespace:
    all_chains = get_all_supported_chains()
    parser = argparse.ArgumentParser(
        description="Compare stablecoins balances from Dune with SIM API",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s arbitrum
  %(prog)s ethereum --summary-only
  %(prog)s solana --limit 50
  %(prog)s celo --check-gas
  %(prog)s base --diffs-only --large-only
  %(prog)s polygon --transfer-check --limit 50
  %(prog)s arbitrum --delay 2.0  # slower for Free plan (10 req/min)

Rate limit guidelines (--delay):
  Enterprise: 0.5s (120 req/min) - default
  Team:       1.0s (60 req/min)
  Pro:        2.0s (30 req/min)
  Free:       6.0s (10 req/min)
        """,
    )

    parser.add_argument(
        "chain",
        type=str,
        help=f"Chain name to compare. Supported: {all_chains}",
    )

    parser.add_argument(
        "--summary-only",
        action="store_true",
        help="Only show summary, no list of differences",
    )

    parser.add_argument(
        "--diffs-only",
        action="store_true",
        help="Only show differences, no summary",
    )

    parser.add_argument(
        "--small-only",
        action="store_true",
        help="Only show differences with <1%% diff",
    )

    parser.add_argument(
        "--large-only",
        action="store_true",
        help="Only show differences with >=1%% diff",
    )

    parser.add_argument(
        "--transfer-check",
        action="store_true",
        help="Exclude addresses with any transfers in the last 48h",
    )

    parser.add_argument(
        "--check-gas",
        action="store_true",
        help="Only include addresses that paid Celo gas fees with stablecoins (Celo only)",
    )

    parser.add_argument(
        "--limit",
        type=int,
        default=100,
        help="Maximum number of balance entries to fetch (default: 100)",
    )

    parser.add_argument(
        "--delay",
        type=float,
        default=0.5,
        help="Delay in seconds between API requests (default: 0.5, use 2.0 for Free plan)",
    )

    parser.add_argument(
        "--show-not-found",
        action="store_true",
        help="Show 'not found' entries (tokens not in SIM API). Hidden by default.",
    )

    return parser.parse_args()


def main() -> None:
    env_path = Path(__file__).parent.parent.parent / ".env"
    load_dotenv(env_path)

    args = parse_args()
    chain = args.chain.lower()
    all_chains = get_all_supported_chains()
    if chain not in CHAIN_ID_MAP and chain not in SVM_CHAINS:
        print(f"ERROR: Unknown chain '{chain}'. Supported chains: {all_chains}")
        sys.exit(1)

    is_svm = chain in SVM_CHAINS
    api_key = os.environ.get("SIM_METADATA_API_KEY")
    if not api_key:
        print("ERROR: SIM_METADATA_API_KEY not found in environment or .env file")
        sys.exit(1)

    if args.check_gas and chain != "celo":
        print("WARNING: --check-gas is only applicable to the 'celo' chain, ignoring")
        args.check_gas = False

    query = get_dune_balances_query(
        chain,
        limit=args.limit,
        include_transfer_check=args.transfer_check,
        include_gas_check=args.check_gas,
    )

    print(f"Fetching stablecoin balances from Dune for {chain} (limit={args.limit})...")
    if args.transfer_check:
        transfer_window = "3h" if is_svm else "48h"
        print(f"  (Excluding addresses with transfers in the last {transfer_window})")
    if args.check_gas:
        print("  (Only including addresses that paid gas with stablecoins)")

    try:
        results = run_query(query=query)
        rows = results.result.rows
        print(f"  Retrieved {len(rows)} balance entries")
    except Exception as e:
        print(f"ERROR: Failed to query Dune: {e}")
        sys.exit(1)

    if not rows:
        print("No balance entries found. Nothing to compare.")
        sys.exit(0)
    if is_svm:
        unique_addresses = len(set(str(r.get("address", "")) for r in rows))
        api_info = "SVM beta API"
    else:
        unique_addresses = len(set(str(r.get("address", "")).lower() for r in rows))
        api_info = f"chain_id={CHAIN_ID_MAP[chain]}"
    est_time = unique_addresses * args.delay
    print(f"\nComparing with SIM API ({api_info})...")
    print(f"  {unique_addresses} unique addresses, ~{est_time:.0f}s estimated (delay={args.delay}s)")
    comparisons = asyncio.run(compare_balances(rows, chain, api_key, request_delay=args.delay))
    show_small = not args.large_only
    show_large = not args.small_only

    if not args.diffs_only:
        print_summary(comparisons, show_not_found=args.show_not_found)

    if not args.summary_only:
        print_differences(
            comparisons,
            show_small=show_small,
            show_large=show_large,
            show_not_found=args.show_not_found,
        )


if __name__ == "__main__":
    main()
