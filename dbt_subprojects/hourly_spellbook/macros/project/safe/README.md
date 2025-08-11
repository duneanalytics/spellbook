# Safe Project Macros

This directory contains macros for the Safe project that significantly reduce code duplication and improve maintainability across 22+ blockchain networks.

## Overview

The Safe project has been refactored to use a macro-based approach, reducing code by ~85% while maintaining all existing functionality and table structures.

## Key Macros

### 1. `safe_config`
Generates standardized dbt configuration blocks for all Safe models.

**Usage:**
```sql
{{ 
    safe_config(
        blockchain = 'arbitrum',
        alias_name = 'safes',
        contributors = '\'["foo", "bar"]\''
    )
}}
```

**Parameters:**
- `blockchain`: Network name (required)
- `alias_name`: Table alias like 'safes', 'transactions', etc. (required)
- `contributors`: List of contributors (optional, has defaults)
- Other dbt config parameters can be overridden

### 2. `safe_safes_creation`
Generates the Safe creation query logic for all networks.

**Usage:**
```sql
{{ 
    safe_safes_creation(
        blockchain = 'arbitrum'
    ) 
}}
```

**Parameters:**
- `blockchain`: Network name (required)
- `project_start_date`: Override start date (optional, uses network config default)
- `version_mapping`: Additional version mappings (optional)

### 3. `safe_native_transfer_wrapper`
Simplifies native token transfer files to a single macro call.

**Usage:**
```sql
{{ 
    safe_native_transfer_wrapper(
        blockchain = 'optimism'
    )
}}
```

### 4. `safe_transactions_wrapper`
Wraps the existing `safe_transactions` macro with standardized config.

**Usage:**
```sql
{{ 
    safe_transactions_wrapper(
        blockchain = 'base'
    )
}}
```

### 5. `safe_singletons_by_network`
Automatically generates singleton queries based on network configuration.

**Usage:**
```sql
{{ safe_singletons_by_network('polygon') }}
```

### 6. `safe_aggregation_all`
Generates aggregation queries for _all files dynamically.

**Usage:**
```sql
{{ 
    safe_aggregation_all(
        table_type = 'transactions',
        blockchains = ["arbitrum", "optimism", "base"],
        contributors = '["foo", "bar"]'
    )
}}
```

## Network Configuration

All network-specific information is centralized in `safe_network_config.sql`:
- Start dates
- Native token symbols
- Singleton source types and tables
- Network-specific flags
- Contributors by model type (safes, singletons, transfers, transactions)

Contributors are automatically pulled from the network config. If a network doesn't have specific contributors defined, it uses the default contributors list.

## Adding a New Network

1. Add network configuration to `safe_network_config.sql`:
```sql
'new_network': {
    'start_date': '2024-01-01',
    'native_token': 'TOKEN',
    'singleton_type': 'modern',
    'singleton_sources': ['SafeProxyFactory_v_1_4_1_evt_ProxyCreation'],
    'contributors': {
        'safes': ["contributor1", "contributor2"],
        'singletons': ["contributor1"]
        // Optional: if not specified, uses default contributors
    }
}
```

2. Create network directory: `models/_project/safe/new_network/`

3. Create 4 files using macros:

**safe_new_network_safes.sql:**
```sql
{{ safe_config(blockchain = 'new_network', alias_name = 'safes') }}
{{ safe_safes_creation(blockchain = 'new_network') }}
```

**safe_new_network_token_transfers.sql:**
```sql
{{ safe_native_transfer_wrapper(blockchain = 'new_network') }}
```

**safe_new_network_singletons.sql:**
```sql
{{ 
    config(
        materialized='table',
        alias = 'singletons',
        post_hook='{{ expose_spells(\'["new_network"]\', "project", "safe", \'["contributor"]\') }}'
    ) 
}}
{{ safe_singletons_by_network('new_network') }}
```

**safe_new_network_transactions.sql:**
```sql
{{ safe_transactions_wrapper(blockchain = 'new_network') }}
```

4. Update aggregation files to include the new network in the blockchains array

## Special Cases

### Ethereum
Ethereum uses a custom singleton pattern due to its legacy architecture. The `safe_singletons_ethereum()` macro handles this special case.

### Networks without Native Transfers
Some networks like Fantom and Berachain don't have native transfer files. This is configured in `safe_network_config.sql` with `'has_native_transfers': false`.

### Gnosis xDAI Transfers
Gnosis has special handling for BlockReward events in addition to standard transfers. The native transfer file needs custom logic after the wrapper macro.

## Benefits

- **Code Reduction**: ~85% less code to maintain
- **Consistency**: Enforces standardization across all networks
- **Maintainability**: Logic changes only need to happen in one place
- **Scalability**: Adding new networks is trivial
- **Error Reduction**: Less copy-paste means fewer inconsistencies

## Macro Files

- `safe_config.sql` - Configuration block generation
- `safe_safes.sql` - Safe creation query logic
- `safe_singletons.sql` - Singleton address queries (modern, legacy, and Ethereum patterns)
- `safe_utils.sql` - Wrapper macros for transactions, native transfers, and aggregations
- `safe_network_config.sql` - Centralized network configuration data
