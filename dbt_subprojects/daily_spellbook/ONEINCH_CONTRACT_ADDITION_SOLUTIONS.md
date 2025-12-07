# 1inch Contract Addition Solutions

## Problem Statement

When adding new contracts to `mapped_contracts`, it triggers a cascade of full refreshes across the entire model lineage because:
1. New contracts often have historical data that needs processing (not just forward-looking)
2. `mapped_contracts` is materialized as a table (not incremental)
3. All downstream models depend on it: `mapped_contracts` → `project_calls` → `project_orders` → `project_swaps`
4. The CI/CD workflow rebuilds all downstream models when upstream changes

## Solution Variants

### 1. Make mapped_contracts Incremental

Convert the mapped_contracts model to use incremental materialization:

```sql
{{
    config(
        schema = 'oneinch_' + blockchain,
        alias = 'mapped_contracts',
        materialized = 'incremental',
        incremental_strategy = 'merge',
        unique_key = ['blockchain', 'address'],
        incremental_predicates = [incremental_predicate('created_at')]
    )
}}

-- Add a version/updated_at column to track changes
select 
    *,
    current_timestamp as last_updated
from (
    {{ oneinch_mapped_contracts_macro(blockchain = blockchain) }}
)
{% if is_incremental() %}
    -- Only process new or updated contracts
    where created_at >= (select max(created_at) from {{ this }})
{% endif %}
```

**Pros:**
- Minimal code changes
- Leverages dbt's incremental features

**Cons:**
- Doesn't solve historical data processing needs
- Still requires downstream refreshes for historical data

### 2. Decouple Contract Mapping from Processing

Create a separate "active contracts" view layer:

```sql
-- oneinch_[blockchain]_active_contracts.sql (materialized as view)
select * from {{ ref('oneinch_' + blockchain + '_mapped_contracts') }}
where active = true  -- Add an active flag

-- oneinch_[blockchain]_pending_contracts.sql (materialized as view)
select * from {{ ref('oneinch_' + blockchain + '_mapped_contracts') }}
where active = false and pending_backfill = true
```

**Pros:**
- Can stage contracts before activation
- No immediate impact on production models

**Cons:**
- Requires adding status flags to contract mappings
- Still need a process for historical backfill

### 3. Use Seeds for Contract Configuration

Move contract mappings from macros to seed files:

```yaml
# seeds/oneinch/contract_mappings.csv
blockchain,address,project,tag,active,date_added
ethereum,0x1111111254eeb25477b68fb85ed929f73a960582,1inch,aggregator,true,2019-01-01
ethereum,0x1111111254fb6c44bac0bed2854e76f90643097d,1inch,aggregator_v4,true,2020-01-01
ethereum,0x119c71d3bbac22029622cbaec24854d3d32d2828,1inch,lop,false,2024-01-15
```

```sql
-- Update mapped_contracts model to read from seed
{{ config(materialized = 'table') }}

select 
    cm.*,
    -- Add enrichment data
    coalesce(c.creator, '') as creator,
    coalesce(c.creation_time, timestamp '1970-01-01') as creation_time
from {{ ref('contract_mappings') }} cm
left join contracts c on cm.address = c.address
where cm.blockchain = '{{ blockchain }}'
```

**Pros:**
- Version controlled configuration
- Easy to review changes
- Can add contracts without model changes

**Cons:**
- Loses dynamic enrichment capabilities
- Requires seed refresh on changes

### 4. Partition-Based Selective Refresh

Add contract-based partitioning to enable targeted refreshes:

```sql
{{
    config(
        materialized = 'incremental',
        partition_by = ['block_month', 'contract_address'],
        unique_key = ['blockchain', 'tx_hash', 'evt_index'],
        incremental_strategy = 'insert_overwrite'
    )
}}

-- When adding new contracts, only refresh their partitions
{% if var('refresh_contracts', false) %}
    where contract_address in ({{ var('contract_list', []) | join(', ') }})
{% elif is_incremental() %}
    where block_time >= (select max(block_time) from {{ this }})
{% endif %}
```

Usage:
```bash
dbt run --select oneinch_ethereum_project_swaps \
  --vars '{"refresh_contracts": true, "contract_list": ["0x123...", "0x456..."]}'
```

**Pros:**
- Surgical updates for specific contracts
- Preserves existing data

**Cons:**
- Requires partition support in the warehouse
- Complex variable management

### 5. Contract-Specific Backfill Models

Create parallel models for backfilling new contracts:

```sql
-- oneinch_ethereum_project_swaps_backfill.sql
{{
    config(
        materialized = 'table',
        tags = ['backfill'],
        post_hook = 'insert into {{ ref("oneinch_ethereum_project_swaps") }} select * from {{ this }}'
    )
}}

-- Process only specific contracts
{{ oneinch_project_swaps_macro(
    blockchain = 'ethereum',
    contract_filter = var('backfill_contracts', [])
) }}
```

Workflow:
1. Add contracts to mapped_contracts
2. Run backfill model: `dbt run --select tag:backfill --vars '{"backfill_contracts": ["0x..."]}'`
3. Data automatically merged into main model
4. Drop backfill table

**Pros:**
- Isolated backfill process
- No impact on production models during processing

**Cons:**
- Requires manual orchestration
- Temporary storage overhead

### 6. Time-Bounded Incremental Backfill

Process historical data in time chunks to avoid timeouts:

```sql
-- Macro for chunked processing
{% macro backfill_by_period(model_name, start_date, end_date, contracts) %}
    
    {% set periods = [] %}
    {% set current = start_date %}
    
    -- Generate monthly periods
    {% for i in range(100) %}  -- max 100 months
        {% if current < end_date %}
            {% set next = current + interval '1 month' %}
            {% do periods.append((current, next)) %}
            {% set current = next %}
        {% endif %}
    {% endfor %}
    
    -- Process each period
    {% for period_start, period_end in periods %}
        insert into {{ ref(model_name) }}
        select * from (
            {{ oneinch_project_swaps_macro(
                blockchain = blockchain,
                date_from = period_start,
                date_to = period_end,
                contract_filter = contracts
            ) }}
        );
        commit;
    {% endfor %}
    
{% endmacro %}
```

**Pros:**
- Handles large historical datasets
- Checkpoint commits prevent data loss

**Cons:**
- Slower overall processing
- Complex orchestration

### 7. Smart Merge Strategy

Keep existing data and only process new contracts:

```sql
{% macro process_new_contracts(model_name, new_contracts) %}
    
    -- Create backup of existing data (excluding new contracts)
    create table {{ model_name }}_existing as
    select * from {{ ref(model_name) }}
    where contract_address not in ({{ new_contracts | join(', ') }});
    
    -- Process full history for new contracts only
    create table {{ model_name }}_new as
    {{ oneinch_project_swaps_macro(
        blockchain = blockchain,
        contract_filter = new_contracts
    ) }};
    
    -- Atomic replace
    begin transaction;
    drop table {{ ref(model_name) }};
    create table {{ ref(model_name) }} as
    select * from {{ model_name }}_existing
    union all
    select * from {{ model_name }}_new;
    commit;
    
    -- Cleanup
    drop table {{ model_name }}_existing;
    drop table {{ model_name }}_new;
    
{% endmacro %}
```

**Pros:**
- Preserves existing data perfectly
- Atomic operation prevents inconsistencies

**Cons:**
- Requires 2x storage temporarily
- Complex error handling needed

### 8. Multi-Stage Contract Batching

Divide contracts into logical batches processed separately:

```sql
-- Stage 1: Contract Groups
-- oneinch_ethereum_mapped_contracts_core.sql
{{ config(materialized = 'table') }}
select * from (
    {{ oneinch_mapped_contracts_macro(
        blockchain = 'ethereum',
        contract_batch = 'core'  -- main aggregator contracts
    ) }}
)

-- oneinch_ethereum_mapped_contracts_lop.sql
{{ config(materialized = 'table') }}
select * from (
    {{ oneinch_mapped_contracts_macro(
        blockchain = 'ethereum',
        contract_batch = 'lop'  -- limit order protocol
    ) }}
)

-- oneinch_ethereum_mapped_contracts_external.sql
{{ config(materialized = 'table') }}
select * from (
    {{ oneinch_mapped_contracts_macro(
        blockchain = 'ethereum',
        contract_batch = 'external'  -- integrated DEXs
    ) }}
)

-- Stage 2: Process Each Batch
-- oneinch_ethereum_project_swaps_core.sql
{{ config(
    materialized = 'incremental',
    partition_by = ['block_month']
) }}
{{ oneinch_project_swaps_macro(
    blockchain = 'ethereum',
    contract_source = ref('oneinch_ethereum_mapped_contracts_core')
) }}

-- Stage 3: Union All Batches
-- oneinch_ethereum_project_swaps.sql
{{ config(materialized = 'view') }}

select * from {{ ref('oneinch_ethereum_project_swaps_core') }}
union all
select * from {{ ref('oneinch_ethereum_project_swaps_lop') }}
union all
select * from {{ ref('oneinch_ethereum_project_swaps_external') }}
```

**Pros:**
- Complete isolation between batches
- Can refresh individual batches
- Parallel processing possible
- Clear organization

**Cons:**
- More models to maintain
- Potential for batch imbalance

### 9. Project-Based Separation

Similar to batching but organized by project/protocol:

```sql
-- oneinch_ethereum_project_swaps_1inch.sql
{{ config(materialized = 'incremental') }}
{{ oneinch_project_swaps_macro(
    blockchain = 'ethereum',
    project_filter = '1inch'
) }}

-- oneinch_ethereum_project_swaps_0x.sql
{{ config(materialized = 'incremental') }}
{{ oneinch_project_swaps_macro(
    blockchain = 'ethereum',
    project_filter = '0x'
) }}

-- Final union
-- oneinch_ethereum_project_swaps.sql
{{ config(materialized = 'view') }}
select * from {{ ref('oneinch_ethereum_project_swaps_1inch') }}
union all
select * from {{ ref('oneinch_ethereum_project_swaps_0x') }}
-- ... other projects
```

**Pros:**
- Logical separation by project
- Easy to add new projects
- Clear ownership

**Cons:**
- May have uneven data distribution
- Cross-project queries more complex


## Recommendation Matrix

| Solution | Implementation Effort | Performance Impact | Maintenance | Best For |
|----------|---------------------|-------------------|-------------|----------|
| 1. Incremental mapped_contracts | Low | Low | Low | Forward-only contracts |
| 2. Active contracts view | Low | Low | Medium | Staging new contracts |
| 3. Seeds configuration | Medium | Low | Low | Stable contract sets |
| 4. Partition-based refresh | High | High | High | Large-scale operations |
| 5. Backfill models | Medium | Medium | Medium | Occasional additions |
| 6. Time-bounded backfill | Medium | Medium | High | Very large historical data |
| 7. Smart merge | Medium | High | Medium | Data integrity critical |
| 8. **Multi-stage batching** | High | Very High | Medium | **Frequent additions** |
| 9. Project separation | High | High | Low | Multi-project setups |

## Recommended Approach

For frequent contract additions with historical data requirements:

1. **Immediate**: Implement Solution #8 (Multi-stage batching)
   - Start with 3-4 logical batches
   - Keep "new" batch for recent additions
   - Minimal impact on existing contracts

2. **Enhancement**: Add Solution #3 (Seeds) for configuration
   - Easier contract management
   - Version controlled changes

This combination provides:
- Isolation of changes
- Efficient processing
- Clear organization
- Minimal production impact