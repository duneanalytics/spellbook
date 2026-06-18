{{  config(
    schema = 'zeroex_v2_avalanche_c',
    alias = 'trades',
    materialized='incremental',
    partition_by = ['block_month'],
    unique_key = ['block_month', 'block_date', 'tx_hash', 'evt_index'],
    on_schema_change='sync_all_columns',
    file_format ='delta',
    incremental_strategy='merge',
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
)}}

{% set zeroex_settler_start_date = '2024-07-15' %}
{% set blockchain = 'avalanche_c' %}

WITH zeroex_tx AS (
    -- Read the pre-materialized settler transactions instead of inlining the
    -- zeroex_settler_txs_cte macro, which Trino re-expanded into ~14 avalanche_c.traces scans.
    select
        tx_hash,
        block_time,
        block_number,
        method_id,
        contract_address,
        settler_address,
        zid,
        tag,
        rn,
        cow_rn,
        taker
    from {{ ref('zeroex_v2_avalanche_c_settler_txs') }}
    {% if is_incremental() %}
    where {{ incremental_predicate('block_time') }}
    {% endif %}
),
zeroex_v2_trades AS (
    {{
        zeroex_v2_trades(
            blockchain = blockchain,
            start_date = zeroex_settler_start_date
            
        )
    }}
),

trade_details as (
    {{
        zeroex_v2_trades_detail(
            blockchain = blockchain,
            start_date = zeroex_settler_start_date
            
        )
    }}

)
select 
    *
 from trade_details 
