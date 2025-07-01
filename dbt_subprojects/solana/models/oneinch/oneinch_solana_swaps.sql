{{
    config(
        schema = 'oneinch_solana',
        alias = 'swaps',
        partition_by = ['block_month'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
        unique_key = ['block_month', 'unique_key']
    )
}}



with 

orders as (
    select * from {{ ref('oneinch_solana_fusion_created_orders') }}
    where 
        {% if is_incremental() %}
            {{ incremental_predicate('block_time') }}
        {% else %}
            block_time >= date('{{ oneinch_cfg_macro("project_start_date") }}')
        {% endif %}
)

, transfers as (
    select * from {{ ref('oneinch_solana_transfers') }}
    where 
        {% if is_incremental() %}
            {{ incremental_predicate('block_time') }}
        {% else %}
            block_time >= date('{{ oneinch_cfg_macro("project_start_date") }}')
        {% endif %}
)

, transactions as (
    select * from {{ source('solana', 'transactions') }}
    where 
        {% if is_incremental() %}
            {{ incremental_predicate('block_time') }}
        {% else %}
            block_time >= date('{{ oneinch_cfg_macro("project_start_date") }}')
        {% endif %}
)

, amounts as (
    select 
        t.tx_id
        , t.block_slot
        , t.order_hash
        , t.call_trace_address
        , t.taker
        , sum(if(src_mint = token_mint_address, amount)) as src_amount
        , sum(if(dst_mint = token_mint_address, amount)) as dst_amount
        , sum(if(src_mint = token_mint_address, amount_usd)) as src_amount_usd
        , sum(if(dst_mint = token_mint_address, amount_usd)) as dst_amount_usd
        , max(if(src_mint = token_mint_address, symbol)) as src_symbol
        , max(if(dst_mint = token_mint_address, symbol)) as dst_symbol
    from transfers as t -- {{ ref('oneinch_solana_transfers') }}
    join orders as o on t.order_hash = o.order_hash and t.block_month = o.block_month
    group by 1, 2, 3, 4, 5
)
--test_schema.git_dunesql_b75674d_oneinch_solana_fusion_created_orders

select
    'solana' as blockchain
    , tx.block_slot
    , tx.block_time
    , amounts.tx_id
    , tx.signer as tx_signer
    , call_trace_address
    , 1 as tx_gas_used -- as this one likely used to claculate gas_cost = gas_used * gas_price, we can't put "consumed_compute_units" here
    , 5000*cardinality(tx.signers) as tx_gas_price -- TODO: check on logic correctness
    , tx.fee - 5000*cardinality(tx.signers) as tx_priority_fee_per_gas -- https://solana.com/ru/developers/guides/advanced/how-to-use-priority-fees
    , tx.fee
    , program_name
    , o.version
    , taker as resolver
    , maker as user
    , escrow
    , maker_receiver
    , order_hash
    , order_hash_base58
    , order_id
    , order_src_amount as order_src_token_amount
    , order_min_dst_amount as order_min_dst_token_amount
    , order_estimated_dst_amount as order_estimated_dst_token_amount
    , order_expiration_time
    , order_src_asset_is_native
    , order_dst_asset_is_native
    , src_mint as src_token_mint
    , src_symbol as src_token_symbol
    , src_amount as src_token_amount
    , src_amount_usd as src_token_amount_usd
    , dst_mint as dst_token_mint
    , dst_symbol as dst_token_symbol
    , dst_amount as dst_token_amount
    , dst_amount_usd as dst_token_amount_usd
    , coalesce(src_amount_usd, dst_amount_usd) as amount_usd
    , {{dbt_utils.generate_surrogate_key(["blockchain", "order_hash", "amounts.tx_id", "array_join(call_trace_address, ',')"])}} as unique_key
    , cast(date_trunc('month', tx.block_time) as date) as block_month
from orders as o
join amounts using(order_hash)
join transactions as tx on amounts.tx_id = tx.id and amounts.block_slot = tx.block_slot
