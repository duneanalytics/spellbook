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

token_decimals as (
    select 
        distinct to_base58(contract_address) as token_mint, decimals
    from {{ source('prices_solana', 'tokens') }}
)

, orders as (
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
    select * from {{ ref('oneinch_solana_transactions') }}
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
        , t.method
        , sum(if(src_mint = token_mint_address, amount)) as src_amount
        , sum(if(dst_mint = token_mint_address, amount)) as dst_amount
        , sum(if(src_mint = token_mint_address, amount_usd)) as src_amount_usd
        , sum(if(dst_mint = token_mint_address, amount_usd)) as dst_amount_usd
        , max(if(src_mint = token_mint_address, symbol)) as src_symbol
        , max(if(dst_mint = token_mint_address, symbol)) as dst_symbol
        , sum(if(to_owner = taker, amount_usd)) as from_user_amount_usd
        , sum(if(to_owner = maker, amount_usd)) as to_user_amount_usd
        , count(distinct token_mint_address) as tokens
        , count(*) as transfers
    from transfers as t -- {{ ref('oneinch_solana_transfers') }}
    join orders as o on t.order_hash = o.order_hash and t.block_month = o.block_month
    group by 1, 2, 3, 4, 5, 6
)



select
    'solana' as blockchain
    , tx.block_slot
    , tx.block_time
    , amounts.tx_id
    , tx.signer as tx_signer
    , outer_executing_account
    , call_trace_address
    , 1 as tx_gas_used -- as this one likely used to claculate gas_cost = gas_used * gas_price, we can't put "consumed_compute_units" here
    , 5000*cardinality(tx.signers) as tx_gas_price -- TODO: check on logic correctness
    , tx.fee - 5000*cardinality(tx.signers) as tx_priority_fee_per_gas -- https://solana.com/ru/developers/guides/advanced/how-to-use-priority-fees
    , tx.fee
    , program_name
    , o.version
    , method
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
    , src_t.decimals as src_token_decimals
    , src_amount_usd as src_token_amount_usd
    , dst_mint as dst_token_mint
    , dst_symbol as dst_token_symbol
    , dst_amount as dst_token_amount
    , dst_t.decimals as dst_token_decimals
    , dst_amount_usd as dst_token_amount_usd
    , coalesce(from_user_amount_usd, src_amount_usd, to_user_amount_usd, dst_amount_usd) as amount_usd
    , from_user_amount_usd as sources_amount_usd
    , to_user_amount_usd as user_amount_usd
    , tokens
    , transfers
    , {{dbt_utils.generate_surrogate_key(["tx.blockchain", "order_hash", "amounts.tx_id", "array_join(call_trace_address, ',')"])}} as unique_key
    , cast(date_trunc('month', tx.block_time) as date) as block_month
from orders as o
join amounts using(order_hash)
join transactions as tx on amounts.tx_id = tx.id and amounts.block_slot = tx.block_slot
left join token_decimals as src_t on src_t.token_mint = src_mint
left join token_decimals as dst_t on dst_t.token_mint = dst_mint
