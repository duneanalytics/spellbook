{{
    config(
        schema = 'oneinch',
        alias = 'lop_aggregator_trades',
        materialized = 'view',
        unique_key = ['blockchain', 'block_month', 'tx_hash', 'trace_address', 'evt_index'],
    )
}}

{% set src_symbol = "coalesce(src_executed_symbol, '')" %}
{% set dst_symbol = "coalesce(dst_executed_symbol, '')" %}
{% set stream = oneinch_lo_cfg_macro() %}



-- venue-settled LOP fills: the underlying venue's own row stays in dex.trades, so the
-- intent-layer fill is recorded here instead (user perspective, like oneinch_ar_trades).
-- evt_index comes persisted from the per-chain lop_venue_settled_fills models (same
-- numbering as the per-chain lop_own_trades views via oneinch_lop_evt_index()); keeping
-- this view window-free lets consumers' block_time predicates push down into the
-- oneinch_swaps scan
with venue_settled_fills as (
    {% for blockchain in oneinch_blockchains_cfg_macro() if blockchain.exposed and stream.name in blockchain.exposed %}
    select *
    from {{ ref('oneinch_' + blockchain.name + '_lop_venue_settled_fills') }}
    {% if not loop.last %}union all{% endif %}
    {% endfor %}
)

select
    f.blockchain
    , '1inch-LOP' as project
    , cast(f.protocol_version as varchar) as version
    , f.block_date
    , f.block_month
    , f.block_time
    , {{ dst_symbol }} as token_bought_symbol
    , {{ src_symbol }} as token_sold_symbol
    , case
        when lower({{ src_symbol }}) > lower({{ dst_symbol }}) then concat({{ dst_symbol }}, '-', {{ src_symbol }})
        else concat({{ src_symbol }}, '-', {{ dst_symbol }})
    end as token_pair
    , cast(f.dst_executed_amount as double) / pow(10, cast(element_at(f.complement, 'dst_decimals') as bigint)) as token_bought_amount
    , cast(f.src_executed_amount as double) / pow(10, cast(element_at(f.complement, 'src_decimals') as bigint)) as token_sold_amount
    , f.dst_executed_amount as token_bought_amount_raw
    , f.src_executed_amount as token_sold_amount_raw
    , f.amount_usd
    , f.dst_token_address as token_bought_address
    , f.src_token_address as token_sold_address
    , f.user as taker
    , cast(null as varbinary) as maker
    , f.call_to as project_contract_address
    , f.tx_hash
    , f.tx_from
    , f.tx_to
    , f.call_trace_address as trace_address
    , v.evt_index
from {{ ref('oneinch_swaps') }} as f
join venue_settled_fills as v
    on v.blockchain = f.blockchain
    and v.block_month = f.block_month
    and v.execution_id = f.execution_id
where true
    and f.mode = 'limits'
    and f.tx_success
    and f.call_success
