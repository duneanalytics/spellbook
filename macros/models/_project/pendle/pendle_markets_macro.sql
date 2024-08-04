{%
  macro pendle_markets(
    blockchain = '',
    project = '',
    version = '',
    project_decoded_as = ''
    create_market_table = '',
    create_yield_table = ''
  )
%}

with 
markets as (
    select 
        m.evt_block_time, m.evt_tx_hash, m.evt_index,
        '{{blockchain}}' as chain,
        '{{version}}' as version,
        m.market,
        m.PT,
        y.expiry,
        y.SY,
        y.YT 
    from {{ source(project_decoded_as ~ '_' ~ blockchain, create_market_table) }} m
        join {{ source(project_decoded_as ~ '_' ~ blockchain, create_yield_table) }} y 
        on m.PT = y.PT
    {% if is_incremental() %}
    where {{ incremental_predicate('m.evt_block_time') }}
    {% endif %} 
)

select * from markets