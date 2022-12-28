-- Add sources for decoded projects where trades may not happen daily
-- Project, Blockchain, Table Schema, Table Name, Time Column
{% set trade_sources = [
    {'project': 'fraxswap',
     'blockchain': 'avalanche_c',
     'schema': 'fraxswap_avalanche_c',
     'table_name': 'FraxswapPair_evt_Swap',
     'time_column': 'evt_block_time'},

    {'project': 'fraxswap',
     'blockchain': 'bnb',
     'schema': 'fraxswap_bnb',
     'table_name': 'FraxswapPair_evt_Swap',
     'time_column': 'evt_block_time'},

    {'project': 'dfx',
     'blockchain': 'ethereum',
     'schema': 'dfx_finance_ethereum',
     'table_name': 'Curve_evt_Trade',
     'time_column': 'evt_block_time'},

    {'project': 'hashflow',
     'blockchain': 'avalanche_c',
     'schema': 'hashflow_avalanche_c',
     'table_name': 'Pool_evt_Trade',
     'time_column': 'evt_block_time'},

    {'project': 'hashflow',
     'blockchain': 'ethereum',
     'schema': 'hashflow_ethereum',
     'table_name': 'pool_evt_trade',
     'time_column': 'evt_block_time'},

    {'project': 'zigzag',
     'blockchain': 'arbitrum',
     'schema': 'zigzag_test_v6_arbitrum',
     'table_name': 'zigzag_settelment_call_matchOrders',
     'time_column': 'call_block_time'}
] %}



with delays as (
    SELECT
        project
        , blockchain
        , datediff(now(), max(block_time)) age_of_last_record_days
    from {{ ref('dex_trades') }}
    group by 1,2
)

, sources as
(
{%  for trade_source in trade_sources %}
 select
        '{{ trade_source['project'] }}' as project
        , '{{ trade_source['blockchain'] }}' as blockchain
        , datediff(now(), max({{ trade_source['time_column'] }})) age_of_last_record_days
 from {{ source(trade_source['schema'],trade_source['table_name']) }}
 group by 1,2
{% if not loop.last %}
    UNION ALL
{% endif %}
{% endfor %}
)

select
    d.project,
    d.blockchain,
    coalesce(s.age_of_last_record_days, 0) - d.age_of_last_record_days as age_of_last_record_days_source_minus_table
from delays d
left join sources s
on d.project = s.project and d.blockchain = s.blockchain
where coalesce(s.age_of_last_record_days, 0) - d.age_of_last_record_days != 0
