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
     'time_column': 'call_block_time'},

    {'project': 'mstable',
     'blockchain': 'ethereum',
     'schema': 'mstable_ethereum',
     'table_name': 'Masset_evt_Swapped',
     'time_column': 'evt_block_time'},

    {'project': 'Bancor Network',
     'blockchain': 'ethereum',
     'schema': 'bancornetwork_ethereum',
     'table_name': 'BancorNetwork_v10_evt_Conversion',
     'time_column': 'evt_block_time'}
] %}



with delays as (
    SELECT
        project
        , blockchain
        , date_diff('hour', max(block_time), now()) as age_of_last_record_hours
    from {{ ref('dex_trades') }}
    group by 1,2
)

, sources as
(
{%  for trade_source in trade_sources %}
 select
        '{{ trade_source['project'] }}' as project
        , '{{ trade_source['blockchain'] }}' as blockchain
        , date_diff('hour', max({{ trade_source['time_column'] }}), now()) as age_of_last_record_hours
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
    coalesce(s.age_of_last_record_hours, 0) - d.age_of_last_record_hours as age_of_last_record_hours_source_minus_table
from delays d
left join sources s
on d.project = s.project and d.blockchain = s.blockchain
where coalesce(s.age_of_last_record_hours, 0) - d.age_of_last_record_hours > 24
