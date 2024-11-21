{{ config(
    
    schema = 'tigris_arbitrum',
    alias = 'options_fees_distributed',
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.evt_block_time')],
    unique_key = ['evt_block_time', 'evt_tx_hash', 'evt_index']
    )
}}

WITH 
{% set fees_distributed_v2_evt_tables = [
    'options_evt_OptionsFeesDistributed',
    'Options_V2_evt_OptionsFeesDistributed',
    'Options_V3_evt_OptionsFeesDistributed'
] %}

fees_v2 AS (
    {% for fees_evt in fees_distributed_v2_evt_tables %}
        SELECT
            '{{ 'v2.' + loop.index | string }}' as version,
            '2' as protocol_version,
            CAST(date_trunc('DAY', evt_block_time) AS date) as day, 
            CAST(date_trunc('MONTH', evt_block_time) AS date) as block_month, 
            evt_block_time,
            evt_index,
            CASE WHEN evt_tx_hash = 0xf0a5193fc41599987645f183ae0c3a8311da02ebc9e4ee136edcfd4916133e78 THEN CAST(evt_index as double) + 2 ELSE -1 END as evt_join_index,
            evt_tx_hash,
            botFees/1e18 + daoFees/1e18 + refFees/1e18 as fees,
            contract_address as project_contract_address
        FROM {{ source('tigristrade_v2_arbitrum', fees_evt) }}
        {% if is_incremental() %}
        WHERE {{ incremental_predicate('evt_block_time') }}
        {% endif %}
        {% if not loop.last %}
        UNION ALL
        {% endif %}
    {% endfor %}
)

SELECT * FROM fees_v2 