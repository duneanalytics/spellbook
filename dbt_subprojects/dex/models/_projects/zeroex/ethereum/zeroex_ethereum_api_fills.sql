{{  config(
        schema = 'zeroex_ethereum',
        alias = 'api_fills',
        materialized='incremental',
        partition_by = ['block_month'],
        unique_key = ['block_date', 'tx_hash', 'evt_index'],
        on_schema_change='sync_all_columns',
        file_format ='delta',
        incremental_strategy='merge',
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

{% set zeroex_v3_start_date = '2019-12-01' %}
{% set zeroex_v4_start_date = '2021-01-06' %}
{% set blockchain = 'ethereum' %}
{% set table_name = 'zeroex_' ~ blockchain ~ '_Exchange_evt_Fill' %}

WITH zeroex_tx AS (
    {{
        zeroex_v1_txs(
            blockchain = blockchain,
            zeroex_v3_start_date = zeroex_v3_start_date
        )
    }}
),
v3_fills_no_bridge as (
    {{
        v3_fills_no_bridge(
            blockchain = blockchain,
            zeroex_v3_start_date = zeroex_v3_start_date
        )
    }}
),
v4_rfq_fills_no_bridge as (
    {{
        v4_rfq_fills_no_bridge(
            blockchain = blockchain,
            zeroex_v4_start_date = zeroex_v4_start_date
        )
    }}
),
v4_limit_fills_no_bridge as (
    {{
        v4_limit_fills_no_bridge(
            blockchain = blockchain,
            zeroex_v4_start_date = zeroex_v4_start_date
        )
    }}
),
otc_fills as (
    {{
        otc_fills(
            blockchain = blockchain,
            zeroex_v4_start_date = zeroex_v4_start_date
        )
    }}
),
ERC20BridgeTransfer as (
    {{
        ERC20BridgeTransfer(
            blockchain = blockchain,
            zeroex_v3_start_date = zeroex_v3_start_date
        )
    }}
),
BridgeFill as (
    {{
        BridgeFill(
            blockchain = blockchain,
            zeroex_v4_start_date = zeroex_v4_start_date
        )
    }} 
),
NewBridgeFill as (
    {{
        NewBridgeFill(
            blockchain = blockchain,
            zeroex_v4_start_date = zeroex_v4_start_date
        )
    }} 
),
direct_PLP as (
    {{
        direct_PLP(
            blockchain = blockchain,
            zeroex_v3_start_date = zeroex_v3_start_date
        )
    }}
),
direct_uniswapv2 as (
    {{
        direct_uniswapv2(
            blockchain = blockchain,
            zeroex_v3_start_date = zeroex_v3_start_date
        )
    }}
),
direct_sushiswap as (
    {{
        direct_sushiswap(
            blockchain = blockchain,
            zeroex_v3_start_date = zeroex_v3_start_date
        )
    }}
),
direct_uniswapv3 as (
    {{
        direct_uniswapv3(
            blockchain = blockchain,
            zeroex_v4_start_date = zeroex_v4_start_date
        )
    }}
),
all_tx AS (
    SELECT *
    FROM direct_uniswapv2
    UNION ALL SELECT *
    FROM direct_uniswapv3
    UNION ALL SELECT *
    FROM direct_sushiswap
    UNION ALL SELECT *
    FROM direct_PLP
    UNION ALL SELECT *
    FROM ERC20BridgeTransfer
    UNION ALL SELECT *
    FROM BridgeFill
    UNION ALL SELECT *
    FROM NewBridgeFill
    UNION ALL SELECT *
    FROM v3_fills_no_bridge
    UNION ALL SELECT *
    FROM v4_rfq_fills_no_bridge
    UNION ALL SELECT *
    FROM v4_limit_fills_no_bridge
    UNION ALL SELECT *
    FROM otc_fills
),
tbl_trade_details AS (
    {{
        trade_details(
            blockchain = blockchain,
            zeroex_v3_start_date = zeroex_v3_start_date
        )
    }}
)
select * from tbl_trade_details
order by block_time desc 