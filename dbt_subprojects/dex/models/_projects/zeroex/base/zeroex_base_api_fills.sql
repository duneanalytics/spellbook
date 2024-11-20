{{  config(

    schema = 'zeroex_base',
        alias = 'api_fills',
        materialized='incremental',
        partition_by = ['block_month'],
        unique_key = ['block_date', 'tx_hash', 'evt_index'],
        on_schema_change='sync_all_columns',
        file_format ='delta',
        incremental_strategy='merge',
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
        post_hook='{{ expose_spells(\'["base"]\',
                                "project",
                                "zeroex",
                                \'["rantum"]\') }}'

    )
}}

{% set zeroex_v3_start_date = '2019-12-01' %}
{% set zeroex_v4_start_date = '2021-01-06' %}
{% set blockchain = 'base' %}

WITH zeroex_tx AS (
     SELECT tx_hash,
           block_time as block_time,
           max(affiliate_address) as affiliate_address,
           max(is_gasless) as is_gasless
    FROM (
    {{
        zeroex_v1_txs(
            blockchain = blockchain,
            zeroex_v3_start_date = zeroex_v3_start_date,
        )
    }}

    ) temp
    group by tx_hash, block_time 
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
all_tx AS (
    SELECT *
    FROM ERC20BridgeTransfer
    UNION ALL 
    SELECT *
    FROM BridgeFill
    UNION ALL
    SELECT *
    FROM NewBridgeFill
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