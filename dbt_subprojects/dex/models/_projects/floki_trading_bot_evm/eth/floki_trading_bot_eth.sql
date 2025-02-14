{{
    config(
        alias='bot_trades',
        schema='flokibot_base',
        partition_by=['block_month'],
        materialized='incremental',
        file_format='delta',
        incremental_strategy='merge',
        incremental_predicates=[
            incremental_predicate('DBT_INTERNAL_DEST.block_time')
        ],
        unique_key=['blockchain', 'tx_hash', 'tx_from'],
    )
}}


SELECT *
FROM dex_trades
WHERE tx_hash IN (
    SELECT DISTINCT tx_hash
    FROM transfers_ethereum_eth
    WHERE "to" = 0xfFDc626bb733A8C2e906242598E2e99752DCb922

);
