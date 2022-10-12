{{ config(
        alias ='trades',
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                "sector",
                                "dex",
                                \'["jeff-dude", "hosuke", "0xRob"]\') }}'
        )
}}
SELECT *
FROM
    (
        SELECT
                blockchain,
                project,
                version,
                block_date,
                block_time,
                token_bought_symbol,
                token_sold_symbol,
                token_pair,
                token_bought_amount,
                token_sold_amount,
                token_bought_amount_raw,
                token_sold_amount_raw,
                amount_usd,
                token_bought_address,
                token_sold_address,
                taker,
                maker,
                project_contract_address,
                tx_hash,
                tx_from,
                tx_to,
                trace_address,
                evt_index
        FROM {{ ref('uniswap_trades') }}

        UNION

        SELECT
                blockchain,
                project,
                version,
                block_date,
                block_time,
                token_bought_symbol,
                token_sold_symbol,
                token_pair,
                token_bought_amount,
                token_sold_amount,
                token_bought_amount_raw,
                token_sold_amount_raw,
                amount_usd,
                token_bought_address,
                token_sold_address,
                taker,
                maker,
                project_contract_address,
                tx_hash,
                tx_from,
                tx_to,
                trace_address,
                evt_index
        FROM {{ ref('curvefi_ethereum_trades') }}

        UNION

        SELECT
                blockchain,
                project,
                version,
                block_date,
                block_time,
                token_bought_symbol,
                token_sold_symbol,
                token_pair,
                token_bought_amount,
                token_sold_amount,
                token_bought_amount_raw,
                token_sold_amount_raw,
                amount_usd,
                token_bought_address,
                token_sold_address,
                taker,
                maker,
                project_contract_address,
                tx_hash,
                tx_from,
                tx_to,
                trace_address,
                evt_index
        FROM {{ ref('airswap_ethereum_trades') }}

        UNION

        SELECT
                blockchain,
                project,
                version,
                block_date,
                block_time,
                token_bought_symbol,
                token_sold_symbol,
                token_pair,
                token_bought_amount,
                token_sold_amount,
                token_bought_amount_raw,
                token_sold_amount_raw,
                amount_usd,
                token_bought_address,
                token_sold_address,
                taker,
                maker,
                project_contract_address,
                tx_hash,
                tx_from,
                tx_to,
                trace_address,
                evt_index
        FROM {{ ref('clipper_ethereum_trades') }}
)
