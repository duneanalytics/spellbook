{{ config(
        alias ='trades',
        post_hook='{{ expose_spells(\'["ethereum", "gnosis"]\',
                                "sector",
                                "dex_aggregator",
                                \'["bh2smith"]\') }}'
        )
}}
SELECT *
FROM
(
    SELECT  blockchain
        ,project
        ,version
        ,block_date
        ,block_time
        ,token_bought_symbol
        ,token_sold_symbol
        ,token_pair
        ,token_bought_amount
        ,token_sold_amount
        ,token_bought_amount_raw
        ,token_sold_amount_raw
        ,amount_usd
        ,token_bought_address
        ,token_sold_address
        ,taker
        ,maker
        ,project_contract_address
        ,tx_hash
        ,tx_from
        ,tx_to
        ,trace_address
        ,evt_index
    FROM {{ ref('cow_protocol_trades') }}

    UNION ALL

    SELECT  blockchain
        ,project
        ,version
        ,block_date
        ,block_time
        ,maker_symbol           AS token_bought_symbol
        ,taker_symbol           AS token_sold_symbol
        ,token_pair
        ,maker_token_amount     AS token_bought_amount
        ,taker_token_amount     AS token_sold_amount
        ,maker_token_amount_raw AS token_bought_amount_raw
        ,taker_token_amount_raw AS token_sold_amount_raw
        ,amount_usd
        ,maker_token            AS token_bought_address
        ,taker_token            AS token_sold_address
        ,taker
        ,maker
        ,project_contract_address
        ,tx_hash
        ,tx_from
        ,tx_to
        ,trace_address
        ,evt_index
    FROM {{ ref('zeroex_api_ethereum_deduped_fills') }}
        /*
        UNION ALL
        <add future protocols here>
        */
)
