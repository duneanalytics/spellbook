{{ config(
        alias='trades',
        post_hook='{{ expose_spells(\'["ethereum", "gnosis", "avalanche_c"]\',
                        "project",
                        "sushiswap",
                        \'["augustog", "hosuke"]\') }}'
        )
}}

SELECT *
FROM
(
        SELECT
                blockchain
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
        FROM {{ ref('sushiswap_ethereum_trades') }}

        UNION

        SELECT
                blockchain
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
        FROM {{ ref('sushiswap_avalanche_c_trades') }}

        UNION

        SELECT
                blockchain
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
        FROM {{ ref('sushiswap_gnosis_trades') }}
        /*
        UNION
        <add future protocols versions here>
        */
)