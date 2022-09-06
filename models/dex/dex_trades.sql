{{ config(
    alias = 'trades',
    partition_by = ['block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'unique_trade_id']
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
                ,unique_trade_id
        FROM {{ ref('uniswap_trades') }}
        {% if is_incremental() %}
        WHERE block_date >= date_trunc("day", now() - interval '1 week')
        {% endif %}
        /*
        UNION
        <add future protocols here>
        */
)