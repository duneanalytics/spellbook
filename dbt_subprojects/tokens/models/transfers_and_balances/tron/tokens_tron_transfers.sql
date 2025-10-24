{{config(
    schema = 'tokens_tron',
    alias = 'transfers',
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date','unique_key'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_date')],
    post_hook='{{ expose_spells(\'["tron"]\',
                                "sector",
                                "tokens",
                                \'["0xRob"]\') }}'
)
}}

{% set transfers_start_date = '2018-10-11' %}

WITH base_transfers as (
    SELECT
        *
    FROM
        {{ ref('tokens_tron_base_transfers') }}
    WHERE
        1=1
        {% if is_incremental() -%}
        AND {{ incremental_predicate('block_date') }}
        {% else -%}
        AND block_date >= TIMESTAMP '{{ transfers_start_date }}'
        {% endif -%}
)
, prices AS (
    SELECT
        timestamp
        , blockchain
        , contract_address
        , decimals
        , symbol
        , price
    FROM
        {{ source('prices_coinpaprika', 'hour') }}    
    WHERE
        1=1
        {% if is_incremental() -%}
        AND {{ incremental_predicate('timestamp') }}
        {% else -%}
        AND timestamp >= TIMESTAMP '{{ transfers_start_date }}'
        {% endif -%}    
)
, trusted_tokens AS (
    SELECT
        blockchain
        , contract_address
    FROM {{ source('prices', 'trusted_tokens') }}
)
, transfers as (
    SELECT
        t.unique_key
        , t.blockchain
        , t.block_month
        , t.block_date
        , t.block_time
        , t.block_number
        , t.tx_hash
        , t.evt_index
        , t.trace_address
        , t.token_standard
        , t.tx_from
        , t.tx_to
        , t.tx_index
        , t."from"
        , t.to
        , t.contract_address        
        , t.tx_hash_varchar
        , t.contract_address_varchar
        , t.from_varchar
        , t.to_varchar
        , t.tx_from_varchar
        , t.tx_to_varchar        
        , coalesce(tokens_erc20.symbol, prices.symbol) AS symbol
        , t.amount_raw
        , t.amount_raw / power(10, coalesce(tokens_erc20.decimals, prices.decimals)) AS amount
        , prices.price AS price_usd
        , t.amount_raw / power(10, coalesce(tokens_erc20.decimals, prices.decimals)) * prices.price AS amount_usd
        , CASE WHEN trusted_tokens.blockchain IS NOT NULL THEN true ELSE false END AS is_trusted_token        
    FROM
        base_transfers as t
    LEFT JOIN
        {{ source('tokens', 'erc20') }} as tokens_erc20
        ON tokens_erc20.blockchain = t.blockchain
        AND tokens_erc20.contract_address = t.contract_address
    LEFT JOIN
        trusted_tokens
        ON trusted_tokens.blockchain = t.blockchain
        AND trusted_tokens.contract_address = t.contract_address
    LEFT JOIN
        prices
        ON date_trunc('hour', t.block_time) = prices.timestamp
        AND t.blockchain = prices.blockchain
        AND t.contract_address = prices.contract_address
)
, final as (
    SELECT
        unique_key
        , blockchain
        , block_month
        , block_date
        , block_time
        , block_number
        , tx_hash
        , evt_index
        , trace_address
        , token_standard
        , tx_from
        , tx_to
        , tx_index
        , "from"
        , to
        , contract_address        
        , tx_hash_varchar
        , contract_address_varchar
        , from_varchar
        , to_varchar
        , tx_from_varchar
        , tx_to_varchar        
        , symbol
        , amount_raw
        , amount
        , price_usd
        , CASE
            WHEN is_trusted_token = true THEN amount_usd
            WHEN (is_trusted_token = false AND amount_usd < 1000000000) THEN amount_usd
            WHEN (is_trusted_token = false AND amount_usd >= 1000000000) THEN CAST(NULL as double) /* ignore inflated outlier prices */
            END AS amount_usd
    FROM
        transfers
)
SELECT
    *
FROM
    final