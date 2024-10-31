{{ config(
    schema = 'solana_utils'
    , alias = 'token_address_mapping'
    , materialized = 'table'
    , file_format = 'delta'
    , unique_key = 'base58_address'
) }}

WITH distinct_tokens AS (
    SELECT DISTINCT
        contract_address
        , symbol
    FROM 
        {{ source('prices', 'usd') }}
    WHERE 
        blockchain = 'solana'
        {% if is_incremental() %} 
        AND {{ incremental_predicate('minute') }}
        {% endif %}
)
SELECT
    symbol
    , contract_address AS binary_address
    , to_base58(contract_address) AS base58_address
FROM 
    distinct_tokens