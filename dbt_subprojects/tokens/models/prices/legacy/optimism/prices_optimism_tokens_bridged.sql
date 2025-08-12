{{ config(
        schema = 'prices_optimism',
        alias = 'tokens_bridged',
        materialized = 'table',
        file_format = 'delta',
        
        )
}}
with tokens as (
    SELECT
        e.token_id
        , 'optimism' as blockchain
        , e.symbol as symbol
        , o.l2_token as contract_address
        , e.decimals
        , row_number() over (partition by o.l2_token order by e.token_id) as rn
    FROM {{ source('tokens_optimism', 'erc20_bridged_mapping') }} o
    INNER JOIN {{ ref('prices_ethereum_tokens') }} e
    ON e.contract_address = o.l1_token
)
select *
from tokens
where rn = 1