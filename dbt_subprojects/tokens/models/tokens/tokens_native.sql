{{ config(
        schema = 'tokens',
        alias='native',
        materialized = 'view'
        , post_hook='{{ hide_spells() }}'
    )
}}

SELECT
    ei.blockchain AS chain,
    ei.native_token_symbol AS symbol,
    erc20.symbol AS price_symbol,
    ei.wrapped_native_token_address AS price_address,
    erc20.decimals AS decimals
FROM
    {{ source('evms','info') }} AS ei
LEFT JOIN
    {{ source('tokens','erc20') }} AS erc20
ON
    ei.wrapped_native_token_address = erc20.contract_address
    AND ei.blockchain = erc20.blockchain
WHERE
    ei.wrapped_native_token_address IS NOT NULL
    AND ei.is_on_dune
