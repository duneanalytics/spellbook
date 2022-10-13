{{ config(
        schema='prices',
        alias ='prices_eth',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        post_hook='{{ expose_spells(\'["ethereum", "solana", "arbitrum", "gnosis", "optimism", "bnb", "avalanche_c"]\',
                                    "sector",
                                    "prices",
                                    \'["msilb7"]\') }}'
        )
}}

SELECT
  pusd.blockchain
, pusd.contract_address
, pusd.decimals
, pusd.minute
, pusd.price / peth.price AS price_eth
, pusd.symbol
FROM (
    SELECT blockchain
    , contract_address
    , minute
    , price
    , decimals
    , symbol
    FROM {{ source('prices', 'usd') }}
    {% if is_incremental() %}
    WHERE minute >= date_trunc("day", now() - interval '1 week')
    {% endif %}
    ) pusd
INNER JOIN {{ source('prices', 'usd') }} peth
    ON peth.blockchain IS NULL
    AND peth.symbol = 'ETH'
    AND peth.minute= pusd.minute

{% if is_incremental() %}
WHERE peth.minute >= date_trunc("day", now() - interval '1 week')
{% endif %}

