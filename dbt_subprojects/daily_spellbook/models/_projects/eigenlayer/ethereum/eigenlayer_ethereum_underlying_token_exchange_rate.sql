{{
    config(
        schema = 'eigenlayer_ethereum',
        alias = 'underlying_token_exchange_rate',
        materialized = 'table'
    )
}}


SELECT
  contract_address AS strategy,
  from_big_endian_64(substr(data, -8)) AS exchange_rate,
  block_number
FROM {{ source('ethereum', 'logs') }}
WHERE
    contract_address IN (
        SELECT
            strategy
        FROM {{ source('eigenlayer_ethereum', 'StrategyFactory_evt_StrategySetForToken') }}


        UNION


        SELECT
            strategy
        FROM {{ ref('eigenlayer_ethereum_strategy_category') }}
    )
    AND block_time > TIMESTAMP '2024-08-01'
    AND topic0 = 0xd2494f3479e5da49d386657c292c610b5b01df313d07c62eb0cfa49924a31be8
