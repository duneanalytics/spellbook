{{
  config(
    schema = 'polymarket_polygon',
    alias = 'positions_raw',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    partition_by = ['month'],
    unique_key = ['address', 'token_address', 'token_id', 'month', 'day'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.day')]
  )
}}

WITH changed_balances AS (
    SELECT
        cast(date_trunc('day', block_time) as date) as day,
        address,
        contract_address as token_address,
        token_id,
        amount as balance,
        LEAD(CAST(block_time AS timestamp)) OVER (PARTITION BY contract_address, address, token_id ORDER BY block_time ASC) AS next_update_day
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (PARTITION BY DATE_TRUNC('day', block_time), address, contract_address, token_id 
                                  ORDER BY block_time DESC) as rn
        FROM {{ source('tokens_polygon', 'balances_polygon') }}
        WHERE block_time < DATE(DATE_TRUNC('day', NOW())) 
          AND type = 'erc1155' 
          AND contract_address = 0x4D97DCd97eC945f40cF65F87097ACe5EA0476045
          AND block_time > TIMESTAMP '2020-09-02 00:00:00'
    ) ranked
    WHERE rn = 1 -- picking the latest balance for each day
),

days AS (
    SELECT *
    FROM UNNEST(
        SEQUENCE(CAST('2015-01-01' AS date), DATE(DATE_TRUNC('day', NOW())), INTERVAL '1' day)
    ) AS foo(day)
),

forward_fill AS (
    SELECT
        CAST(d.day AS date) AS day,
        address,
        token_address,
        token_id,
        balance
    FROM days d
    LEFT JOIN changed_balances b
        ON d.day > b.day
        AND (b.next_update_day IS NULL OR d.day < b.next_update_day)
),

balances AS (
    SELECT * 
    FROM forward_fill
    WHERE balance > 0
)

SELECT 
    date_trunc('month', day) as month,
    day,
    address,
    token_address,
    token_id,
    balance / 1e6 AS balance
FROM balances
WHERE 1=1 
{% if is_incremental() %}
AND {{ incremental_predicate('day') }}
{% endif %}
