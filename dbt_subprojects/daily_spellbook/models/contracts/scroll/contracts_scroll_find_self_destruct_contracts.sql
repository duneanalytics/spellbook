{{
  config(
    schema = 'contracts_scroll',
    alias = 'find_self_destruct_contracts',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['blockchain', 'contract_address', 'created_tx_hash']
  )
}}

WITH self_destructs AS (
    SELECT
        c.block_time AS destructed_time
        , c.block_number AS destructed_block_number
        , c."from" AS self_destruct_initiator
        , c.hash AS destructed_tx_hash
        , t.address AS contract_address
        , 'scroll' AS blockchain
    FROM {{ source('scroll', 'transactions') }} c
    INNER JOIN {{ source('scroll', 'traces') }} t
        ON c.hash = t.tx_hash
        AND t.type = 'suicide'
        {% if is_incremental() %}
        AND t.block_time >= date_trunc('day', now() - interval '7' day)
        {% endif %}
    WHERE TRUE
        {% if is_incremental() %}
        AND c.block_time >= date_trunc('day', now() - interval '7' day)
        {% endif %}
)

, contract_creates as (
SELECT 
    c.address as contract_address
    , c.block_time AS created_time
    , c.block_number AS created_block_number
    , c.tx_hash AS created_tx_hash
    , 'scroll' AS blockchain
FROM {{ source('scroll', 'traces') }} c
WHERE c.type = 'create'
    AND c.address IS NOT NULL
    AND c.tx_success
    {% if is_incremental() %}
    AND c.block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}
)

SELECT
  cc.blockchain
  , cc.created_time
  , cc.created_block_number
  , cc.created_tx_hash
  , cc.contract_address
  , d.destructed_time
  , d.destructed_block_number
  , d.destructed_tx_hash
FROM contract_creates cc
INNER JOIN self_destructs d 
    ON d.contract_address = cc.contract_address
    AND d.blockchain = cc.blockchain 
