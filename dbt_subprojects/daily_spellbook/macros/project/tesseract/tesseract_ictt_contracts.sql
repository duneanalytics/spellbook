{%- macro tesseract_ictt_contracts(
        blockchain = null
    )
-%}

{%- set namespace_blockchain = 'tesseract_' + blockchain -%}

WITH
current_chain_id AS (
    SELECT
        ANY_VALUE(blockchainID) AS blockchain_id
    FROM {{ source('avalanche_teleporter_' + blockchain, 'TeleporterMessenger_evt_BlockchainIDInitialized')}}
),
distinct_ictt_contracts AS (
    SELECT
        DISTINCT
        CASE WHEN p.token_home_blockchain_id = c.blockchain_id THEN p.token_home_address ELSE p.token_remote_address END AS contract_address
        , CASE WHEN p.token_home_blockchain_id = c.blockchain_id THEN TRUE ELSE FALSE END AS is_token_home
    FROM {{ ref( namespace_blockchain + '_ictt_pairs') }} p
    CROSS JOIN current_chain_id c
    {%- if is_incremental() %}
    WHERE
        {{ incremental_predicate('p.block_time') }}
    {%- endif %}
)
SELECT
    '{{ blockchain }}' AS blockchain
    , c.contract_address
    , c.is_token_home
    , ct.block_time AS creation_block_time
    , ct.block_number AS creation_block_number
    , ct.tx_hash AS creation_tx_hash
FROM distinct_ictt_contracts c
INNER JOIN {{ source(blockchain, 'creation_traces') }} ct
    ON ct.address = c.contract_address
WHERE
    ct.block_time > TIMESTAMP '2024-01-01' -- Didn't have any ICTT contracts before this date

{%- endmacro -%}