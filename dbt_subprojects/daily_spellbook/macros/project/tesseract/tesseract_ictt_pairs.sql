{%- macro tesseract_ictt_pairs(
        blockchain = null
    )
-%}

{%- set namespace_blockchain = 'avalanche_teleporter_' + blockchain -%}

WITH
current_chain_id AS (
    SELECT
        ANY_VALUE(blockchainID) AS blockchain_id
    FROM {{ source(namespace_blockchain, 'TeleporterMessenger_evt_BlockchainIDInitialized')}}
),
new_token_home_links AS (
    SELECT
        c.blockchain_id AS token_home_blockchain_id
        , l.contract_address AS token_home_address
        , l.topic1 AS token_remote_blockchain_id
        , varbinary_substring(l.topic2, 13) AS token_remote_address
        , l.block_time
        , l.block_number
    FROM current_chain_id c
    CROSS JOIN {{ source(blockchain, 'logs') }} l
    WHERE
        -- Below value is equal to keccak(to_utf8('RemoteRegistered(bytes32,address,uint256,uint8)'))
        -- Hard-coded for efficiency
        l.topic0 = 0xf229b02a51a4c8d5ef03a096ae0dd727d7b48b710d21b50ebebb560eef739b90
        AND l.block_time > TIMESTAMP '2024-01-01' -- Safe to use this to reduce the size of the logs table
        {%- if is_incremental() %}
        AND {{ incremental_predicate('l.block_time') }}
        {%- endif %}
),
new_token_remote_links AS (
    SELECT
        s.destinationBlockchainID AS token_home_blockchain_id
        , from_hex(json_extract_scalar(s.message, '$.destinationAddress')) AS token_home_address
        , c.blockchain_id AS token_remote_blockchain_id
        , from_hex(json_extract_scalar(s.message, '$.originSenderAddress')) AS token_remote_address
        , MIN(s.evt_block_time) AS block_time
        , MIN(s.evt_block_number) AS block_number
    FROM current_chain_id c
    CROSS JOIN {{ source(namespace_blockchain, 'TeleporterMessenger_evt_SendCrossChainMessage') }} s
    INNER JOIN {{ source(blockchain, 'traces')}} t
        ON t.block_time = s.evt_block_time
        AND t.block_number = s.evt_block_number
        AND t.tx_hash = s.evt_tx_hash
    WHERE
        -- Below value is equal to varbinary_substring(keccak(to_utf8('registerWithHome((address,uint256))')), 1, 4) (function signature)
        -- Hard-coded for efficiency
        VARBINARY_SUBSTRING(t.input, 1, 4) = 0xb8a46d02
        AND t.block_time > TIMESTAMP '2024-01-01' -- Safe to use this to reduce the size of the traces table
        {%- if is_incremental() %}
        AND {{ incremental_predicate('t.block_time') }}
        AND {{ incremental_predicate('s.evt_block_time') }}
        {%- endif %}
    GROUP BY 1, 2, 3, 4
)
SELECT
    '{{ blockchain }}' AS blockchain
    , token_home_blockchain_id
    , token_home_address
    , token_remote_blockchain_id
    , token_remote_address
    , block_time
    , block_number
FROM (
    SELECT * FROM new_token_home_links
    UNION ALL
    SELECT n.* FROM new_token_remote_links n
    {%- if is_incremental() %}
    LEFT JOIN {{ this }} t
        ON n.token_home_blockchain_id = t.token_home_blockchain_id
        AND n.token_home_address = t.token_home_address
        AND n.token_remote_blockchain_id = t.token_remote_blockchain_id
        AND n.token_remote_address = t.token_remote_address
    WHERE t.block_time IS NULL -- ensuring there are not already duplicate records for the registerWithHome calls (which is possible as it can be called however many times)
    {%- endif %}
)

{%- endmacro -%}