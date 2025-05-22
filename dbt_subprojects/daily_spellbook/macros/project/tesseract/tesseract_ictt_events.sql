{%- macro tesseract_ictt_events(
        blockchain = null,
        event_name = null,
        topic0_filter = null,
        topic1_name = 'teleporterMessageID',
        topic1_type = 'bytes32'
    )
-%}

{%- set namespace_blockchain = 'tesseract_' + blockchain -%}
{%- set log_fields = ["tx_hash", "index", "block_time", "block_number", "block_date"] -%}

{%- set additional_columns -%}
    {%- if topic1_type == 'address' -%}
    , varbinary_substring(l.topic1, 13) AS {{ topic1_name }}
    {%- else -%}
    , l.topic1 AS {{ topic1_name }}
    {%- endif %}
    {%- if event_name in ["TokensWithdrawn", "CallSucceeded", "CallFailed"] %}
    , TRY(varbinary_to_uint256(l.data)) AS amount -- We wrap this in a TRY just in case there is an ABI which uses different indexing (very unlikely)
    {%- else %}
        {%- if event_name in ["TokensSent", "TokensAndCallSent"] %}
    , varbinary_substring(l.topic2, 13) AS sender
        {%- endif %}
        {%- if event_name in ["TokensSent", "TokensRouted"] %}
    , varbinary_substring(l.data, 1, 32) AS destinationBlockchainID
    , varbinary_substring(l.data, 45, 20) AS destinationTokenTransferrerAddress
    , varbinary_substring(l.data, 77, 20) AS recipient
    , varbinary_substring(l.data, 109, 20) AS primaryFeeTokenAddress
    , TRY(varbinary_to_uint256(varbinary_substring(l.data, 129, 32))) AS primaryFee
    , TRY(varbinary_to_uint256(varbinary_substring(l.data, 161, 32))) AS secondaryFee
    , TRY(varbinary_to_uint256(varbinary_substring(l.data, 193, 32))) AS requiredGasLimit
    , varbinary_substring(l.data, 237, 20) AS multiHopFallback
    , TRY(varbinary_to_uint256(varbinary_substring(l.data, 257, 32))) AS amount
        {%- else %}
    , varbinary_substring(l.data, 65, 32) AS destinationBlockchainID
    , varbinary_substring(l.data, 109, 20) AS destinationTokenTransferrerAddress
    , varbinary_substring(l.data, 141, 20) AS recipientContract
    , TRY(varbinary_substring(l.data, 449, CAST(varbinary_to_uint256(varbinary_substring(l.data, 417, 32)) AS bigint))) AS recipientPayload
    , TRY(varbinary_to_uint256(varbinary_substring(l.data, 193, 32))) AS requiredGasLimit
    , TRY(varbinary_to_uint256(varbinary_substring(l.data, 225, 32))) AS recipientGasLimit
    , varbinary_substring(l.data, 269, 20) AS multiHopFallback
    , varbinary_substring(l.data, 301, 20) AS fallbackRecipient
    , varbinary_substring(l.data, 333, 20) AS primaryFeeTokenAddress
    , TRY(varbinary_to_uint256(varbinary_substring(l.data, 353, 32))) AS primaryFee
    , TRY(varbinary_to_uint256(varbinary_substring(l.data, 385, 32))) AS secondaryFee
    , TRY(varbinary_to_uint256(varbinary_substring(l.data, 33, 32))) AS amount
        {%- endif -%}
    {%- endif -%}
{%- endset -%}

SELECT
    '{{ blockchain }}' AS blockchain
    , l.contract_address
    {%- for log_field in log_fields %}
    , l.{{ log_field }} AS evt_{{ log_field }}
    {%- endfor %}
    {{ additional_columns }}
FROM {{ ref(namespace_blockchain + '_ictt_contracts') }} c
INNER JOIN {{ source(blockchain, 'logs') }} l
    ON l.contract_address = c.contract_address
WHERE
    topic0 = {{ topic0_filter }}
    AND l.block_time > TIMESTAMP '2024-01-01' -- Safe to use this to reduce the size of the logs table as there weren't any ICTT contracts before this
    {%- if is_incremental() %}
    AND {{ incremental_predicate('l.block_time') }}
    {%- endif -%}

{%- endmacro -%}