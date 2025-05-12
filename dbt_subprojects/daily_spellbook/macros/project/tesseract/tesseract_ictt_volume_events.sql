{%- macro tesseract_ictt_volume_events(
        blockchain = null
    )
-%}

{%- set namespace_blockchain = 'tesseract_' + blockchain -%}
{%- set volume_events = ["TokensSent", "TokensAndCallSent", "TokensRouted", "TokensAndCallRouted", "TokensWithdrawn", "CallSucceeded", "CallFailed"] -%}

SELECT
    '{{ blockchain }}' AS blockchain
    , *
FROM (
    {%- for event_name in volume_events %}
    SELECT
        e.contract_address
        , e.evt_tx_hash
        , e.evt_index
        , e.evt_block_time
        , e.evt_block_number
        , e.evt_block_date
        , '{{ event_name }}' AS evt_name
        , e.amount
        {%- if event_name in ["TokensSent", "TokensAndCallSent"] %}
        , CASE WHEN cr.contract_address IS NOT NULL THEN TRUE ELSE FALSE END AS used_tesseract
        {%- elif event_name in ["CallSucceeded", "CallFailed"] %}
        , CASE WHEN c.address IS NOT NULL THEN TRUE ELSE FALSE END AS used_tesseract
        {%- else %}
        , NULL AS used_tesseract
        {%- endif %}
        {%- if event_name in ["TokensSent", "TokensAndCallSent"] %}
        , NULL AS source_blockchain_id
        {%- else %}
        , r.sourceBlockchainID AS source_blockchain_id
        {%- endif %}
        {%- if event_name in ["TokensWithdrawn", "CallSucceeded", "CallFailed"] %}
        , NULL AS destination_blockchain_id
        {%- else %}
        , e.destinationBlockchainID AS destination_blockchain_id
        {%- endif %}
    FROM {{ ref(namespace_blockchain + '_ictt_evt_' + event_name | lower) }} e
    {%- if event_name in ["TokensSent", "TokensAndCallSent"] %}
    LEFT JOIN {{ ref(namespace_blockchain + '_cell_routed') }} cr
        ON cr.messageID = e.teleporterMessageID
        {%- if is_incremental() %}
        AND {{ incremental_predicate('cr.evt_block_time') }}
        {%- endif %}
    {%- elif event_name in ["CallSucceeded", "CallFailed"] %}
    LEFT JOIN {{ source(blockchain, 'contracts') }} c
        ON c.address = e.recipientContract
        AND c.namespace = 'tesseract'
        AND c.name IN ('{{ tesseract_cell_types(blockchain) | join("', '") }}')
    {%- endif -%}
    {%- if event_name in ["TokensRouted", "TokensAndCallRouted", "TokensWithdrawn", "CallSucceeded", "CallFailed"] %}
    INNER JOIN {{ source('avalanche_teleporter_' + blockchain, 'TeleporterMessenger_evt_ReceiveCrossChainMessage') }} r -- This join assumes one message per transaction, which has been the case so far
        ON r.evt_tx_hash = e.evt_tx_hash
        {%- if is_incremental() %}
        AND {{ incremental_predicate('r.evt_block_time') }}
        {%- endif -%}
    {%- endif %}
    {%- if is_incremental() -%}
    WHERE
        {{ incremental_predicate('e.evt_block_time') }}
    {%- endif -%}
    {%- if not loop.last %}
    UNION ALL
    {%- endif -%}
    {%- endfor %}
)

{%- endmacro -%}