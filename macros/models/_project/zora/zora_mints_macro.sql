{% macro 
    zora_mints(blockchain, erc721_mints, erc1155_mints) 
%}

SELECT '{{blockchain}}' AS blockchain
, evt_block_time AS block_time
, evt_block_number AS block_number
, 'erc721' AS token_standard
, firstPurchasedTokenId + sequence_element AS token_id
, pricePerToken/1e18/quantity AS total_price
, to AS recipient
, evt_tx_hash AS tx_hash
, evt_index
, contract_address
FROM {{erc721_mints}}
CROSS JOIN UNNEST(sequence(1, CAST(quantity AS BIGINT))) AS t (sequence_element)
{% if is_incremental() %}
WHERE evt_block_time >= date_trunc('day', now() - interval '7' day)
{% endif %}

UNION ALL

SELECT '{{blockchain}}' AS blockchain
, evt_block_time AS block_time
, evt_block_number AS block_number
, 'erc1155' AS token_standard
, tokenId AS token_id
, value/1e18/quantity AS total_price
, sender AS recipient
, evt_tx_hash AS tx_hash
, evt_index
, contract_address
FROM {{erc1155_mints}}
CROSS JOIN UNNEST(sequence(1, CAST(quantity AS BIGINT))) AS t (sequence_element)
{% if is_incremental() %}
WHERE evt_block_time >= date_trunc('day', now() - interval '7' day)
{% endif %}

{% endmacro %}