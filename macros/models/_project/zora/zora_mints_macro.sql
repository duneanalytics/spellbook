{% macro 
    zora_mints(blockchain, erc721_mints, erc1155_mints) 
%}

SELECT '{{blockchain}}' AS blockchain
, mints.evt_block_time AS block_time
, mints.evt_block_number AS block_number
, 'erc721' AS token_standard
, mints.firstPurchasedTokenId + t.sequence_element AS token_id
, 1 AS quantity
, mints.pricePerToken/1e18/mints.quantity AS total_price
, mints.to AS recipient
, mints.evt_tx_hash AS tx_hash
, mints.evt_index
, mints.contract_address
, txs."from" AS tx_from
, txs.to AS tx_to
FROM {{erc721_mints}} mints
INNER JOIN {{transactions}} txs ON txs.block_number=mints.evt_block_number
        AND txs.hash=mints.evt_tx_hash
CROSS JOIN UNNEST(sequence(1, CAST(quantity AS BIGINT))) AS t(sequence_element)
{% if is_incremental() %}
WHERE evt_block_time >= date_trunc('day', now() - interval '7' day)
{% endif %}

UNION ALL

SELECT '{{blockchain}}' AS blockchain
, mints.evt_block_time AS block_time
, mints.evt_block_number AS block_number
, 'erc1155' AS token_standard
, mints.tokenId AS token_id
, mints.quantity
, mints.value/1e18 AS total_price
, mints.sender AS recipient
, mints.evt_tx_hash AS tx_hash
, mints.evt_index
, mints.contract_address
, txs."from" AS tx_from
, txs.to AS tx_to
FROM {{erc1155_mints}} mints
INNER JOIN {{transactions}} txs ON txs.block_number=mints.evt_block_number
        AND txs.hash=mints.evt_tx_hash
{% if is_incremental() %}
WHERE evt_block_time >= date_trunc('day', now() - interval '7' day)
{% endif %}

{% endmacro %}