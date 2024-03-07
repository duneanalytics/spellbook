{% macro 
    zora_mints(blockchain, wrapped_native_token_address, erc721_mints, erc1155_mints, transactions) 
%}

SELECT '{{blockchain}}' AS blockchain
, mints.evt_block_time AS block_time
, mints.evt_block_number AS block_number
, 'erc721' AS token_standard
, mints.firstPurchasedTokenId + t.sequence_element AS token_id
, 1 AS quantity
, mints.pricePerToken/1e18/mints.quantity AS total_price
, pu.price*(mints.pricePerToken/1e18/mints.quantity) AS total_price_usd
, mints.to AS recipient
, mints.evt_tx_hash AS tx_hash
, mints.evt_index
, mints.contract_address
, txs."from" AS tx_from
, txs.to AS tx_to
FROM {{erc721_mints}} mints
INNER JOIN {{transactions}} txs ON txs.block_number=mints.evt_block_number
        AND txs.hash=mints.evt_tx_hash
        {% if is_incremental() %}
        AND {{incremental_predicate('txs.block_time')}}
        {% endif %}
INNER JOIN {{ ref('prices_usd_forward_fill') }} pu ON pu.blockchain='{{blockchain}}'
        AND pu.contract_address= {{wrapped_native_token_address}}
        AND pu.minute=date_trunc('minute', mints.evt_block_time)
        {% if is_incremental() %}
        AND {{incremental_predicate('pu.minute')}}
        {% endif %}
CROSS JOIN UNNEST(sequence(1, CAST(quantity AS BIGINT))) AS t(sequence_element)
{% if is_incremental() %}
WHERE {{incremental_predicate('mints.evt_block_time')}}
{% endif %}

UNION ALL

SELECT '{{blockchain}}' AS blockchain
, mints.evt_block_time AS block_time
, mints.evt_block_number AS block_number
, 'erc1155' AS token_standard
, mints.tokenId AS token_id
, mints.quantity
, mints.value/1e18 AS total_price
, pu.price*(mints.value/1e18) AS total_price_usd
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
        AND {{incremental_predicate('txs.block_time')}}
        {% endif %}
INNER JOIN {{ ref('prices_usd_forward_fill') }} pu ON pu.blockchain='{{blockchain}}'
        AND pu.contract_address= {{wrapped_native_token_address}}
        AND pu.minute=date_trunc('minute', mints.evt_block_time)
        {% if is_incremental() %}
        AND {{incremental_predicate('pu.minute')}}
        {% endif %}
{% if is_incremental() %}
WHERE {{incremental_predicate('mints.evt_block_time')}}
{% endif %}

{% endmacro %}