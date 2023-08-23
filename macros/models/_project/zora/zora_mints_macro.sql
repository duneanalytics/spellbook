{% macro 
    zora_mints(blockchain, erc721_mints, erc721_fee, erc721_zora_transfers, erc1155_mints, erc1155_royalties, zora_protocol_rewards) 
%}

WITH zora_mints AS (
    SELECT 'erc721' AS nft_type
    , s.evt_block_time AS block_time
    , s.evt_block_number AS block_number
    , s.quantity AS amount
    , t.to AS nft_recipient
    , COALESCE((s.pricePerToken+f.mintFeeAmount)/1e18, 0) AS price
    , COALESCE(f.mintFeeAmount/1e18, 0) AS marketplace_fee
    , f.mintFeeRecipient AS marketplace_fee_recipient
    , COALESCE(s.pricePerToken/1e18, 0) AS creator_fee
    , mintFeeRecipient AS creator_fee_recipient
    , s.contract_address AS nft_contract_address
    , s.firstPurchasedTokenId+1 AS nft_token_id
    , s.evt_tx_hash AS tx_hash
    , s.evt_index
    , NULL AS contract_address
    FROM {{erc721_mints}} s
    LEFT JOIN {{erc721_fee}} f ON f.evt_block_number=s.evt_block_number
        AND f.evt_tx_hash=s.evt_tx_hash
        AND f.success
        AND s.pricePerToken > CAST(0 AS UINT256)
    LEFT JOIN {{erc721_zora_transfers}} t ON t.evt_block_number=s.evt_block_number
        AND t.evt_tx_hash=s.evt_tx_hash
        AND t.contract_address=s.contract_address
        AND t.tokenId=s.firstPurchasedTokenId+1
    LEFT JOIN {{ source(blockchain, 'traces') }} traces ON traces.block_number=s.evt_block_number
        AND traces.tx_hash=s.evt_tx_hash
        AND traces."from"=s.evt_tx_hash
        AND traces.value > CAST(0 AS UINT256)
        AND (traces.call_type NOT IN ('delegatecall', 'callcode', 'staticcall') OR traces.call_type IS null)
    WHERE s.evt_block_time > NOW() - interval '7' day
    
    UNION ALL
    
    SELECT 'erc1155' AS nft_type
    , s.evt_block_time AS block_time
    , s.evt_block_number AS block_number
    , s.quantity AS amount
    , s.minter AS nft_recipient
    , s.value/1e18 AS price
    , CASE WHEN s.value/1e18 >= 0.000777 THEN 0.000777 ELSE 0 END AS marketplace_fee
    , from_hex(JSON_EXTRACT_SCALAR(r.configuration, '$.royaltyRecipient')) AS marketplace_fee_recipient
    , GREATEST((s.value/1e18)-0.000777, 0) AS creator_fee
    , CAST(NULL AS varbinary) AS creator_fee_recipient
    , s.contract_address AS nft_contract_address
    , s.tokenId AS nft_token_id
    , s.evt_tx_hash AS tx_hash
    , s.evt_index
    , s.sender AS contract_address
    FROM {{erc1155_mints}} s
    LEFT JOIN {{erc1155_royalties}} r ON r.contract_address=s.contract_address
        AND JSON_EXTRACT_SCALAR(r.configuration, '$.royaltyRecipient') != '0x0000000000000000000000000000000000000000'
    WHERE s.evt_block_time > NOW() - interval '7' day
    )

SELECT '{{blockchain}}' AS blockchain
, date_trunc('day', m.block_time) AS block_date
, m.block_time
, m.block_number
, txs."from" AS minter
, m.nft_recipient
, m.nft_type
, m.nft_contract_address
, m.nft_token_id
, m.amount
, COALESCE((deps.zoraReward+deps.creatorReward+deps.createReferralReward+deps.firstMinterReward+deps.mintReferralReward)/1e18, m.price) AS price
, m.tx_hash
, COALESCE(deps.zoraReward/1e18, m.marketplace_fee) AS marketplace_fee
, COALESCE(deps.zora, m.marketplace_fee_recipient) AS marketplace_fee_recipient
, COALESCE(deps.creatorReward/1e18, m.creator_fee) AS creator_fee
, COALESCE(deps.creator, m.creator_fee_recipient) AS creator_fee_recipient
, deps.createReferralReward/1e18 AS create_referral_reward
, deps.createReferral AS create_referral_reward_recipient
, deps.firstMinterReward/1e18 AS first_minter_reward
, deps.firstMinter AS first_minter_reward_recipient
, deps.mintReferralReward/1e18 AS mint_referral_reward
, deps.mintReferral AS mint_referral_reward_recipient
, m.evt_index
, m.contract_address
, CASE WHEN COALESCE(deps.zoraReward/1e18, m.marketplace_fee) = 0 THEN 1 -- WHEN marketplace fee was 0
    WHEN deps.creator IS NOT NULL THEN 3 -- New fee split, as detailed here: https://support.zora.co/en/articles/8192123-understanding-protocol-rewards-on-zora
    WHEN COALESCE(deps.zoraReward/1e18, m.marketplace_fee) = 0.000777 THEN 2 -- WHEN marketplace fee was always 0.000777
    END AS rewards_version
FROM zora_mints m
LEFT JOIN {{zora_protocol_rewards}} deps ON deps.call_block_number=m.block_number
    AND deps.call_tx_hash=m.tx_hash
    AND deps.call_success
    AND deps.creatorReward > CAST(0 AS UINT256)
    AND deps.creator != 0x0000000000000000000000000000000000000000
LEFT JOIN {{ source(blockchain, 'transactions') }} txs ON txs.block_number=m.block_number
    AND txs.hash=m.tx_hash

{% endmacro %}