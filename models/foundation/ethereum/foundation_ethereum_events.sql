{{ config(
    alias = 'events',
    partition_by = ['block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'unique_trade_id'],
    post_hook='{{ expose_spells(\'["ethereum"]\',
                                "project",
                                "foundation",
                                \'["hildobby", "soispoke"]\') }}'
    )
}}

WITH all_foundation_trades AS (
    SELECT 'ethereum' AS blockchain
    , 'foundation' AS project
    , 'v1' AS version
    , f.evt_block_time AS block_time
    , f.evt_block_number AS block_number
    , c.tokenId AS token_id
    , 1 AS number_of_items
    , 'Auction Settled' AS trade_category
    , 'Trade' AS evt_type
    , f.seller
    , f.bidder AS buyer
    , (f.creatorRev+f.totalFees+f.sellerRev)/POWER(10, 18) AS amount_original
    , f.creatorRev+f.totalFees+f.sellerRev AS amount_raw
    , 'ETH' currency_symbol
    , '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' AS currency_contract
    , f.contract_address AS project_contract_address
    , c.nftContract AS nft_contract_address
    , f.evt_tx_hash AS tx_hash
    , CAST(f.totalFees AS DOUBLE) AS platform_fee_amount_raw
    , f.totalFees/POWER(10, 18) AS platform_fee_amount
    , f.creatorRev AS royalty_fee_amount_raw
    , f.creatorRev/POWER(10, 18) royalty_fee_amount
    , f.evt_index
    FROM {{ source('foundation_ethereum','market_evt_ReserveAuctionFinalized') }} f
    LEFT JOIN {{ source('foundation_ethereum','market_evt_ReserveAuctionCreated') }} c ON c.auctionId=f.auctionId AND c.evt_block_time<=f.evt_block_time
     {% if is_incremental() %} -- this filter will only be applied on an incremental run
     WHERE f.evt_block_time >= date_trunc("day", now() - interval '1 week')
     {% endif %}
    UNION ALL
    SELECT 'ethereum' AS blockchain
    , 'foundation' AS project
    , 'v1' AS version
    , evt_block_time AS block_time
    , evt_block_number AS block_number
    , tokenId AS token_id
    , 1 AS number_of_items
    , 'Buy' AS trade_category
    , 'Trade' AS evt_type
    , seller
    , buyer
    , (creatorRev+totalFees+sellerRev)/POWER(10, 18) AS amount_original
    , creatorRev+totalFees+sellerRev AS amount_raw
    , 'ETH' AS currency_symbol
    , '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' AS currency_contract
    , contract_address AS project_contract_address
    , nftContract AS nft_contract_address
    , evt_tx_hash AS tx_hash
    , totalFees AS platform_fee_amount_raw
    , totalFees/POWER(10, 18) AS platform_fee_amount
    , creatorRev AS royalty_fee_amount_raw
    , creatorRev/POWER(10, 18) AS royalty_fee_amount
    , evt_index
    FROM {{ source('foundation_ethereum','market_evt_BuyPriceAccepted') }} f
     {% if is_incremental() %} -- this filter will only be applied on an incremental run
     WHERE f.evt_block_time >= date_trunc("day", now() - interval '1 week')
     {% endif %}
    UNION ALL
    SELECT 'ethereum' AS blockchain
    , 'foundation' AS project
    , 'v1' AS version
    , evt_block_time AS block_time
    , evt_block_number AS block_number
    , tokenId AS token_id
    , 1 AS number_of_items
    , 'Buy' AS trade_category
    , 'Trade' AS evt_type
    , seller
    , buyer
    , (creatorRev+totalFees+sellerRev)/POWER(10, 18) AS amount_original
    , creatorRev+totalFees+sellerRev AS amount_raw
    , 'ETH' AS currency_symbol
    , '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' AS currency_contract
    , contract_address AS project_contract_address
    , nftContract AS nft_contract_address
    , evt_tx_hash AS tx_hash
    , totalFees AS platform_fee_amount_raw
    , totalFees/POWER(10, 18) AS platform_fee_amount
    , creatorRev AS royalty_fee_amount_raw
    , creatorRev/POWER(10, 18) AS royalty_fee_amount
    , evt_index
    FROM {{ source('foundation_ethereum','market_evt_OfferAccepted') }} f
     {% if is_incremental() %} -- this filter will only be applied on an incremental run
     WHERE f.evt_block_time >= date_trunc("day", now() - interval '1 week')
     {% endif %}
    UNION ALL
    SELECT 'ethereum' AS blockchain
    , 'foundation' AS project
    , 'v1' AS version
    , evt_block_time AS block_time
    , evt_block_number AS block_number
    , tokenId AS token_id
    , 1 AS number_of_items
    , 'Private Sale' AS trade_category
    , 'Trade' AS evt_type
    , seller
    , buyer
    , (creatorFee+protocolFee+sellerRev)/POWER(10, 18) AS amount_original
    , creatorFee+protocolFee+sellerRev AS amount_raw
    , 'ETH' AS currency_symbol
    , '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' AS currency_contract
    , contract_address AS project_contract_address
    , nftContract AS nft_contract_address
    , evt_tx_hash AS tx_hash
    , protocolFee AS platform_fee_amount_raw
    , protocolFee/POWER(10, 18) AS platform_fee_amount
    , creatorFee AS royalty_fee_amount_raw
    , creatorFee/POWER(10, 18) AS royalty_fee_amount
    , evt_index
    FROM {{ source('foundation_ethereum','market_evt_PrivateSaleFinalized') }} f
     {% if is_incremental() %} -- this filter will only be applied on an incremental run
     WHERE f.evt_block_time >= date_trunc("day", now() - interval '1 week')
     {% endif %}
    )

SELECT t.blockchain
, t.project
, version
, date_trunc('day', t.block_time) AS block_date
, t.block_time
, t.block_number
, t.token_id
, nft.name AS collection
, t.amount_original*pu.price AS amount_usd
, CASE WHEN nft_t.contract_address IS NOT NULL THEN 'erc721'
    ELSE 'erc1155'
    END AS token_standard
, CASE WHEN agg.contract_address IS NOT NULL THEN 'Bundle Trade'
    ELSE 'Single Item Purchase'
    END AS trade_type
, CAST(t.number_of_items AS DECIMAL(38,0)) AS number_of_items
, t.trade_category
, t.evt_type
, t.seller
, t.buyer
, t.amount_original
, CAST(t.amount_raw AS DECIMAL(38,0)) AS amount_raw
, t.currency_symbol
, t.currency_contract
, t.project_contract_address
, t.nft_contract_address
, agg.name AS aggregator_name
, agg.contract_address aggregator_address
, t.tx_hash
, et.from AS tx_from
, et.to AS tx_to
, CAST(t.platform_fee_amount_raw AS DOUBLE) AS platform_fee_amount_raw
, t.platform_fee_amount
, t.platform_fee_amount*pu.price AS platform_fee_amount_usd
, CAST(100.0*ROUND(t.platform_fee_amount/t.amount_original, 2) AS DOUBLE) AS platform_fee_percentage
, CASE WHEN t.royalty_fee_amount/t.amount_original < 0.5 THEN CAST(t.royalty_fee_amount_raw AS DOUBLE)
    ELSE CAST(0 AS DOUBLE)
    END AS royalty_fee_amount_raw
, CASE WHEN t.royalty_fee_amount/t.amount_original < 0.5 THEN t.royalty_fee_amount
    ELSE 0
    END AS royalty_fee_amount
, CASE WHEN t.royalty_fee_amount/t.amount_original < 0.5 THEN t.royalty_fee_amount*pu.price
    ELSE 0
    END AS royalty_fee_amount_usd
, CASE WHEN t.royalty_fee_amount/t.amount_original < 0.5 THEN CAST(100.0*ROUND(t.royalty_fee_amount/t.amount_original, 2) AS DOUBLE)
    ELSE CAST(0 AS DOUBLE)
    END AS royalty_fee_percentage
, CASE WHEN t.royalty_fee_amount_raw = 0 THEN cast(NULL as string) ELSE ett.to END AS royalty_fee_receive_address
, t.currency_symbol AS royalty_fee_currency_symbol
, t.block_number || t.tx_hash || t.evt_index  AS unique_trade_id
FROM all_foundation_trades t
LEFT JOIN {{ ref('tokens_ethereum_nft') }} nft ON t.nft_contract_address=nft.contract_address
LEFT JOIN {{ source('foundation_ethereum','market_evt_SellerReferralPaid') }} sellref ON sellref.evt_block_time = t.block_time AND sellref.evt_tx_hash = t.tx_hash
LEFT JOIN {{ source('erc721_ethereum','evt_transfer') }} nft_t ON nft_t.evt_block_time=t.block_time
    AND nft_t.evt_tx_hash=t.tx_hash
    AND nft_t.tokenId=t.token_id
    AND nft_t.from=t.seller
    AND nft_t.to=t.buyer
    AND nft_t.contract_address = t.nft_contract_address
    {% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    AND nft_t.evt_block_time >=  date_trunc("day", now() - interval '1 week')
    {% endif %}
LEFT JOIN {{ source('ethereum','transactions') }} et ON et.block_time=t.block_time
    AND et.hash=t.tx_hash
    {% if not is_incremental() %}
    AND et.block_time > '2021-02-01'
    {% endif %}
    {% if is_incremental() %}
    AND et.block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
LEFT JOIN {{ ref('nft_ethereum_aggregators') }} agg ON agg.contract_address=et.to
LEFT JOIN {{ source('prices', 'usd') }} pu ON pu.minute=date_trunc('minute', t.block_time)
    AND pu.blockchain='ethereum'
    AND pu.contract_address=t.currency_contract
    {% if is_incremental() %}
    AND pu.minute >= date_trunc("day", now() - interval '1 week')
    {% endif %}
LEFT JOIN {{ source('ethereum','traces') }} ett ON ett.block_time=t.block_time
    AND ett.tx_hash=t.tx_hash
    AND ett.from = t.project_contract_address
    AND cast(ett.value as string) = cast(t.royalty_fee_amount_raw as string)
    AND call_type = 'call'
    AND ett.to!= t.project_contract_address
    AND ett.to != sellref.sellerReferrer
    AND t.royalty_fee_amount/t.amount_original < 0.5
    and ett.success = true
    {% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    AND ett.block_time >=  date_trunc("day", now() - interval '1 week')
    {% endif %}
{% if is_incremental() %}
-- this filter will only be applied on an incremental run
AND t.block_time >=  date_trunc("day", now() - interval '1 week')
{% endif %}
