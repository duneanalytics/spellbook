{{ config(
    alias = 'events',
    partition_by = ['block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'unique_trade_id'],
    post_hook='{{ expose_spells(\'["ethereum"]\',
                                "project",
                                "zora",
                                \'["hildobby"]\') }}'
    )
}}

WITH zora_trades AS (
    SELECT 'v3' AS version
    , z3_o1_ee.evt_block_time AS block_time
    , z3_o1_ee.evt_block_number AS block_number
    , get_json_object(z3_o1_ee.a, '$.tokenId') AS token_id
    , 'Offer Accepted' AS trade_category
    , z3_o1_ee.userA AS seller
    , z3_o1_ee.userB AS buyer
    , get_json_object(z3_o1_ee.b, '$.amount') AS amount_raw
    , get_json_object(z3_o1_ee.b, '$.tokenContract') AS currency_contract
    , get_json_object(z3_o1_ee.a, '$.tokenContract') AS nft_contract_address
    , z3_o1_ee.contract_address AS project_contract_address
    , z3_o1_ee.evt_tx_hash AS tx_hash
    , z3_o1_rp.amount AS royalty_fee_amount_raw
    , z3_o1_rp.recipient AS royalty_fee_receive_address
    FROM {{ source('zora_v3_ethereum','OffersV1_evt_ExchangeExecuted') }} z3_o1_ee
    LEFT JOIN {{ source('zora_v3_ethereum','OffersV1_evt_RoyaltyPayout') }} z3_o1_rp ON z3_o1_ee.evt_block_time=z3_o1_rp.evt_block_time
        AND z3_o1_ee.evt_tx_hash=z3_o1_rp.evt_tx_hash
        AND get_json_object(z3_o1_ee.a, '$.tokenContract')=z3_o1_rp.tokenContract
        AND get_json_object(z3_o1_ee.a, '$.tokenId')=z3_o1_rp.tokenId
        {% if is_incremental() %}
        AND z3_o1_rp.evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
    UNION ALL
    SELECT 'v3' AS version
    , z3_a0_ee.evt_block_time AS block_time
    , z3_a0_ee.evt_block_number AS block_number
    , get_json_object(z3_a0_ee.a, '$.tokenId') AS token_id
    , 'Buy' AS trade_category
    , z3_a0_ee.userA AS seller
    , z3_a0_ee.userB AS buyer
    , get_json_object(z3_a0_ee.b, '$.amount') AS amount_raw
    , get_json_object(z3_a0_ee.b, '$.tokenContract') AS currency_contract
    , get_json_object(z3_a0_ee.a, '$.tokenContract') AS nft_contract_address
    , z3_a0_ee.contract_address AS project_contract_address
    , z3_a0_ee.evt_tx_hash AS tx_hash
    , z3_a0_rp.amount AS royalty_fee_amount_raw
    , z3_a0_rp.recipient AS royalty_fee_receive_address
    FROM {{ source('zora_v3_ethereum','AsksV1_0_evt_ExchangeExecuted') }} z3_a0_ee
    LEFT JOIN {{ source('zora_v3_ethereum','AsksV1_0_evt_RoyaltyPayout') }} z3_a0_rp ON z3_a0_ee.evt_block_time=z3_a0_rp.evt_block_time
        AND z3_a0_ee.evt_tx_hash=z3_a0_rp.evt_tx_hash
        AND get_json_object(z3_a0_ee.a, '$.tokenContract')=z3_a0_rp.tokenContract
        AND get_json_object(z3_a0_ee.a, '$.tokenId')=z3_a0_rp.tokenId
        {% if is_incremental() %}
        AND z3_a0_rp.evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
    UNION ALL
    SELECT 'v3' AS version
    , z3_a1_ee.evt_block_time AS block_time
    , z3_a1_ee.evt_block_number AS block_number
    , get_json_object(z3_a1_ee.a, '$.tokenId') AS token_id
    , 'Buy' AS trade_category
    , z3_a1_ee.userA AS seller
    , z3_a1_ee.userB AS buyer
    , get_json_object(z3_a1_ee.b, '$.amount') AS amount_raw
    , get_json_object(z3_a1_ee.b, '$.tokenContract') AS currency_contract
    , get_json_object(z3_a1_ee.a, '$.tokenContract') AS nft_contract_address
    , z3_a1_ee.contract_address AS project_contract_address
    , z3_a1_ee.evt_tx_hash AS tx_hash
    , z3_a1_rp.amount AS royalty_fee_amount_raw
    , z3_a1_rp.recipient AS royalty_fee_receive_address
    FROM {{ source('zora_v3_ethereum','AsksV1_1_evt_ExchangeExecuted') }} z3_a1_ee
    LEFT JOIN {{ source('zora_v3_ethereum','AsksV1_1_evt_RoyaltyPayout') }} z3_a1_rp ON z3_a1_ee.evt_block_time=z3_a1_rp.evt_block_time
        AND z3_a1_ee.evt_tx_hash=z3_a1_rp.evt_tx_hash
        AND get_json_object(z3_a1_ee.a, '$.tokenContract')=z3_a1_rp.tokenContract
        AND get_json_object(z3_a1_ee.a, '$.tokenId')=z3_a1_rp.tokenId
        {% if is_incremental() %}
        AND z3_a1_rp.evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
    UNION ALL
    SELECT 'v3' AS version
    , z3_rafe_ae.evt_block_time AS block_time
    , z3_rafe_ae.evt_block_number AS block_number
    , z3_rafe_ae.tokenId AS token_id
    , 'Auction Settled' AS trade_category
    , get_json_object(z3_rafe_ae.auction, '$.seller') AS seller
    , get_json_object(z3_rafe_ae.auction, '$.highestBidder') AS buyer
    , get_json_object(z3_rafe_ae.auction, '$.highestBid') AS amount_raw
    , '0x0000000000000000000000000000000000000000' AS currency_contract
    , z3_rafe_ae.tokenContract AS nft_contract_address
    , z3_rafe_ae.contract_address AS project_contract_address
    , z3_rafe_ae.evt_tx_hash AS tx_hash
    , z3_rafe_rp.amount AS royalty_fee_amount_raw
    , z3_rafe_rp.recipient AS royalty_fee_receive_address
    FROM {{ source('zora_v3_ethereum','ReserveAuctionFindersEth_evt_AuctionEnded') }} z3_rafe_ae
    LEFT JOIN {{ source('zora_v3_ethereum','ReserveAuctionFindersEth_evt_RoyaltyPayout') }} z3_rafe_rp ON z3_rafe_ae.evt_block_time=z3_rafe_rp.evt_block_time
        AND z3_rafe_ae.evt_tx_hash=z3_rafe_rp.evt_tx_hash
        AND z3_rafe_ae.tokenContract=z3_rafe_rp.tokenContract
        AND z3_rafe_ae.tokenId=z3_rafe_rp.tokenId
        {% if is_incremental() %}
        AND z3_rafe_rp.evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
    UNION ALL
    SELECT 'v3' AS version
    , z3_ape_af.evt_block_time AS block_time
    , z3_ape_af.evt_block_number AS block_number
    , z3_ape_af.tokenId AS token_id
    , 'Private Sale' AS trade_category
    , get_json_object(z3_ape_af.ask, '$.seller') AS seller
    , get_json_object(z3_ape_af.ask, '$.buyer') AS buyer
    , get_json_object(z3_ape_af.ask, '$.price') AS amount_raw
    , '0x0000000000000000000000000000000000000000' AS currency_contract
    , z3_ape_af.tokenContract AS nft_contract_address
    , z3_ape_af.contract_address AS project_contract_address
    , z3_ape_af.evt_tx_hash AS tx_hash
    , z3_ape_rp.amount AS royalty_fee_amount_raw
    , z3_ape_rp.recipient AS royalty_fee_receive_address
    FROM {{ source('zora_v3_ethereum','AsksPrivateEth_evt_AskFilled') }} z3_ape_af
    LEFT JOIN {{ source('zora_v3_ethereum','AsksPrivateEth_evt_RoyaltyPayout') }} z3_ape_rp ON z3_ape_af.evt_block_time=z3_ape_rp.evt_block_time
        AND z3_ape_af.evt_tx_hash=z3_ape_rp.evt_tx_hash
        AND z3_ape_af.tokenContract=z3_ape_rp.tokenContract
        AND z3_ape_af.tokenId=z3_ape_rp.tokenId
        {% if is_incremental() %}
        AND z3_ape_rp.evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
    UNION ALL
    SELECT 'v3' AS version
    , z3_ace_af.evt_block_time AS block_time
    , z3_ace_af.evt_block_number AS block_number
    , z3_ace_af.tokenId AS token_id
    , 'Buy' AS trade_category
    , z3_ace_af.seller AS seller
    , z3_ace_af.buyer AS buyer
    , z3_ace_af.price AS amount_raw
    , '0x0000000000000000000000000000000000000000' AS currency_contract
    , z3_ace_af.tokenContract AS nft_contract_address
    , z3_ace_af.contract_address AS project_contract_address
    , z3_ace_af.evt_tx_hash AS tx_hash
    , z3_ace_rp.amount AS royalty_fee_amount_raw
    , z3_ace_rp.recipient AS royalty_fee_receive_address
    FROM {{ source('zora_v3_ethereum','AsksCoreEth_evt_AskFilled') }} z3_ace_af
    LEFT JOIN {{ source('zora_v3_ethereum','AsksCoreEth_evt_RoyaltyPayout') }} z3_ace_rp ON z3_ace_af.evt_block_time=z3_ace_rp.evt_block_time
        AND z3_ace_af.evt_tx_hash=z3_ace_rp.evt_tx_hash
        AND z3_ace_af.tokenContract=z3_ace_rp.tokenContract
        AND z3_ace_af.tokenId=z3_ace_rp.tokenId
        {% if is_incremental() %}
        AND z3_ace_rp.evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
    UNION ALL
    SELECT 'v3' AS version
    , z3_race_ae.evt_block_time AS block_time
    , z3_race_ae.evt_block_number AS block_number
    , z3_race_ae.tokenId AS token_id
    , 'Buy' AS trade_category
    , get_json_object(z3_race_ae.auction, '$.seller') AS seller
    , get_json_object(z3_race_ae.auction, '$.highestBidder') AS buyer
    , get_json_object(z3_race_ae.auction, '$.highestBid') AS amount_raw
    , '0x0000000000000000000000000000000000000000' AS currency_contract
    , z3_race_ae.tokenContract AS nft_contract_address
    , z3_race_ae.contract_address AS project_contract_address
    , z3_race_ae.evt_tx_hash AS tx_hash
    , z3_race_rp.amount AS royalty_fee_amount_raw
    , z3_race_rp.recipient AS royalty_fee_receive_address
    FROM {{ source('zora_v3_ethereum','ReserveAuctionCoreEth_evt_AuctionEnded') }} z3_race_ae
    LEFT JOIN {{ source('zora_v3_ethereum','ReserveAuctionCoreEth_evt_RoyaltyPayout') }} z3_race_rp ON z3_race_ae.evt_block_time=z3_race_rp.evt_block_time
        AND z3_race_ae.evt_tx_hash=z3_race_rp.evt_tx_hash
        AND z3_race_ae.tokenContract=z3_race_rp.tokenContract
        AND z3_race_ae.tokenId=z3_race_rp.tokenId
        {% if is_incremental() %}
        AND z3_race_rp.evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
    UNION ALL
    SELECT 'v3' AS version
    , z3_racerc_ae.evt_block_time AS block_time
    , z3_racerc_ae.evt_block_number AS block_number
    , z3_racerc_ae.tokenId AS token_id
    , 'Buy' AS trade_category
    , get_json_object(z3_racerc_ae.auction, '$.seller') AS seller
    , get_json_object(z3_racerc_ae.auction, '$.highestBidder') AS buyer
    , get_json_object(z3_racerc_ae.auction, '$.highestBid') AS amount_raw
    , get_json_object(z3_racerc_ae.auction, '$.currency') AS currency_contract
    , z3_racerc_ae.tokenContract AS nft_contract_address
    , z3_racerc_ae.contract_address AS project_contract_address
    , z3_racerc_ae.evt_tx_hash AS tx_hash
    , z3_racerc_rp.amount AS royalty_fee_amount_raw
    , z3_racerc_rp.recipient AS royalty_fee_receive_address
    FROM {{ source('zora_v3_ethereum','ReserveAuctionCoreErc20_evt_AuctionEnded') }} z3_racerc_ae
    LEFT JOIN {{ source('zora_v3_ethereum','ReserveAuctionCoreErc20_evt_RoyaltyPayout') }} z3_racerc_rp ON z3_racerc_ae.evt_block_time=z3_racerc_rp.evt_block_time
        AND z3_racerc_ae.evt_tx_hash=z3_racerc_rp.evt_tx_hash
        AND z3_racerc_ae.tokenContract=z3_racerc_rp.tokenContract
        AND z3_racerc_ae.tokenId=z3_racerc_rp.tokenId
        {% if is_incremental() %}
        AND z3_racerc_rp.evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
    UNION ALL
    SELECT 'v3' AS version
    , z3_raferc_ae.evt_block_time AS block_time
    , z3_raferc_ae.evt_block_number AS block_number
    , z3_raferc_ae.tokenId AS token_id
    , 'Buy' AS trade_category
    , get_json_object(z3_raferc_ae.auction, '$.seller') AS seller
    , get_json_object(z3_raferc_ae.auction, '$.highestBidder') AS buyer
    , get_json_object(z3_raferc_ae.auction, '$.highestBid') AS amount_raw
    , get_json_object(z3_raferc_ae.auction, '$.currency') AS currency_contract
    , z3_raferc_ae.tokenContract AS nft_contract_address
    , z3_raferc_ae.contract_address AS project_contract_address
    , z3_raferc_ae.evt_tx_hash AS tx_hash
    , z3_raferc_rp.amount AS royalty_fee_amount_raw
    , z3_raferc_rp.recipient AS royalty_fee_receive_address
    FROM {{ source('zora_v3_ethereum','ReserveAuctionFindersErc20_evt_AuctionEnded') }} z3_raferc_ae
    LEFT JOIN {{ source('zora_v3_ethereum','ReserveAuctionFindersErc20_evt_RoyaltyPayout') }} z3_raferc_rp ON z3_raferc_ae.evt_block_time=z3_raferc_rp.evt_block_time
        AND z3_raferc_ae.evt_tx_hash=z3_raferc_rp.evt_tx_hash
        AND z3_raferc_ae.tokenContract=z3_raferc_rp.tokenContract
        AND z3_raferc_ae.tokenId=z3_raferc_rp.tokenId
        {% if is_incremental() %}
        AND z3_raferc_rp.evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
    UNION ALL
    SELECT 'v3' AS version
    , z3_rale_ae.evt_block_time AS block_time
    , z3_rale_ae.evt_block_number AS block_number
    , z3_rale_ae.tokenId AS token_id
    , 'Buy' AS trade_category
    , get_json_object(z3_rale_ae.auction, '$.seller') AS seller
    , get_json_object(z3_rale_ae.auction, '$.highestBidder') AS buyer
    , get_json_object(z3_rale_ae.auction, '$.highestBid') AS amount_raw
    , '0x0000000000000000000000000000000000000000' AS currency_contract
    , z3_rale_ae.tokenContract AS nft_contract_address
    , z3_rale_ae.contract_address AS project_contract_address
    , z3_rale_ae.evt_tx_hash AS tx_hash
    , z3_rale_rp.amount AS royalty_fee_amount_raw
    , z3_rale_rp.recipient AS royalty_fee_receive_address
    FROM {{ source('zora_v3_ethereum','ReserveAuctionListingEth_evt_AuctionEnded') }} z3_rale_ae
    LEFT JOIN {{ source('zora_v3_ethereum','ReserveAuctionListingEth_evt_RoyaltyPayout') }} z3_rale_rp ON z3_rale_ae.evt_block_time=z3_rale_rp.evt_block_time
        AND z3_rale_ae.evt_tx_hash=z3_rale_rp.evt_tx_hash
        AND z3_rale_ae.tokenContract=z3_rale_rp.tokenContract
        AND z3_rale_ae.tokenId=z3_rale_rp.tokenId
        {% if is_incremental() %}
        AND z3_rale_rp.evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
    UNION ALL
    SELECT 'v3' AS version
    , z3_rale_ae.evt_block_time AS block_time
    , z3_rale_ae.evt_block_number AS block_number
    , z3_rale_ae.tokenId AS token_id
    , 'Buy' AS trade_category
    , get_json_object(z3_rale_ae.auction, '$.seller') AS seller
    , get_json_object(z3_rale_ae.auction, '$.highestBidder') AS buyer
    , get_json_object(z3_rale_ae.auction, '$.highestBid') AS amount_raw
    , get_json_object(z3_rale_ae.auction, '$.currency') AS currency_contract
    , z3_rale_ae.tokenContract AS nft_contract_address
    , z3_rale_ae.contract_address AS project_contract_address
    , z3_rale_ae.evt_tx_hash AS tx_hash
    , z3_rale_rp.amount AS royalty_fee_amount_raw
    , z3_rale_rp.recipient AS royalty_fee_receive_address
    FROM {{ source('zora_v3_ethereum','ReserveAuctionListingErc20_evt_AuctionEnded') }} z3_rale_ae
    LEFT JOIN {{ source('zora_v3_ethereum','ReserveAuctionListingErc20_evt_RoyaltyPayout') }} z3_rale_rp ON z3_rale_ae.evt_block_time=z3_rale_rp.evt_block_time
        AND z3_rale_ae.evt_tx_hash=z3_rale_rp.evt_tx_hash
        AND z3_rale_ae.tokenContract=z3_rale_rp.tokenContract
        AND z3_rale_ae.tokenId=z3_rale_rp.tokenId
        {% if is_incremental() %}
        AND z3_rale_rp.evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
    UNION ALL
    SELECT 'v2' AS version
    , z2_ae.evt_block_time AS block_time
    , z2_ae.evt_block_number AS block_number
    , z2_ae.tokenId AS token_id
    , 'Auction Settled' AS trade_category
    , z2_ae.tokenOwner AS seller
    , z2_ae.winner AS buyer
    , z2_ae.amount+z2_ae.curatorFee AS amount_raw
    , z2_ae.auctionCurrency AS currency_contract
    , z2_ae.tokenContract AS nft_contract_address
    , z2_ae.contract_address AS project_contract_address
    , z2_ae.evt_tx_hash AS tx_hash
    , 0 AS royalty_fee_amount_raw
    , NULL AS royalty_fee_receive_address
    FROM {{ source('zora_ethereum','AuctionHouse_evt_AuctionEnded') }} z2_ae
    {% if is_incremental() %}
    WHERE z2_ae.evt_block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
    UNION ALL
    SELECT 'v1' AS version
    , z1_bf.evt_block_time AS block_time
    , z1_bf.evt_block_number AS block_number
    , z1_bf.tokenId AS token_id
    , 'Buy' AS trade_category
    , z1_mt.from AS seller
    , get_json_object(z1_bf.bid, '$.recipient') AS buyer
    , get_json_object(z1_bf.bid, '$.amount') AS amount_raw
    , get_json_object(z1_bf.bid, '$.currency') AS currency_contract
    , '0xabefbc9fd2f806065b4f3c237d4b59d9a97bcac7' AS nft_contract_address
    , z1_bf.contract_address AS project_contract_address
    , z1_bf.evt_tx_hash AS tx_hash
    , 0 AS royalty_fee_amount_raw
    , NULL AS royalty_fee_receive_address
    FROM {{ source('zora_ethereum','Market_evt_BidFinalized') }} z1_bf
    LEFT JOIN {{ source('zora_ethereum','Media_evt_Transfer') }} z1_mt ON z1_bf.evt_block_time = z1_mt.evt_block_time
        AND z1_bf.evt_tx_hash = z1_mt.evt_tx_hash
        AND z1_bf.tokenId = z1_mt.tokenId
    {% if is_incremental() %}
    AND z1_mt.evt_block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
    WHERE get_json_object(z1_bf.bid, '$.bidder') != '0xe468ce99444174bd3bbbed09209577d25d1ad673'
    )

SELECT 'ethereum' AS blockchain
    , 'zora' AS project
    , zt.version
    , zt.block_time
    , date_trunc('day', zt.block_time) AS block_date
    , zt.block_number
    , zt.token_id
    , nft.name AS collection
    , CASE WHEN zt.currency_contract='0x0000000000000000000000000000000000000000' THEN zt.amount_raw/POWER(10, 18)*pu.price
        ELSE zt.amount_raw/POWER(10, pu.decimals)*pu.price END AS amount_usd
    , CASE WHEN erc721.evt_index IS NOT NULL THEN 'erc721' ELSE 'erc1155' END AS token_standard
    , CASE WHEN agg.name IS NOT NULL THEN 'Bundle Trade' ELSE 'Single Item Trade' END AS trade_type
    , CAST(1 AS DECIMAL(38,0)) AS number_of_items
    , zt.trade_category
    , 'Trade' AS evt_type
    , zt.seller
    , zt.buyer
    , CASE WHEN zt.currency_contract='0x0000000000000000000000000000000000000000' THEN zt.amount_raw/POWER(10, 18)
        ELSE zt.amount_raw/POWER(10, pu.decimals) END AS amount_original
    , CAST(zt.amount_raw AS DECIMAL(38,0)) AS amount_raw
    , CASE WHEN zt.currency_contract='0x0000000000000000000000000000000000000000' THEN 'ETH'
        ELSE pu.symbol END AS currency_symbol
    , CASE WHEN zt.currency_contract='0x0000000000000000000000000000000000000000' THEN '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
        ELSE zt.currency_contract END AS currency_contract
    , zt.nft_contract_address
    , zt.project_contract_address
    , agg.name AS aggregator_name
    , agg.contract_address AS aggregator_address
    , zt.tx_hash
    , et.from AS tx_from
    , et.to AS tx_to
    , CAST(0 AS DOUBLE) AS platform_fee_amount_raw
    , CAST(0 AS DOUBLE) AS platform_fee_amount
    , CAST(0 AS DOUBLE) AS platform_fee_amount_usd
    , CAST(0 AS DOUBLE) AS platform_fee_percentage
    , CAST(SUM(zt.royalty_fee_amount_raw) AS DOUBLE) AS royalty_fee_amount_raw
    , CASE WHEN zt.currency_contract='0x0000000000000000000000000000000000000000' THEN COALESCE(SUM(zt.royalty_fee_amount_raw)/POWER(10, 18), 0)
        ELSE COALESCE(SUM(zt.royalty_fee_amount_raw)/POWER(10, pu.decimals), 0) END AS royalty_fee_amount
    , CASE WHEN zt.currency_contract='0x0000000000000000000000000000000000000000' THEN COALESCE(SUM(zt.royalty_fee_amount_raw)/POWER(10, 18)*pu.price, 0)
        ELSE COALESCE(SUM(zt.royalty_fee_amount_raw)/POWER(10, pu.decimals)*pu.price, 0) END AS royalty_fee_amount_usd
    , CAST(COALESCE(100.0*SUM(zt.royalty_fee_amount_raw)/zt.amount_raw, 0) AS DOUBLE) AS royalty_fee_percentage
    , FIRST(zt.royalty_fee_receive_address) AS royalty_fee_receive_address
    , CASE WHEN zt.currency_contract='0x0000000000000000000000000000000000000000' THEN 'ETH'
        ELSE pu.symbol END AS royalty_fee_currency_symbol
    , 'ethereumzora' || COALESCE(version, '-1') || COALESCE(zt.tx_hash, '-1') || COALESCE(zt.nft_contract_address, '-1') || COALESCE(zt.token_id, '-1') || COALESCE(zt.buyer, '-1') || COALESCE(zt.seller, '-1') AS unique_trade_id
FROM zora_trades zt
LEFT JOIN {{ source('ethereum','transactions') }} et ON et.block_time=zt.block_time
    AND et.hash=zt.tx_hash
    {% if is_incremental() %}
    AND et.block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
LEFT JOIN {{ ref('nft_aggregators') }} agg ON agg.blockchain='ethereum'
    AND agg.contract_address=et.to
LEFT JOIN {{ source('erc721_ethereum','evt_transfer') }} erc721 ON erc721.evt_block_time=zt.block_time
    AND erc721.evt_tx_hash=zt.tx_hash
    AND erc721.contract_address=zt.nft_contract_address
    AND erc721.tokenId=zt.token_id
    AND erc721.to=zt.buyer
    {% if is_incremental() %}
    AND erc721.evt_block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
LEFT JOIN {{ source('prices','usd') }} pu ON pu.blockchain='ethereum'
    AND pu.minute=date_trunc('minute', zt.block_time)
    AND (pu.contract_address=zt.currency_contract
    OR (pu.contract_address='0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
    AND zt.currency_contract='0x0000000000000000000000000000000000000000'))
    {% if is_incremental() %}
    AND pu.minute >= date_trunc("day", now() - interval '1 week')
    {% endif %}
LEFT JOIN {{ ref('tokens_nft') }}  nft ON nft.blockchain='ethereum'
    AND nft.contract_address=zt.nft_contract_address
GROUP BY zt.version, zt.block_time, zt.block_number, zt.token_id, nft.name, pu.price, erc721.evt_index, agg.name, zt.trade_category, zt.seller, zt.buyer, zt.amount_raw, pu.decimals, pu.symbol, zt.currency_contract, zt.nft_contract_address, zt.project_contract_address, agg.name, agg.contract_address, zt.tx_hash, et.from, et.to
