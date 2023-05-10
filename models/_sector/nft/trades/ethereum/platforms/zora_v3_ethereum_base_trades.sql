{{ config(
    schema = 'zora_v1_ethereum',
    alias ='base_trades',
    partition_by = ['block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_number','tx_hash','sub_tx_trade_id'],
    )
}}


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
