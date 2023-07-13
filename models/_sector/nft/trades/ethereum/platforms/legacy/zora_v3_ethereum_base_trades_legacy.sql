{{ config(
    schema = 'zora_v3_ethereum',
    alias = alias('base_trades', legacy_model=True),
    partition_by = ['block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_number','tx_hash','sub_tx_trade_id'],
    )
}}

WITH v3_trades as (
     SELECT
          evt_block_time AS block_time
        , evt_block_number AS block_number
        , get_json_object(a, '$.tokenId') AS nft_token_id
        , 'Offer Accepted' AS trade_category
        , userA AS seller
        , userB AS buyer
        , get_json_object(b, '$.amount') AS price_raw
        , get_json_object(b, '$.tokenContract') AS currency_contract
        , get_json_object(a, '$.tokenContract') AS nft_contract_address
        , contract_address AS project_contract_address
        , evt_tx_hash AS tx_hash
        , evt_index AS sub_tx_trade_id
    FROM (
        SELECT evt_block_time, evt_block_number, evt_tx_hash, evt_index, contract_address, userA, a, userB, b
        FROM {{ source('zora_v3_ethereum','OffersV1_evt_ExchangeExecuted') }}
        UNION ALL SELECT evt_block_time, evt_block_number, evt_tx_hash, evt_index, contract_address, userA, a, userB, b
        FROM {{ source('zora_v3_ethereum','AsksV1_0_evt_ExchangeExecuted') }}
        UNION ALL SELECT evt_block_time, evt_block_number, evt_tx_hash, evt_index, contract_address, userA, a, userB, b
        FROM {{ source('zora_v3_ethereum','AsksV1_1_evt_ExchangeExecuted') }}
    )
    {% if is_incremental() %}
    WHERE evt_block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
    UNION ALL
    SELECT
          evt_block_time AS block_time
        , evt_block_number AS block_number
        , tokenId AS nft_token_id
        , 'Buy' AS trade_category
        , get_json_object(auction, '$.seller') AS seller
        , get_json_object(auction, '$.highestBidder') AS buyer
        , get_json_object(auction, '$.highestBid') AS price_raw
        , coalesce(get_json_object(auction, '$.currency'),'0x0000000000000000000000000000000000000000') AS currency_contract
        , tokenContract AS nft_contract_address
        , contract_address AS project_contract_address
        , evt_tx_hash AS tx_hash
        , evt_index AS sub_tx_trade_id
    FROM (
        SELECT evt_block_time, evt_block_number, evt_tx_hash, evt_index, tokenId, auction, tokenContract, contract_address
        FROM {{ source('zora_v3_ethereum','ReserveAuctionFindersEth_evt_AuctionEnded') }}
        UNION ALL SELECT evt_block_time, evt_block_number, evt_tx_hash, evt_index, tokenId, auction, tokenContract, contract_address
        FROM {{ source('zora_v3_ethereum','ReserveAuctionFindersErc20_evt_AuctionEnded') }}
        UNION ALL SELECT evt_block_time, evt_block_number, evt_tx_hash, evt_index, tokenId, auction, tokenContract, contract_address
        FROM {{ source('zora_v3_ethereum','ReserveAuctionCoreEth_evt_AuctionEnded') }}
        UNION ALL SELECT evt_block_time, evt_block_number, evt_tx_hash, evt_index, tokenId, auction, tokenContract, contract_address
        FROM {{ source('zora_v3_ethereum','ReserveAuctionCoreErc20_evt_AuctionEnded') }}
        UNION ALL SELECT evt_block_time, evt_block_number, evt_tx_hash, evt_index, tokenId, auction, tokenContract, contract_address
        FROM {{ source('zora_v3_ethereum','ReserveAuctionListingEth_evt_AuctionEnded') }}
        UNION ALL SELECT evt_block_time, evt_block_number, evt_tx_hash, evt_index, tokenId, auction, tokenContract, contract_address
        FROM {{ source('zora_v3_ethereum','ReserveAuctionListingErc20_evt_AuctionEnded') }}
    )
    {% if is_incremental() %}
    WHERE evt_block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
    UNION ALL
    SELECT
          evt_block_time AS block_time
        , evt_block_number AS block_number
        , tokenId AS nft_token_id
        , 'Private Sale' AS trade_category
        , get_json_object(ask, '$.seller') AS seller
        , get_json_object(ask, '$.buyer') AS buyer
        , get_json_object(ask, '$.price') AS price_raw
        , '0x0000000000000000000000000000000000000000' AS currency_contract
        , tokenContract AS nft_contract_address
        , contract_address AS project_contract_address
        , evt_tx_hash AS tx_hash
        , evt_index AS sub_tx_trade_id
    FROM {{ source('zora_v3_ethereum','AsksPrivateEth_evt_AskFilled') }}
    {% if is_incremental() %}
    WHERE evt_block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
    UNION ALL
    SELECT
          evt_block_time AS block_time
        , evt_block_number AS block_number
        , tokenId AS nft_token_id
        , 'Buy' AS trade_category
        , seller AS seller
        , buyer AS buyer
        , price AS price_raw
        , '0x0000000000000000000000000000000000000000' AS currency_contract
        , tokenContract AS nft_contract_address
        , contract_address AS project_contract_address
        , evt_tx_hash AS tx_hash
        , evt_index AS sub_tx_trade_id
    FROM {{ source('zora_v3_ethereum','AsksCoreEth_evt_AskFilled') }}
    {% if is_incremental() %}
    WHERE evt_block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
    )

, royalty_payouts as (
    SELECT
     evt_block_time
    ,evt_tx_hash
    ,tokenContract
    ,tokenId
    ,sum(cast(amount as decimal(38))) as royalty_fee_amount_raw
    ,case when count(distinct recipient) = 1
      then min(recipient)
      else cast(null as varchar(1))
     end as royalty_fee_address
    FROM (
    SELECT * FROM (
        SELECT evt_block_time, evt_tx_hash, tokenContract, tokenId, amount, recipient
        FROM {{ source('zora_v3_ethereum','OffersV1_evt_RoyaltyPayout') }}
        UNION ALL SELECT evt_block_time, evt_tx_hash, tokenContract, tokenId, amount, recipient
        FROM {{ source('zora_v3_ethereum','AsksV1_0_evt_RoyaltyPayout') }}
        UNION ALL SELECT evt_block_time, evt_tx_hash, tokenContract, tokenId, amount, recipient
        FROM {{ source('zora_v3_ethereum','AsksV1_1_evt_RoyaltyPayout') }}
        UNION ALL SELECT evt_block_time, evt_tx_hash, tokenContract, tokenId, amount, recipient
        FROM {{ source('zora_v3_ethereum','AsksPrivateEth_evt_RoyaltyPayout') }}
        UNION ALL SELECT evt_block_time, evt_tx_hash, tokenContract, tokenId, amount, recipient
        FROM {{ source('zora_v3_ethereum','AsksCoreEth_evt_RoyaltyPayout') }}
        UNION ALL SELECT evt_block_time, evt_tx_hash, tokenContract, tokenId, amount, recipient
        FROM {{ source('zora_v3_ethereum','ReserveAuctionCoreEth_evt_RoyaltyPayout') }}
        UNION ALL SELECT evt_block_time, evt_tx_hash, tokenContract, tokenId, amount, recipient
        FROM {{ source('zora_v3_ethereum','ReserveAuctionCoreErc20_evt_RoyaltyPayout') }}
        UNION ALL SELECT evt_block_time, evt_tx_hash, tokenContract, tokenId, amount, recipient
        FROM {{ source('zora_v3_ethereum','ReserveAuctionFindersEth_evt_RoyaltyPayout') }}
        UNION ALL SELECT evt_block_time, evt_tx_hash, tokenContract, tokenId, amount, recipient
        FROM {{ source('zora_v3_ethereum','ReserveAuctionFindersErc20_evt_RoyaltyPayout') }}
        UNION ALL SELECT evt_block_time, evt_tx_hash, tokenContract, tokenId, amount, recipient
        FROM {{ source('zora_v3_ethereum','ReserveAuctionListingEth_evt_RoyaltyPayout') }}
        UNION ALL SELECT evt_block_time, evt_tx_hash, tokenContract, tokenId, amount, recipient
        FROM {{ source('zora_v3_ethereum','ReserveAuctionListingErc20_evt_RoyaltyPayout') }}
        )
        WHERE cast(amount as decimal(38)) > 0
        {% if is_incremental() %}
        AND evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
    )
    GROUP BY 1,2,3,4
)

SELECT
    date_trunc('day',block_time) as block_date
    , block_time
    , block_number
    , project_contract_address
    , tx_hash
    , nft_contract_address
    , nft_token_id
    , CAST(1 as INT) as nft_amount
    , trade_category
    , 'secondary' as trade_type
    , buyer
    , seller
    , CAST(price_raw as DECIMAL(38)) as price_raw
    , currency_contract
    , CAST(0 as DECIMAL(38)) AS platform_fee_amount_raw
    , CAST(coalesce(roy.royalty_fee_amount_raw,0) as DECIMAL(38)) as royalty_fee_amount_raw
    , CAST(NULL as VARCHAR(1)) AS platform_fee_address
    , CAST(roy.royalty_fee_address as VARCHAR(42)) as royalty_fee_address
    , sub_tx_trade_id
FROM v3_trades
LEFT JOIN royalty_payouts roy
    ON block_time = roy.evt_block_time
    AND tx_hash = roy.evt_tx_hash
    AND nft_contract_address = roy.tokenContract
    AND nft_token_id = roy.tokenId


