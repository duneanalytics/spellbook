{{ config(
    schema = 'zora_v3_ethereum',
    
    alias = 'base_trades',
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
        , CAST(JSON_EXTRACT_SCALAR(a, '$.tokenId') as uint256) AS nft_token_id
        , 'Offer Accepted' AS trade_category
        , userA AS seller
        , userB AS buyer
        , CAST(JSON_EXTRACT_SCALAR(b, '$.amount') as uint256) AS price_raw
        , from_hex(JSON_EXTRACT_SCALAR(b, '$.tokenContract')) AS currency_contract
        , from_hex(JSON_EXTRACT_SCALAR(a, '$.tokenContract')) AS nft_contract_address
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
    WHERE {{incremental_predicate('evt_block_time')}}
    {% endif %}
    UNION ALL
    SELECT
          evt_block_time AS block_time
        , evt_block_number AS block_number
        , tokenId AS nft_token_id
        , 'Buy' AS trade_category
        , from_hex(JSON_EXTRACT_SCALAR(auction, '$.seller')) AS seller
        , from_hex(JSON_EXTRACT_SCALAR(auction, '$.highestBidder')) AS buyer
        , CAST(JSON_EXTRACT_SCALAR(auction, '$.highestBid') as uint256) AS price_raw
        , coalesce(from_hex(JSON_EXTRACT_SCALAR(auction, '$.currency')),0x0000000000000000000000000000000000000000) AS currency_contract
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
    WHERE {{incremental_predicate('evt_block_time')}}
    {% endif %}
    UNION ALL
    SELECT
          evt_block_time AS block_time
        , evt_block_number AS block_number
        , tokenId AS nft_token_id
        , 'Private Sale' AS trade_category
        , from_hex(JSON_EXTRACT_SCALAR(ask, '$.seller')) AS seller
        , from_hex(JSON_EXTRACT_SCALAR(ask, '$.buyer')) AS buyer
        , CAST(JSON_EXTRACT_SCALAR(ask, '$.price') as uint256) AS price_raw
        , 0x0000000000000000000000000000000000000000 AS currency_contract
        , tokenContract AS nft_contract_address
        , contract_address AS project_contract_address
        , evt_tx_hash AS tx_hash
        , evt_index AS sub_tx_trade_id
    FROM {{ source('zora_v3_ethereum','AsksPrivateEth_evt_AskFilled') }}
    {% if is_incremental() %}
    WHERE {{incremental_predicate('evt_block_time')}}
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
        , 0x0000000000000000000000000000000000000000 AS currency_contract
        , tokenContract AS nft_contract_address
        , contract_address AS project_contract_address
        , evt_tx_hash AS tx_hash
        , evt_index AS sub_tx_trade_id
    FROM {{ source('zora_v3_ethereum','AsksCoreEth_evt_AskFilled') }}
    {% if is_incremental() %}
    WHERE {{incremental_predicate('evt_block_time')}}
    {% endif %}
    )

, royalty_payouts as (
    SELECT
     evt_block_time
    ,evt_tx_hash
    ,tokenContract
    ,tokenId
    ,sum(cast(amount as uint256)) as royalty_fee_amount_raw
    ,case when count(distinct recipient) = 1
      then min(recipient)
      else cast(null as varbinary)
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
        WHERE cast(amount as uint256) > uint256 '0'
        {% if is_incremental() %}
        AND {{incremental_predicate('evt_block_time')}}
        {% endif %}
    )
    GROUP BY 1,2,3,4
)

SELECT
      'ethereum' as blockchain
    , 'zora' as project
    , 'v3' as project_version
    , block_time
    , block_number
    , project_contract_address
    , tx_hash
    , nft_contract_address
    , nft_token_id
    , uint256 '1' as nft_amount
    , trade_category
    , 'secondary' as trade_type
    , buyer
    , seller
    , price_raw as price_raw
    , currency_contract
    , uint256 '0' AS platform_fee_amount_raw
    , coalesce(roy.royalty_fee_amount_raw,uint256 '0') as royalty_fee_amount_raw
    , CAST(NULL as varbinary) AS platform_fee_address
    , roy.royalty_fee_address as royalty_fee_address
    , sub_tx_trade_id
FROM v3_trades
LEFT JOIN royalty_payouts roy
    ON block_time = roy.evt_block_time
    AND tx_hash = roy.evt_tx_hash
    AND nft_contract_address = roy.tokenContract
    AND nft_token_id = roy.tokenId


