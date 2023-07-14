{{ config(
	tags=['legacy'],
    schema = 'foundation_ethereum',
    alias = alias('base_trades', legacy_model=True),
    partition_by = ['block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_number','tx_hash','sub_tx_trade_id'],
    )
}}
{% set project_start_date='2021-2-04' %}

WITH all_foundation_trades AS (
    SELECT
     f.evt_block_time AS block_time
    , f.evt_block_number AS block_number
    , c.tokenId AS nft_token_id
    , 'Auction Settled' AS trade_category
    , case when (f.sellerRev = 0 and cast(f.creatorRev as decimal(38)) > 0 ) then 'primary' else 'secondary' end as trade_type
    , f.seller
    , f.bidder AS buyer
    , f.creatorRev+f.totalFees+f.sellerRev AS price_raw
    , f.contract_address AS project_contract_address
    , c.nftContract AS nft_contract_address
    , f.evt_tx_hash AS tx_hash
    , f.totalFees AS platform_fee_amount_raw
    , case when (f.sellerRev = 0 and cast(f.creatorRev as decimal(38)) > 0 ) then 0 else f.creatorRev end AS royalty_fee_amount_raw
    , f.evt_index as sub_tx_trade_id
    FROM {{ source('foundation_ethereum','market_evt_ReserveAuctionFinalized') }} f
    INNER JOIN {{ source('foundation_ethereum','market_evt_ReserveAuctionCreated') }} c ON c.auctionId=f.auctionId AND c.evt_block_time<=f.evt_block_time
     {% if is_incremental() %} -- this filter will only be applied on an incremental run
     WHERE f.evt_block_time >= date_trunc("day", now() - interval '1 week')
     {% else %}
     WHERE f.evt_block_time >= '{{project_start_date}}'
     {% endif %}
    UNION ALL
    SELECT
     evt_block_time AS block_time
    , evt_block_number AS block_number
    , tokenId AS nft_token_id
    , 'Buy' AS trade_category
    , case when (sellerRev = 0 and cast(creatorRev as decimal(38)) > 0 ) then 'primary' else 'secondary' end as trade_type
    , seller
    , buyer
    , creatorRev+totalFees+sellerRev AS price_raw
    , contract_address AS project_contract_address
    , nftContract AS nft_contract_address
    , evt_tx_hash AS tx_hash
    , totalFees AS platform_fee_amount_raw
    , case when (sellerRev = 0 and cast(creatorRev as decimal(38)) > 0 ) then 0 else creatorRev end AS royalty_fee_amount_raw
    , evt_index as sub_tx_trade_id
    FROM {{ source('foundation_ethereum','market_evt_BuyPriceAccepted') }} f
     {% if is_incremental() %} -- this filter will only be applied on an incremental run
     WHERE f.evt_block_time >= date_trunc("day", now() - interval '1 week')
     {% else %}
     WHERE f.evt_block_time >= '{{project_start_date}}'
     {% endif %}
    UNION ALL
    SELECT
     evt_block_time AS block_time
    , evt_block_number AS block_number
    , tokenId AS nft_token_id
    , 'Sell' AS trade_category
    , case when (sellerRev = 0 and cast(creatorRev as decimal(38)) > 0 ) then 'primary' else 'secondary' end as trade_type
    , seller
    , buyer
    , creatorRev+totalFees+sellerRev AS price_raw
    , contract_address AS project_contract_address
    , nftContract AS nft_contract_address
    , evt_tx_hash AS tx_hash
    , totalFees AS platform_fee_amount_raw
    , case when (sellerRev = 0 and cast(creatorRev as decimal(38)) > 0 ) then 0 else creatorRev end AS royalty_fee_amount_raw
    , evt_index as sub_tx_trade_id
    FROM {{ source('foundation_ethereum','market_evt_OfferAccepted') }} f
     {% if is_incremental() %} -- this filter will only be applied on an incremental run
     WHERE f.evt_block_time >= date_trunc("day", now() - interval '1 week')
     {% else %}
     WHERE f.evt_block_time >= '{{project_start_date}}'
     {% endif %}
    UNION ALL
    SELECT
     evt_block_time AS block_time
    , evt_block_number AS block_number
    , tokenId AS nft_token_id
    , 'Private Sale' AS trade_category
    , case when (sellerRev = 0 and cast(creatorFee as decimal(38)) > 0 ) then 'primary' else 'secondary' end as trade_type
    , seller
    , buyer
    , creatorFee+protocolFee+sellerRev AS price_raw
    , contract_address AS project_contract_address
    , nftContract AS nft_contract_address
    , evt_tx_hash AS tx_hash
    , protocolFee AS platform_fee_amount_raw
    , case when (sellerRev = 0 and cast(creatorFee as decimal(38)) > 0 ) then 0 else creatorFee end AS royalty_fee_amount_raw
    , evt_index as sub_tx_trade_id
    FROM {{ source('foundation_ethereum','market_evt_PrivateSaleFinalized') }} f
     {% if is_incremental() %} -- this filter will only be applied on an incremental run
     WHERE f.evt_block_time >= date_trunc("day", now() - interval '1 week')
     {% else %}
     WHERE f.evt_block_time >= '{{project_start_date}}'
     {% endif %}
    )

SELECT
 date_trunc('day', t.block_time) AS block_date
, t.block_time
, t.block_number
, t.nft_token_id
, 1 AS nft_amount
, t.trade_category
, t.trade_type
, t.seller
, t.buyer
, cast(t.price_raw as decimal(38)) as price_raw
, '{{ var("ETH_ERC20_ADDRESS") }}' AS currency_contract -- all trades are in ETH
, t.project_contract_address
, t.nft_contract_address
, t.tx_hash
, cast(t.platform_fee_amount_raw as decimal(38)) as platform_fee_amount_raw
, cast(t.royalty_fee_amount_raw as decimal(38)) as royalty_fee_amount_raw
, cast(NULL as string) AS royalty_fee_address
, cast(NULL as string) AS platform_fee_address
, t.sub_tx_trade_id
FROM all_foundation_trades t
