{{ config(
    schema = 'foundation_ethereum',
    alias = 'base_trades',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_number','tx_hash','sub_tx_trade_id'],
    )
}}
{% set project_start_date='2021-2-04' %}

WITH
  auctions AS (
    SELECT
      f.evt_block_time AS block_time,
      f.evt_block_number AS block_number,
      c.tokenId AS nft_token_id,
      1 as nft_amount,
      'Auction Settled' AS trade_category,
      case
        when (
          f.sellerRev = UINT256 '0'
          and f.creatorRev > UINT256 '0'
        ) then 'primary'
        else 'secondary'
      end as trade_type,
      f.seller,
      f.bidder AS buyer,
      f.creatorRev + f.totalFees + f.sellerRev AS price_raw,
      f.contract_address AS project_contract_address,
      c.nftContract AS nft_contract_address,
      f.evt_tx_hash AS tx_hash,
      f.totalFees AS platform_fee_amount_raw,
      case
        when (
          f.sellerRev = UINT256 '0'
          and f.creatorRev > UINT256 '0'
        ) then UINT256 '0'
        else f.creatorRev
      end AS royalty_fee_amount_raw,
      f.evt_index as sub_tx_trade_id
    FROM {{ source('foundation_ethereum','market_evt_ReserveAuctionFinalized') }} f
      INNER JOIN {{ source('foundation_ethereum','market_evt_ReserveAuctionCreated') }} c ON c.auctionId=f.auctionId AND c.evt_block_time<=f.evt_block_time
       {% if is_incremental() %} -- this filter will only be applied on an incremental run
       WHERE {{incremental_predicate('f.evt_block_time')}}
       {% else %}
        WHERE
        f.evt_block_time >= TIMESTAMP '{{project_start_date}}'
       {% endif %}
  ),
  buys AS (
    SELECT
      evt_block_time AS block_time,
      evt_block_number AS block_number,
      tokenId AS nft_token_id,
      1 as nft_amount,
      'Buy' AS trade_category,
      case
        when (
          sellerRev = UINT256 '0'
          and creatorRev > UINT256 '0'
        ) then 'primary'
        else 'secondary'
      end as trade_type,
      seller,
      buyer,
      creatorRev + totalFees + sellerRev AS price_raw,
      contract_address AS project_contract_address,
      nftContract AS nft_contract_address,
      evt_tx_hash AS tx_hash,
      totalFees AS platform_fee_amount_raw,
      case
        when (
          sellerRev = UINT256 '0'
          and creatorRev > UINT256 '0'
        ) then UINT256 '0'
        else creatorRev
      end AS royalty_fee_amount_raw,
      evt_index as sub_tx_trade_id
    FROM {{ source('foundation_ethereum','market_evt_BuyPriceAccepted') }} f
       {% if is_incremental() %} -- this filter will only be applied on an incremental run
       WHERE f.{{incremental_predicate('evt_block_time')}}
       {% else %}
        WHERE
        f.evt_block_time >= TIMESTAMP '{{project_start_date}}'
       {% endif %}
  ),
  offers AS (
    SELECT
      evt_block_time AS block_time,
      evt_block_number AS block_number,
      tokenId AS nft_token_id,
      1 as nft_amount,
      'Sell' AS trade_category,
      case
        when (
          sellerRev = UINT256 '0'
          and creatorRev > UINT256 '0'
        ) then 'primary'
        else 'secondary'
      end as trade_type,
      seller,
      buyer,
      creatorRev + totalFees + sellerRev AS price_raw,
      contract_address AS project_contract_address,
      nftContract AS nft_contract_address,
      evt_tx_hash AS tx_hash,
      totalFees AS platform_fee_amount_raw,
      case
        when (
          sellerRev = UINT256 '0'
          and creatorRev > UINT256 '0'
        ) then UINT256 '0'
        else creatorRev
      end AS royalty_fee_amount_raw,
      evt_index as sub_tx_trade_id
    FROM {{ source('foundation_ethereum','market_evt_OfferAccepted') }} f
       {% if is_incremental() %} -- this filter will only be applied on an incremental run
       WHERE f.{{incremental_predicate('evt_block_time')}}
       {% else %}
        WHERE
        f.evt_block_time >= TIMESTAMP '{{project_start_date}}'
       {% endif %}
  ),
  private_sales AS (
    SELECT
      evt_block_time AS block_time,
      evt_block_number AS block_number,
      tokenId AS nft_token_id,
      1 as nft_amount,
      'Private Sale' AS trade_category,
      case
        when (
          sellerRev = UINT256 '0'
          and creatorFee > UINT256 '0'
        ) then 'primary'
        else 'secondary'
      end as trade_type,
      seller,
      buyer,
      creatorFee + protocolFee + sellerRev AS price_raw,
      contract_address AS project_contract_address,
      nftContract AS nft_contract_address,
      evt_tx_hash AS tx_hash,
      protocolFee AS platform_fee_amount_raw,
      case
        when (
          sellerRev = UINT256 '0'
          and creatorFee > UINT256 '0'
        ) then UINT256 '0'
        else creatorFee
      end AS royalty_fee_amount_raw,
      evt_index as sub_tx_trade_id
    FROM {{ source('foundation_ethereum','market_evt_PrivateSaleFinalized') }} f
       {% if is_incremental() %} -- this filter will only be applied on an incremental run
       WHERE f.{{incremental_predicate('evt_block_time')}}
       {% else %}
        WHERE
        f.evt_block_time >= TIMESTAMP '{{project_start_date}}'
       {% endif %}
  ),

  fixed_price_mints AS (
    SELECT
      evt_block_time AS block_time,
      evt_block_number AS block_number,
      firstTokenId AS nft_token_id,
      f."count" as nft_amount,
      'Fixed Price Mint' AS trade_category,
      'primary' as trade_type,
      0x0000000000000000000000000000000000000000 as seller,
      buyer,
      creatorRev + totalFees AS price_raw,
      contract_address AS project_contract_address,
      nftContract AS nft_contract_address,
      evt_tx_hash AS tx_hash,
      totalFees AS platform_fee_amount_raw,
      0 AS royalty_fee_amount_raw,
      evt_index as sub_tx_trade_id
    FROM {{ source('foundation_ethereum','NFTDropMarket_evt_MintFromFixedPriceDrop') }} f
       {% if is_incremental() %} -- this filter will only be applied on an incremental run
       WHERE f.{{incremental_predicate('evt_block_time')}}
       {% else %}
        WHERE
        f.evt_block_time >= TIMESTAMP '{{project_start_date}}'
       {% endif %}
  ),

  dutch_auction_prices AS (
    select
      f.nftContract,
      LEAST(
        COALESCE(MIN(f.pricePaidPerNft), MIN(cp.clearingPrice))
      ) AS price
    FROM {{ source('foundation_ethereum', 'NFTDropMarket_evt_MintFromDutchAuction') }} f
      LEFT JOIN {{ source('foundation_ethereum', 'NFTDropMarket_evt_WithdrawCreatorRevenueFromDutchAuction') }} cp
        ON cp.nftContract = f.nftContract
    group by
      1
  ),

  dutch_auction_mints AS (
    SELECT
      evt_block_time AS block_time,
      evt_block_number AS block_number,
      firstTokenId AS nft_token_id,
      1 as nft_amount,
      'Dutch Auction Mint' AS trade_category,
      'primary' as trade_type,
      0x0000000000000000000000000000000000000000 as seller,
      buyer,
      p.price AS price_raw,
      contract_address AS project_contract_address,
      f.nftContract AS nft_contract_address,
      evt_tx_hash AS tx_hash,
      -- 15% take rate for dutch auctions
      price * 0.15 AS platform_fee_amount_raw,
      0 AS royalty_fee_amount_raw,
      evt_index as sub_tx_trade_id
    FROM {{ source('foundation_ethereum', 'NFTDropMarket_evt_MintFromDutchAuction') }} f
      LEFT JOIN dutch_auction_prices p ON p.nftContract = f.nftContract
    {% if is_incremental() %} -- this filter will only be applied on an incremental run
      WHERE f.{{incremental_predicate('evt_block_time')}}
    {% else %}
      WHERE
      f.evt_block_time >= TIMESTAMP '{{project_start_date}}'
    {% endif %}
  ),

  all_foundation_trades AS (
    select
      *
    from
      auctions
    UNION ALL
    select
      *
    from
      buys
    UNION ALL
    select
      *
    from
      offers
    UNION ALL
    select
      *
    from
      private_sales
    UNION ALL
    select
      *
    from
      fixed_price_mints
    UNION ALL
    select
      *
    from
      dutch_auction_mints
  )
SELECT
  'ethereum' as blockchain,
  'foundation' as project,
  'v1' as project_version,
  t.block_time,
  t.block_number,
  t.nft_token_id,
  t.nft_amount AS nft_amount,
  t.trade_category,
  t.trade_type,
  t.seller,
  t.buyer,
  cast(t.price_raw as UINT256) as price_raw,
  {{var("ETH_ERC20_ADDRESS")}} AS currency_contract -- all trades are in ETH
,
  t.project_contract_address,
  t.nft_contract_address,
  t.tx_hash,
  cast(t.platform_fee_amount_raw as UINT256) as platform_fee_amount_raw,
  cast(t.royalty_fee_amount_raw as UINT256) as royalty_fee_amount_raw,
  cast(NULL as varbinary) AS royalty_fee_address,
  cast(NULL as varbinary) AS platform_fee_address,
  t.sub_tx_trade_id
FROM
  all_foundation_trades t
