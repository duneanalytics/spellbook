{{ config(
    schema = 'opensea_v1_ethereum',
    alias = 'base_trades',
    materialized = 'incremental',
    incremental_strategy = 'merge',
    file_format = 'delta',
    unique_key = ['block_number','tx_hash','sub_tx_trade_id'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

{% set ETH_ERC20='0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' %}
{% set ZERO_ADDR='0x0000000000000000000000000000000000000000' %}
{% set SHARED_STOREFRONT='0x495f947276749ce646f68ac8c248420045cb7b5e' %}
{% set OS_WALLET='0x5b3256965e7c3cf26e11fcaf296dfc8807c01073' %}
{% set START_DATE='2018-07-18' %}
{% set END_DATE='2022-08-02' %}

WITH wyvern_call_data as (
    SELECT
      call_tx_hash as tx_hash,
      call_block_time as block_time,
      call_block_number as block_number,
      element_at(addrs,1) as project_contract_address,
      element_at(addrs,2) buyer,                           -- maker of buy order
      element_at(addrs,9) as seller,                       -- maker of sell order
      CASE -- we check which side defines the fee_recipient to determine the category
        when element_at(addrs,4) != {{ZERO_ADDR}} then 'Sell'
        when element_at(addrs,11) != {{ZERO_ADDR}} then 'Buy'
      END as trade_category,
      CASE when element_at(feeMethodsSidesKindsHowToCalls,3) = 0 then 'Fixed price' else 'Auction' END as sale_type,
      CASE -- buyer payment token
          WHEN element_at(addrs,7) = {{ZERO_ADDR}} THEN {{ETH_ERC20}}
          ELSE element_at(addrs,7)
      END AS currency_contract,
      (element_at(addrs,7) = {{ZERO_ADDR}}) AS native_eth,
      CASE -- fee_recipient
        when element_at(addrs,4) != {{ZERO_ADDR}} then element_at(addrs,4)
        when element_at(addrs,11) != {{ZERO_ADDR}} then element_at(addrs,11)
      END as fee_recipient,
      CASE WHEN element_at(addrs,5) = {{SHARED_STOREFRONT}} THEN 'Mint'  -- todo: this needs to be verified if correct
        ELSE 'Trade' END as evt_type,
      CASE
        when element_at(addrs,4) != {{ZERO_ADDR}}  -- SELL
            then (case when (element_at(uints,1)+element_at(uints,2))/1e4 < 0.025 -- we assume no marketplace fees then..
                then 0.0
                else 0.025 end)
        when element_at(addrs,11) != {{ZERO_ADDR}}  -- BUY
            then (case when (element_at(addrs,10) != {{ZERO_ADDR}} --private listing
                    OR (element_at(uints,10)+element_at(uints,11))/1e4 < 0.025) -- we assume no marketplace fees then..
                then 0.0
                else 0.025 end)
      END as platform_fee,
      CASE
        when element_at(addrs,4) != {{ZERO_ADDR}}  -- SELL
            then case when (element_at(uints,1)+element_at(uints,2))/1e4 < 0.025 -- we assume no marketplace fees then..
                then (element_at(uints,1)+element_at(uints,2))/1e4
                else (element_at(uints,1)+element_at(uints,2)-250)/1e4 end
        when element_at(addrs,11) != {{ZERO_ADDR}}  -- BUY
            then case when (element_at(addrs,10) != {{ZERO_ADDR}} --private listing
                    OR (element_at(uints,10)+element_at(uints,11))/1e4 < 0.025) -- we assume no marketplace fees then..
                then (element_at(uints,10)+element_at(uints,11))/1e4
                else (element_at(uints,10)+element_at(uints,11)-250)/1e4 end
      END as royalty_fee,
      case when element_at(addrs,11) != {{ZERO_ADDR}} and element_at(addrs,7) = {{ZERO_ADDR}}
        then 1.0 + element_at(uints,11)/1e4      -- on ERC20 BUY: add sell side taker fee (this is not included in the price from the evt) https://etherscan.io/address/0x7be8076f4ea4a4ad08075c2508e481d6c946d12b#code#L838
        else 1.0 end as price_correction,
      call_trace_address,
      row_number() over (partition by call_block_number, call_tx_hash order by call_trace_address asc) as tx_call_order
    FROM
      {{ source('opensea_ethereum','wyvernexchange_call_atomicmatch_') }} wc
    WHERE 1=1
    {% if is_incremental() %}
    AND {{incremental_predicate('call_block_time')}}
    {% endif %}
    AND (element_at(addrs,4) = {{OS_WALLET}} OR element_at(addrs,11) = {{OS_WALLET}}) -- limit to OpenSea
    AND call_success = true
    AND call_block_time >= TIMESTAMP '{{START_DATE}}' AND call_block_time <= TIMESTAMP '{{END_DATE}}'

),


-- needed to pull correct prices
order_prices as (
    select
        evt_block_number as block_number,
        evt_tx_hash as tx_hash,
        evt_index as order_evt_index,
        price,
        row_number() over (partition by evt_block_number, evt_tx_hash order by evt_index asc) as orders_evt_order,
        lag(evt_index) over (partition by evt_block_number, evt_tx_hash order by evt_index asc) as prev_order_evt_index
    from {{ source('opensea_ethereum','wyvernexchange_evt_ordersmatched') }}
    WHERE evt_block_time >= TIMESTAMP '{{START_DATE}}' AND evt_block_time <= TIMESTAMP '{{END_DATE}}'
    {% if is_incremental() %}
    AND {{incremental_predicate('evt_block_time')}}
    {% endif %}

),
-- needed to pull token_id, token_amounts, token_standard and nft_contract_address
nft_transfers as (
    select
        block_time,
        block_number,
        "from",
        to,
        contract_address as nft_contract_address,
        token_standard,
        token_id,
        amount,
        evt_index,
        tx_hash
    from {{ ref('nft_ethereum_transfers') }}
    WHERE block_time >= TIMESTAMP '{{START_DATE}}' AND block_time <= TIMESTAMP '{{END_DATE}}'
    {% if is_incremental() %}
    AND {{incremental_predicate('block_time')}}
    {% endif %}
),

-- join call and order data
enhanced_orders as (
    select
        c.*,
        o.price * c.price_correction as total_amount_raw,
        o.order_evt_index,
        o.prev_order_evt_index
    from wyvern_call_data c
    inner join order_prices o -- there should be a 1-to-1 match here
    ON c.block_number = o.block_number
        AND c.tx_hash = o.tx_hash
        AND c.tx_call_order = o.orders_evt_order -- in case of multiple calls in 1 tx_hash
),

-- join nft transfers and split trades, we divide the total amount (and fees) proportionally for bulk trades
enhanced_trades as (
    select
    o.*,
    nft.nft_contract_address,
    nft.token_standard,
    nft.token_id,
    nft.amount as number_of_items,
    nft.to as nft_to,
    nft."from" as nft_from,
    cast(total_amount_raw*amount/(sum(nft.amount) over (partition by o.block_number, o.tx_hash, o.order_evt_index)) as uint256) as amount_raw,
    'secondary' as trade_type,
    nft.evt_index as nft_evt_index
    from enhanced_orders o
    inner join nft_transfers nft
    ON o.block_number = nft.block_number
        AND o.tx_hash = nft.tx_hash
        AND ((trade_category = 'Buy' AND nft."from" = o.seller) OR (trade_category = 'Sell' AND nft.to = o.buyer))
        AND nft.evt_index <= o.order_evt_index and (prev_order_evt_index is null OR nft.evt_index > o.prev_order_evt_index )
)

SELECT
  'ethereum' as blockchain,
  'opensea' as project,
  'v1' as project_version,
  project_contract_address,
  t.block_time,
  t.block_number,
  t.tx_hash,
  t.nft_contract_address,
  t.token_id as nft_token_id,
  t.amount_raw as price_raw,
  t.trade_category,
  t.trade_type,
  t.number_of_items as nft_amount,
  coalesce(t.nft_from, t.seller) AS seller,
  coalesce(t.nft_to, t.buyer) as buyer,
  t.currency_contract,
  -- some complex price calculations, (t.amount_raw/t.price_correction) is the original base price for fees.
  cast(platform_fee * (t.amount_raw/t.price_correction) as uint256) AS platform_fee_amount_raw,
  cast(royalty_fee * (t.amount_raw/t.price_correction) as uint256) AS royalty_fee_amount_raw,
  t.fee_recipient as royalty_fee_address,
  cast(null as varbinary) as platform_fee_address,
  row_number() over (partition by tx_hash order by nft_evt_index) as sub_tx_trade_id -- using this downstream in nft mints
FROM enhanced_trades t
