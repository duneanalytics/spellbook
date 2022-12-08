{{ config(
    schema = 'opensea_v1_ethereum',
    alias = 'events',
    materialized = 'table',
    file_format = 'delta',
    partition_by = ['block_date']
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
      addrs[0] as project_contract_address,
      addrs[1] buyer,                           -- maker of buy order
      addrs[8] as seller,                       -- maker of sell order
      CASE -- we check which side defines the fee_recipient to determine the category
        when addrs[3] != '{{ZERO_ADDR}}' then 'Sell'
        when addrs[10] != '{{ZERO_ADDR}}' then 'Buy'
      END as trade_category,
      CASE when feeMethodsSidesKindsHowToCalls[2] = 0 then 'Fixed price' else 'Auction' END as sale_type,
      CASE -- buyer payment token
          WHEN addrs [6] = '{{ZERO_ADDR}}' THEN '{{ETH_ERC20}}'
          ELSE addrs [6]
      END AS currency_contract,
      (addrs [6] = '{{ZERO_ADDR}}') AS native_eth,
      CASE -- fee_recipient
        when addrs[3] != '{{ZERO_ADDR}}' then addrs[3]
        when addrs[10] != '{{ZERO_ADDR}}' then addrs[10]
      END as fee_recipient,
      CASE WHEN addrs[4] = '{{SHARED_STOREFRONT}}' THEN 'Mint'
        ELSE 'Trade' END as evt_type,
      CASE -- Relayer fee contains both royalty, marketplace and protocol fees, There were never any 'Wyvern' protocol fees so we can ignore them
        when addrs[3] != '{{ZERO_ADDR}}' then uints[1]/10000
        when addrs[10] != '{{ZERO_ADDR}}' then uints[0]/10000
      END as seller_fee_percentage,    -- incl in price.
      CASE
        when addrs[3] != '{{ZERO_ADDR}}' then uints[0]/10000
        when addrs[10] != '{{ZERO_ADDR}}' then uints[1]/10000
      END as buyer_fee_percentage,    -- excl in price.
      call_trace_address,
      row_number() over (partition by call_block_number, call_tx_hash order by call_trace_address asc) as tx_call_order
    FROM
      {{ source('opensea_ethereum','wyvernexchange_call_atomicmatch_') }} wc
    WHERE 1=1
        AND (addrs[3] = '{{OS_WALLET}}' OR addrs[10] = '{{OS_WALLET}}') -- limit to OpenSea
    AND call_success = true
    AND call_block_time >= '{{START_DATE}}' AND call_block_time <= '{{END_DATE}}'
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
    WHERE evt_block_time >= '{{START_DATE}}' AND evt_block_time <= '{{END_DATE}}'

),
-- needed to pull token_id, token_amounts, token_standard and nft_contract_address
nft_transfers as (
    select
        block_time,
        block_number,
        from,
        to,
        contract_address as nft_contract_address,
        token_standard,
        token_id,
        amount,
        evt_index,
        tx_hash
    from {{ ref('nft_ethereum_transfers') }}
    WHERE block_time >= '{{START_DATE}}' AND block_time <= '{{END_DATE}}'
),

-- join call and order data
enhanced_orders as (
    select
        c.*,
        o.price * (1+c.buyer_fee_percentage) as total_amount_raw,
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
    nft.from as nft_from,
    total_amount_raw*amount/(sum(nft.amount) over (partition by o.block_number, o.tx_hash, o.order_evt_index)) as amount_raw,
    case when count(nft.evt_index) over (partition by o.block_number, o.tx_hash, o.order_evt_index) > 1
        then concat('Bundle trade: ',sale_type)
        else concat('Single Item Trade: ',sale_type)
    END as trade_type,
    nft.evt_index as nft_evt_index
    from enhanced_orders o
    inner join nft_transfers nft
    ON o.block_number = nft.block_number
        AND o.tx_hash = nft.tx_hash
        AND ((trade_category = 'Buy' AND nft.from = o.seller) OR (trade_category = 'Sell' AND nft.to = o.buyer))
        AND nft.evt_index <= o.order_evt_index and (prev_order_evt_index is null OR nft.evt_index > o.prev_order_evt_index )
)


SELECT
  'ethereum' as blockchain,
  'opensea' as project,
  'v1' as version,
  project_contract_address,
  TRY_CAST(date_trunc('DAY', t.block_time) AS date) AS block_date,
  t.block_time,
  t.block_number,
  t.tx_hash,
  tx.from,
  tx.to,
  t.nft_contract_address,
  t.token_standard,
  nft.name AS collection,
  t.token_id,
  CAST(t.amount_raw AS DECIMAL(38,0)) as amount_raw,
  t.amount_raw / power(10,erc20.decimals) as amount_original,
  t.amount_raw / power(10,erc20.decimals) * p.price AS amount_usd,
  t.trade_category,
  t.trade_type,
  CAST(t.number_of_items AS DECIMAL(38,0)) as number_of_items,
  coalesce(t.nft_from, t.seller) AS seller,
  coalesce(t.nft_to, t.buyer) as buyer,
  t.evt_type,
  CASE WHEN t.native_eth THEN 'ETH' ELSE erc20.symbol END AS currency_symbol,
  t.currency_contract,
  agg.name as aggregator_name,
  agg.contract_address as aggregator_address,
  tx.from as tx_from,
  tx.to as tx_to,
  -- some complex price calculations
  2.5/100 * (t.amount_raw/(1+buyer_fee_percentage)) AS platform_fee_amount_raw,
  2.5/100 * (t.amount_raw/(1+buyer_fee_percentage)) / power(10,erc20.decimals) AS platform_fee_amount,
  2.5/100 * (t.amount_raw/(1+buyer_fee_percentage)) / power(10,erc20.decimals) * p.price AS platform_fee_amount_usd,
  CAST(2.5 AS DOUBLE) AS platform_fee_percentage,
  CAST((100 * (seller_fee_percentage + buyer_fee_percentage - 2.5/100)) AS DOUBLE) as royalty_fee_percentage,
  ((seller_fee_percentage + buyer_fee_percentage - 2.5/100) / (1 + buyer_fee_percentage) * amount_raw) AS royalty_fee_amount_raw,
  ((seller_fee_percentage + buyer_fee_percentage - 2.5/100) / (1 + buyer_fee_percentage) * amount_raw) / power(10,erc20.decimals) AS royalty_fee_amount,
  ((seller_fee_percentage + buyer_fee_percentage - 2.5/100) / (1 + buyer_fee_percentage) * amount_raw) / power(10,erc20.decimals) * p.price AS royalty_fee_amount_usd,
  t.fee_recipient as royalty_fee_receive_address,
  CASE WHEN t.native_eth THEN 'ETH' ELSE erc20.symbol END AS royalty_fee_currency_symbol,
  'wyvern-opensea' || '-' || t.tx_hash || '-' || t.order_evt_index || '-' || t.nft_evt_index || '-' || token_id as unique_trade_id
FROM enhanced_trades t
INNER JOIN {{ source('ethereum','transactions') }} tx ON t.block_number = tx.block_number AND t.tx_hash = tx.hash
    AND tx.block_time >= '{{START_DATE}}' AND tx.block_time <= '{{END_DATE}}'
LEFT JOIN {{ ref('tokens_nft') }} nft ON nft.contract_address = t.nft_contract_address and nft.blockchain = 'ethereum'
LEFT JOIN {{ ref('nft_aggregators') }} agg ON agg.contract_address = tx.to AND agg.blockchain = 'ethereum'
LEFT JOIN {{ source('prices', 'usd') }} p ON p.minute = date_trunc('minute', t.block_time)
    AND p.contract_address = t.currency_contract
    AND p.blockchain ='ethereum'
    AND minute >= '{{START_DATE}}' AND minute <= '{{END_DATE}}'
LEFT JOIN {{ ref('tokens_erc20') }} erc20 ON erc20.contract_address = t.currency_contract and erc20.blockchain = 'ethereum'
;
