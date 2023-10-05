-- Element NFT trades (re-usable macro for all chains)
{% macro element_v1_base_trades_legacy(erc721_sell_order_filled, erc721_buy_order_filled, erc1155_sell_order_filled, erc1155_buy_order_filled) %}


SELECT
  date_trunc('day',evt_block_time) as block_date
, evt_block_time AS block_time
, evt_block_number AS block_number
, 'Buy' AS trade_category
, 'secondary' AS trade_type
, erc721Token AS nft_contract_address
, erc721TokenId AS nft_token_id
, 1 AS nft_amount
, taker AS buyer
, maker AS seller
, cast(erc20TokenAmount as decimal(38)) AS price_raw
, CASE WHEN erc20Token='0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' THEN '{{ var("ETH_ERC20_ADDRESS") }}'
    ELSE erc20Token END AS currency_contract
, cast(0 as decimal(38)) as platform_fee_amount_raw
, cast(0 as decimal(38)) as royalty_fee_amount_raw
, cast(null as varchar(1)) as platform_fee_address
, cast(null as varchar(1)) as royalty_fee_address
, contract_address AS project_contract_address
, evt_tx_hash AS tx_hash
, evt_index as sub_tx_trade_id
FROM {{ erc721_sell_order_filled }}
{% if is_incremental() %}
WHERE evt_block_time >= date_trunc("day", now() - interval '1 week')
{% endif %}

UNION ALL

SELECT
  date_trunc('day',evt_block_time) as block_date
, evt_block_time AS block_time
, evt_block_number AS block_number
, 'Sell' AS trade_category
, 'secondary' AS trade_type
, erc721Token AS nft_contract_address
, erc721TokenId AS nft_token_id
, 1 AS nft_amount
, maker AS buyer
, taker AS seller
, cast(erc20TokenAmount as decimal(38)) AS price_raw
, CASE WHEN erc20Token='0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' THEN '{{ var("ETH_ERC20_ADDRESS") }}'
    ELSE erc20Token END AS currency_contract
, cast(0 as decimal(38)) as platform_fee_amount_raw
, cast(0 as decimal(38)) as royalty_fee_amount_raw
, cast(null as varchar(1)) as platform_fee_address
, cast(null as varchar(1)) as royalty_fee_address
, contract_address AS project_contract_address
, evt_tx_hash AS tx_hash
, evt_index as sub_tx_trade_id
FROM {{ erc721_buy_order_filled }}
{% if is_incremental() %}
WHERE evt_block_time >= date_trunc("day", now() - interval '1 week')
{% endif %}

UNION ALL

SELECT
  date_trunc('day',evt_block_time) as block_date
, evt_block_time AS block_time
, evt_block_number AS block_number
, 'Buy' AS trade_category
, 'secondary' AS trade_type
, erc1155Token AS nft_contract_address
, erc1155TokenId AS nft_token_id
, 1 AS nft_amount
, taker AS buyer
, maker AS seller
, cast(erc20FillAmount as decimal(38)) AS price_raw
, CASE WHEN erc20Token='0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' THEN '{{ var("ETH_ERC20_ADDRESS") }}'
    ELSE erc20Token END AS currency_contract
, cast(0 as decimal(38)) as platform_fee_amount_raw
, cast(0 as decimal(38)) as royalty_fee_amount_raw
, cast(null as varchar(1)) as platform_fee_address
, cast(null as varchar(1)) as royalty_fee_address
, contract_address AS project_contract_address
, evt_tx_hash AS tx_hash
, evt_index as sub_tx_trade_id
FROM {{ erc1155_buy_order_filled }}
{% if is_incremental() %}
WHERE evt_block_time >= date_trunc("day", now() - interval '1 week')
{% endif %}

UNION ALL

SELECT
  date_trunc('day',evt_block_time) as block_date
, evt_block_time AS block_time
, evt_block_number AS block_number
, 'Buy' AS trade_category
, 'secondary' AS trade_type
, erc1155Token AS nft_contract_address
, erc1155TokenId AS nft_token_id
, 1 AS nft_amount
, maker AS buyer
, taker AS seller
, cast(erc20FillAmount as decimal(38)) AS price_raw
, CASE WHEN erc20Token='0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' THEN '{{ var("ETH_ERC20_ADDRESS") }}'
    ELSE erc20Token END AS currency_contract
, cast(0 as decimal(38)) as platform_fee_amount_raw
, cast(0 as decimal(38)) as royalty_fee_amount_raw
, cast(null as varchar(1)) as platform_fee_address
, cast(null as varchar(1)) as royalty_fee_address
, contract_address AS project_contract_address
, evt_tx_hash AS tx_hash
, evt_index as sub_tx_trade_id
FROM {{ erc1155_sell_order_filled }}
{% if is_incremental() %}
WHERE evt_block_time >= date_trunc("day", now() - interval '1 week')
{% endif %}

{% endmacro %}
