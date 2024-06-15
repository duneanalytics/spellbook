{{ config(
    schema = 'nft_solana',
    alias = 'base_trades',
    materialized = 'view'
    )
}}

-- transformative view to get the solana trades into nft.trades as much as possible
-- columns are filled on a best effort basis, but many will probably hold null values..
-- the main goal is to get transaction count and overall volume correct
-- any values cast to varbinary will be wrongly encoded but will still hold equality and uniqueness constraints


SELECT
    blockchain,
    project,
    version as project_version,
    block_date,
    block_month,
    block_time,
    block_slot as block_number,
    cast(tx_id as varbinary) as tx_hash,
    cast(project_program_id as varbinary) as project_contract_address,
    trade_category,
    trade_type,
    cast(buyer as varbinary) as buyer,
    cast(seller as varbinary) as seller,
    cast(null as varbinary) as nft_contract_address,
    cast(null as uint256) as nft_token_id,
    coalesce(try(cast(number_of_items as uint256)), uint256 '0') as nft_amount,
    coalesce(try(cast(amount_raw as uint256)), uint256 '0') as price_raw,
    case when currency_symbol = 'SOL' then 0x069b8857feab8184fb687f634618c035dac439dc1aeb3b5598a0f00000000001 else null end as currency_contract,
    coalesce(try(cast(taker_fee_amount_raw + maker_fee_amount_raw as uint256)), uint256 '0') as platform_fee_amount_raw,
    coalesce(try(cast(royalty_fee_amount_raw as uint256)), uint256 '0') as royalty_fee_amount_raw,
    cast(null as varbinary) as platform_fee_address,
    cast(null as varbinary) as royalty_fee_address,
    cast(null as varbinary) as tx_from,
    cast(null as varbinary) as tx_to,
    cast(null as varbinary) as tx_data_marker,                                                  -- forwarc compatibility with aggregator marker matching
    row_number() over (partition by tx_id order by leaf_id) as sub_tx_trade_id       -- intermediate fix to fill this column
FROM {{ ref('nft_solana_trades') }}
