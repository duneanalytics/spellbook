{{ config(
    schema = 'king_of_destiny_nova',
    alias = 'base_trades',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_number','tx_hash','sub_tx_trade_id'],
    )
}}


select
    'nova' as blockchain
    ,'king_of_destiny' as project
    ,'v1' as project_version
   ,s.evt_block_time as block_time
     ,cast(date_trunc('day', s.evt_block_time) as date) as block_date
    ,cast(date_trunc('month', s.evt_block_time) as date) as block_month
   ,s.evt_block_number as block_number
   ,s.evt_tx_hash as tx_hash
    ,'secondary' as trade_type
    ,'Buy' as trade_category
   ,s.evt_index
   ,s.buyer
   ,s.listingCreator as seller
   ,s.contract_address as project_contract_address
   ,s.tokenId as nft_token_id
   ,s.assetContract as nft_contract_address
   ,s.quantityBought as nft_amount
   ,s.totalPricePaid as price_raw
   ,from_hex(json_extract_scalar(l.listing, '$.currency')) as currency_contract
   ,element_at(r.output_amounts,1) as royalty_fee_amount_raw
   ,cast(null as uint256) as platform_fee_amount_raw
   ,cast(null as varbinary) as platform_fee_address
   ,element_at(r.output_recipients,1) as royalty_fee_address
   ,s.listingId as listing_id
   ,s.evt_index as sub_tx_trade_id
from {{source('king_of_destiny_nova','MarketplaceV3_DirectListingsLogic_evt_NewSale')}} s
left join  {{source('king_of_destiny_nova','MarketplaceV3_DirectListingsLogic_evt_NewListing')}} l
    on s.listingId = l.listingId
    {% if is_incremental() %}
    and {{incremental_predicate('l.evt_block_time')}}
    {% endif %}
left join  {{source('king_of_destiny_nova','MarketplaceV3_call_GetRoyalty')}}  r
    on s.evt_tx_hash = r.call_tx_hash
    and s.evt_block_number = r.call_block_number
    and s.totalPricePaid = r.value
    and s.assetContract = r.tokenAddress
    and s.tokenId = r.tokenId
    {% if is_incremental() %}
    and {{incremental_predicate('r.call_block_time')}}
    {% endif %}
WHERE
    s.evt_tx_hash != 0xde7696b64c41bc6ffe2834ba51da7f04121f49c08fc89de724413bf81e6e16be
    and (s.evt_index not in (13, 19))
{% if is_incremental() %}
 and {{incremental_predicate('s.evt_block_time')}}
{% endif %}
