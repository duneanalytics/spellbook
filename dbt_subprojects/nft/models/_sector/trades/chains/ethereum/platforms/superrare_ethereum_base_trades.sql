{{ config(
    schema = 'superrare_ethereum',
    
    alias = 'base_trades',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_number','tx_hash','sub_tx_trade_id'],
    )
}}
{% set project_start_date="TIMESTAMP '2018-04-05'" %}

-- raw data table with all sales on superrare platform -- both primary and secondary
with all_superrare_sales as (
    -- 0x2947f98c42597966a0ec25e92843c09ac17fbaa7 -- SuperRareMarketAuction
    -- 0x65b49f7aee40347f5a90b714be4ef086f3fe5e2c -- SuperRareMarketAuction : V2 https://github.com/superrare/pixura-contracts/blob/66b39164255d29d07e00b4ad8df206c379bf35e7/contracts/build/SuperRareMarketAuctionV2.json
    select  evt_block_time as block_time
            , evt_block_number as block_number
            , "_originContract" as contract_address
            , "_tokenId" as nft_token_id
            , "_seller" as seller
            , "_buyer" as buyer
            , "_amount" as price_raw
            , evt_tx_hash
            , {{var("ETH_ERC20_ADDRESS")}} as currency_contract
            , evt_index as sub_tx_trade_id
    from {{ source('superrare_ethereum','SuperRareMarketAuction_evt_Sold') }}
    {% if is_incremental() %}
    where {{incremental_predicate('evt_block_time')}}
    {% endif %}

    union all

    select evt_block_time
            , evt_block_number
            , contract_address
            , "_tokenId" as nft_token_id
            , "_seller" as seller
            , "_buyer" as buyer
            , "_amount" as price_raw
            , evt_tx_hash
            , {{var("ETH_ERC20_ADDRESS")}} as currency_contract
            , evt_index as sub_tx_trade_id
    from {{ source('superrare_ethereum','SuperRare_evt_Sold') }}
    {% if is_incremental() %}
    where {{incremental_predicate('evt_block_time')}}
    {% endif %}

    union all

    select evt_block_time
            , evt_block_number
            , "_originContract" as contract_address
            , "_tokenId" as nft_token_id
            , "_seller" as seller
            , "_bidder" as buyer
            , "_amount" as price_raw
            , evt_tx_hash
            , {{var("ETH_ERC20_ADDRESS")}} as currency_contract
            , evt_index as sub_tx_trade_id
    from {{ source('superrare_ethereum','SuperRareMarketAuction_evt_AcceptBid') }}
    {% if is_incremental() %}
    where {{incremental_predicate('evt_block_time')}}
    {% endif %}

    union all

    select evt_block_time
            , evt_block_number
            , contract_address
            , "_tokenId" as nft_token_id
            , "_seller" as seller
            , "_bidder" as buyer
            , "_amount" as price_raw
            , evt_tx_hash
            , {{var("ETH_ERC20_ADDRESS")}} as currency_contract
            , evt_index as sub_tx_trade_id
    from {{ source('superrare_ethereum','SuperRare_evt_AcceptBid') }}
    {% if is_incremental() %}
    where {{incremental_predicate('evt_block_time')}}
    {% endif %}

    union all

    select evt_block_time
            , evt_block_number
            , "_originContract" as contract_address
            , "_tokenId" as nft_token_id
            , "_seller" as seller
            , "_bidder" as buyer
            , "_amount" as price_raw
            , evt_tx_hash
            , "_currencyAddress" as currency_contract
            , evt_index as sub_tx_trade_id
    from {{ source('superrare_ethereum','SuperRareBazaar_evt_AcceptOffer') }}
    {% if is_incremental() %}
    where {{incremental_predicate('evt_block_time')}}
    {% endif %}

    union all

    select evt_block_time
            , evt_block_number
            , "_contractAddress" as contract_address
            , "_tokenId" as nft_token_id
            , "_seller" as seller
            , "_bidder" as buyer
            , "_amount" as price_raw
            , evt_tx_hash
            , "_currencyAddress" as currency_contract
            , evt_index as sub_tx_trade_id
    from {{ source('superrare_ethereum','SuperRareBazaar_evt_AuctionSettled') }}
    {% if is_incremental() %}
    where {{incremental_predicate('evt_block_time')}}
    {% endif %}

    union all

    select evt_block_time
            , evt_block_number
            , "_originContract" as contract_address
            , "_tokenId" as nft_token_id
            , "_seller" as seller
            , "_buyer" as buyer
            , "_amount" as price_raw
            , evt_tx_hash
            , "_currencyAddress" as currency_contract
            , evt_index as sub_tx_trade_id
    from {{ source('superrare_ethereum','SuperRareBazaar_evt_Sold') }}
    {% if is_incremental() %}
    where {{incremental_predicate('evt_block_time')}}
    {% endif %}

    union all

    -- 0x8c9f364bf7a56ed058fc63ef81c6cf09c833e656 -- SuperRareAuctionHouse : https://github.com/superrare/pixura-contracts/blob/66b39164255d29d07e00b4ad8df206c379bf35e7/contracts/build/SuperRareAuctionHouse.json
    select  evt_block_time
            , evt_block_number
            , "_contractAddress" as contract_address
            , "_tokenId" as nft_token_id
            , "_seller" as seller
            , "_bidder" as buyer
            , "_amount" as price_raw
            , evt_tx_hash
            , {{var("ETH_ERC20_ADDRESS")}} as currency_contract -- SuperRareAuctionHouse hadn't currency_contract in AuctionSettled event.
            , evt_index as sub_tx_trade_id
    from {{ source('superrare_ethereum','SuperRareAuctionHouse_evt_AuctionSettled') }}
    {% if is_incremental() %}
    where {{incremental_predicate('evt_block_time')}}
    {% endif %}
)

SELECT
    'ethereum' as blockchain,
    'superrare' as project,
    'v1' as project_version,
    a.block_time,
    a.block_number,
    a.nft_token_id,
    uint256 '1' as nft_amount,
    'Buy' as trade_category,
    a.seller,
    a.buyer,
    a.price_raw as price_raw,
    a.currency_contract,
    a.contract_address as nft_contract_address,
    cast(NULL as varbinary) as project_contract_address,
    a.evt_tx_hash as tx_hash,
    case
        when a.seller = coalesce(minter.to, minter_superrare.to)
        then 'primary'
        else 'secondary'
    end as trade_type,
    case
        when a.seller = coalesce(minter.to, minter_superrare.to)
        then cast(ROUND((double '0.03' + double '0.15') * (a.price_raw)) as uint256) -- superrare takes fixed 3% fee + 15% commission on primary sales
        else cast(ROUND(double '0.03' * (a.price_raw)) as uint256)    -- fixed 3% fee
    end as platform_fee_amount_raw,
    case
        when a.seller = coalesce(minter.to, minter_superrare.to)
        then uint256 '0'
        else cast(ROUND(double '0.10' * (a.price_raw)) as uint256)  -- fixed 10% royalty fee on secondary sales
    end as royalty_fee_amount_raw,
    cast(NULL as varbinary) as royalty_fee_address,
    cast(NULL as varbinary) as platform_fee_address,
    sub_tx_trade_id
from all_superrare_sales a
left join {{ source('erc721_ethereum','evt_transfer') }} minter on minter.contract_address = a.contract_address
    and minter.tokenId = a.nft_token_id
    and minter."from" = 0x0000000000000000000000000000000000000000
    {% if is_incremental() %}
    and minter.{{incremental_predicate('evt_block_time')}}
    {% else %}
    and minter.evt_block_time >= {{ project_start_date }}
    {% endif %}

left join {{ source('erc20_ethereum','evt_transfer') }} minter_superrare on minter_superrare.contract_address = a.contract_address
    and minter_superrare.value = a.nft_token_id
    and minter_superrare."from" = 0x0000000000000000000000000000000000000000
    {% if is_incremental() %}
    and minter_superrare.{{incremental_predicate('evt_block_time')}}
    {% else %}
    and minter_superrare.evt_block_time >= {{ project_start_date }}
    {% endif %}

