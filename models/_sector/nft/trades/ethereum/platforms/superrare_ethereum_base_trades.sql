{{ config(
    schema = 'superrare_ethereum',
    tags = ['dunesql', 'prod_exclude'],
    alias = alias('base_trades'),
    partition_by = ['block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_number','tx_hash','sub_tx_trade_id'],
    )
}}
{% set project_start_date="TIMESTAMP '2018-04-05'" %}

-- raw data table with all sales on superrare platform -- both primary and secondary
with all_superrare_sales as (
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
    where evt_block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}

    union all

    select evt_block_time
            , evt_block_number
            , contract_address
            , "_tokenId"
            , "_seller"
            , "_buyer"
            , "_amount"
            , evt_tx_hash
            , {{var("ETH_ERC20_ADDRESS")}}
            , evt_index
    from {{ source('superrare_ethereum','SuperRare_evt_Sold') }}
    {% if is_incremental() %}
    where evt_block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}

    union all

    select evt_block_time
            , evt_block_number
            , "_originContract" as contract_address
            , "_tokenId"
            , "_seller"
            , "_bidder"
            , "_amount"
            , evt_tx_hash
            , {{var("ETH_ERC20_ADDRESS")}}
            , evt_index
    from {{ source('superrare_ethereum','SuperRareMarketAuction_evt_AcceptBid') }}
    {% if is_incremental() %}
    where evt_block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}

    union all

    select evt_block_time
            , evt_block_number
            , contract_address
            , "_tokenId"
            , "_seller"
            , "_bidder"
            , "_amount"
            , evt_tx_hash
            , {{var("ETH_ERC20_ADDRESS")}}
            , evt_index
    from {{ source('superrare_ethereum','SuperRare_evt_AcceptBid') }}
    {% if is_incremental() %}
    where evt_block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}

    union all

    select evt_block_time
            , evt_block_number
            , "_originContract"
            , "_tokenId"
            , "_seller"
            , "_bidder"
            , "_amount"
            , evt_tx_hash
            , "_currencyAddress"
            , evt_index
    from {{ source('superrare_ethereum','SuperRareBazaar_evt_AcceptOffer') }}
    {% if is_incremental() %}
    where evt_block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}

    union all

    select evt_block_time
            , evt_block_number
            , "_contractAddress"
            , "_tokenId"
            , "_seller"
            , "_bidder"
            , "_amount"
            , evt_tx_hash
            , "_currencyAddress"
            , evt_index
    from {{ source('superrare_ethereum','SuperRareBazaar_evt_AuctionSettled') }}
    {% if is_incremental() %}
    where evt_block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}

    union all

    select evt_block_time
            , evt_block_number
            , "_originContract"
            , "_tokenId"
            , "_seller"
            , "_buyer"
            , "_amount"
            , evt_tx_hash
            , "_currencyAddress"
            , evt_index
    from {{ source('superrare_ethereum','SuperRareBazaar_evt_Sold') }}
    {% if is_incremental() %}
    where evt_block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}

    union all

    -- Superrare AuctionHouse (not decoded)
    select  block_time
            , block_number
            , bytearray_substring(topic1, 13, 20) AS contract_address
            , bytearray_to_uint256(topic3) AS nft_token_id
            , bytearray_substring(data, 13, 20) AS seller
            , bytearray_substring(topic2, 13, 20) AS buyer
            , bytearray_to_uint256(bytearray_substring(data, 33, 32)) AS price_raw
            , tx_hash
            , {{var("ETH_ERC20_ADDRESS")}}
            , index
    from {{ source('ethereum','logs') }}
    where contract_address = 0x8c9f364bf7a56ed058fc63ef81c6cf09c833e656
        and topic0 = 0xea6d16c6bfcad11577aef5cc6728231c9f069ac78393828f8ca96847405902a9
        {% if is_incremental() %}
        and block_time >= date_trunc('day', now() - interval '7' day)
        {% else %}
        and block_time >= {{ project_start_date }}
        {% endif %}

    union all

    -- Superrare Marketplace (not decoded)
    select block_time
            , block_number
            , bytearray_substring(topic1, 13, 20) AS contract_address
            , bytearray_to_uint256(bytearray_substring(data, 33, 32)) AS nft_token_id
            , bytearray_substring(topic3, 13, 20) AS seller
            , bytearray_substring(topic2, 13, 20) AS buyer
            , bytearray_to_uint256(bytearray_substring(data, 1, 32)) as price_raw
            , tx_hash
            , {{var("ETH_ERC20_ADDRESS")}}
            , index
    from {{ source('ethereum','logs') }}
    where contract_address =  0x65b49f7aee40347f5a90b714be4ef086f3fe5e2c
        and topic0 in (0x2a9d06eec42acd217a17785dbec90b8b4f01a93ecd8c127edd36bfccf239f8b6
                        ,0x5764dbcef91eb6f946584f4ea671217c686fa7e858ce4f9f42d08422b86556a9)
        {% if is_incremental() %}
        and block_time >= date_trunc('day', now() - interval '7' day)
        {% else %}
        and block_time >= {{ project_start_date }}
        {% endif %}
)

SELECT
    cast(date_trunc('month', a.block_time) AS date) AS block_date,
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
        then cast(ROUND((0.03+0.15) * (a.price_raw)) as uint256) -- superrare takes fixed 3% fee + 15% commission on primary sales
        else cast(ROUND(0.03 * (a.price_raw)) as uint256)    -- fixed 3% fee
    end as platform_fee_amount_raw,
    case
        when a.seller = coalesce(minter.to, minter_superrare.to)
        then cast(0 as uint256)
        else cast(ROUND(0.10 * (a.price_raw)) as uint256)  -- fixed 10% royalty fee on secondary sales
    end as royalty_fee_amount_raw,
    cast(NULL as varbinary) as royalty_fee_address,
    cast(NULL as varbinary) as platform_fee_address,
    sub_tx_trade_id
from all_superrare_sales a
left join {{ source('erc721_ethereum','evt_transfer') }} minter on minter.contract_address = a.contract_address
    and minter.tokenId = a.nft_token_id
    and minter."from" = 0x0000000000000000000000000000000000000000
    {% if is_incremental() %}
    and minter.evt_block_time >= date_trunc('day', now() - interval '7' day)
    {% else %}
    and minter.evt_block_time >= {{ project_start_date }}
    {% endif %}

left join {{ source('erc20_ethereum','evt_transfer') }} minter_superrare on minter_superrare.contract_address = a.contract_address
    and minter_superrare.value = a.nft_token_id
    and minter_superrare."from" = 0x0000000000000000000000000000000000000000
    {% if is_incremental() %}
    and minter_superrare.evt_block_time >= date_trunc('day', now() - interval '7' day)
    {% else %}
    and minter_superrare.evt_block_time >= {{ project_start_date }}
    {% endif %}

