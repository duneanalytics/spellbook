{{ config(
    alias = 'events',
    partition_by = ['block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_time', 'unique_trade_id'],
    post_hook='{{ expose_spells(\'["ethereum"]\',
                                "project",
                                "knownorigin",
                                \'["cat"]\') }}'
    )
}}

with raw_ko_transactions as (
    select  evt_block_time as block_time
            , 'v2' as version
            , _tokenId as token_id
            , _seller as seller
            , _buyer as buyer
            , _price as raw_amount
            , evt_block_number as block_number
            , evt_index
            , evt_tx_hash as tx_hash
            , 'Buy' as trade_category
            , contract_address as project_contract_address
    from {{ source('knownorigin_ethereum','TokenMarketplaceV2_evt_TokenPurchased') }}
        {% if is_incremental() %}
        where evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
    union all
    select  evt_block_time as block_time
            , 'v1' as version
            , _tokenId as token_id
            , _currentOwner as seller
            , _bidder as buyer
            , _amount as raw_amount
            , evt_block_number as block_number
            , evt_index
            , evt_tx_hash as tx_hash
            , 'Offer Accepted' as trade_category
            , contract_address as project_contract_address
    from {{ source('knownorigin_ethereum','TokenMarketplace_evt_BidAccepted') }}
        {% if is_incremental() %}
        where evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
    union all
    select  evt_block_time as block_time
            , 'v2' as version
            , _tokenId as token_id
            , null as seller
            , _buyer as buyer
            , _priceInWei as raw_amount
            , evt_block_number as block_number
            , evt_index
            , evt_tx_hash as tx_hash
            , 'Buy' as trade_category
            , contract_address as project_contract_address
    from {{ source('knownorigin_ethereum','KnownOriginDigitalAssetV2_evt_Purchase') }}
        {% if is_incremental() %}
        where evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
)

select  'ethereum' as blockchain
        , 'known origin' as project
        , version
        , TRY_CAST(date_trunc('day', a.block_time) AS date) as block_date
        , a.block_time
        , a.token_id
        , nft_t.name as collection
        , (raw_amount/1e18) * ep.price as amount_usd
        , 'erc721' as token_standard
        , 'Single Item Trade' as trade_type
        , CAST(1 AS DECIMAL(38,0)) as number_of_items
        , trade_category
        , coalesce(seller,nft.from) as seller
        , coalesce(buyer,nft.to) as buyer
        , case when nft.from = '0x0000000000000000000000000000000000000000' then 'Mint' else 'Trade' end as evt_type
        , raw_amount/1e18 as amount_original
        , CAST(raw_amount AS DECIMAL(38,0)) as amount_raw
        , 'ETH' as currency_symbol
        , '0x0000000000000000000000000000000000000000' as currency_contract
        , nft.contract_address as nft_contract_address
        , project_contract_address
        , agg.name as aggregator_name
        , agg.contract_address as aggregator_address
        , a.block_number
        , a.tx_hash
        , t.from as tx_from
        , t.to as tx_to
        , round((2.5 * (raw_amount) / 100), 7) as platform_fee_amount_raw
        , round((2.5 * (raw_amount/1e18) / 100), 7) as platform_fee_amount
        , round((2.5 * (raw_amount/1e18) * ep.price / 100), 7) as platform_fee_amount_usd
        , cast(2.5 as double) as platform_fee_percentage
        , round((12.5 * (raw_amount) / 100), 7) as royalty_fee_amount_raw
        , round((12.5 * (raw_amount/1e18) / 100), 7) as royalty_fee_amount
        , round((12.5 * (raw_amount/1e18) * ep.price / 100), 7) as royalty_fee_amount_usd
        , cast(12.5 as double) as royalty_fee_percentage
        , cast(null as varchar(5)) as royalty_fee_receive_address
        , 'ETH' as royalty_fee_currency_symbol
        , 'knownorigin' || '-' || a.tx_hash || '-' || a.token_id || '-' ||  seller || '-' || a.block_number || '-' || a.evt_index as unique_trade_id
from raw_ko_transactions a
inner join {{ source('ethereum','transactions') }} t
on a.block_time = t.block_time and a.tx_hash = t.hash
    {% if is_incremental() %}
    and t.block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
inner join {{ source('erc721_ethereum','evt_transfer') }} nft
on a.block_time = nft.evt_block_time and a.tx_hash = nft.evt_tx_hash
    {% if is_incremental() %}
    and nft.evt_block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
left join {{ source('prices','usd') }} ep
on ep.blockchain = 'ethereum' and ep.contract_address = '0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2' --WETH
    and ep.minute = date_trunc('minute', a.block_time)
    {% if is_incremental() %}
    and ep.minute >= date_trunc("day", now() - interval '1 week')
    {% endif %}
left join {{ ref('nft_ethereum_aggregators') }} agg ON t.to=agg.contract_address
left join  {{ ref('tokens_ethereum_nft') }} nft_t ON nft.contract_address=nft_t.contract_address
