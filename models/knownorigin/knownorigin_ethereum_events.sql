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
    select  a.evt_block_time
            , b.contract_address
            , _tokenId as token_id
            , _seller as seller 
            , _buyer as buyer
            , _price as raw_amount
            , a.evt_block_number
            , a.evt_index
            , a.evt_tx_hash
            , case when b.from = '0x0000000000000000000000000000000000000000' then 'Primary' else 'Secondary' end as sale_type
            , 'Buy' as trade_category
            , lower('0xc322cdd03f34b6d25633c2abbc8716a058c7fe9e') as project_contract_address
    from {{ source('knownorigin_ethereum','TokenMarketplaceV2_evt_TokenPurchased') }} a
    inner join {{ source('erc721_ethereum','evt_transfer') }} b on a.evt_tx_hash = b.evt_tx_hash and a._tokenId = b.tokenId
    {% if is_incremental() %}
    where a.evt_block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}

    union all 
    
    select a.evt_block_time
            , b.contract_address
            , _tokenId as token_id
            , _currentOwner as seller
            , _bidder as buyer
            , _amount as raw_amount
            , a.evt_block_number
            , a.evt_index
            , a.evt_tx_hash
            , case when b.from = '0x0000000000000000000000000000000000000000' then 'Primary' else 'Secondary' end as sale_type
            , 'Offer Accepted' as trade_category
            , lower('0xc1697d340807324200e26e4617ce9c0070488e23') as project_contract_address
    from {{ source('knownorigin_ethereum','TokenMarketplace_evt_BidAccepted') }} a
    inner join {{ source('erc721_ethereum','evt_transfer') }} b on a.evt_tx_hash = b.evt_tx_hash and a._tokenId = b.tokenId
    {% if is_incremental() %}
    where a.evt_block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}

    union all 
    
    select a.evt_block_time
            , a.contract_address
            , _tokenId as token_id
            , b.from as seller
            , _buyer as buyer
            , _priceInWei as raw_amount
            , a.evt_block_number
            , a.evt_index
            , a.evt_tx_hash
            , case when b.from = '0x0000000000000000000000000000000000000000' then 'Primary' else 'Secondary' end as sale_type
            , 'Buy' as trade_category
            , lower('0xfbeef911dc5821886e1dda71586d90ed28174b7d') as project_contract_address    
    from {{ source('knownorigin_ethereum','KnownOriginDigitalAssetV2_evt_Purchase') }} a
    inner join {{ source('erc721_ethereum','evt_transfer') }} b on a.evt_tx_hash = b.evt_tx_hash and a._tokenId = b.tokenId
    {% if is_incremental() %}
    where a.evt_block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}

)

select  'ethereum' as blockchain
        , 'known origin' as project
        , 'v1' as `version`
        , cast(date_trunc('day', a.evt_block_time) AS date) as block_date
        , a.evt_block_time as block_time
        , a.token_id
        , 'Known Origin' as collection
        , (raw_amount/1e18) * ep.price as amount_usd
        , 'erc721' as token_standard
        , 'Single Item Trade' as trade_type 
        , 1 as number_of_items
        , trade_category 
        , seller 
        , buyer 
        , 'Trade' as evt_type
        , raw_amount/1e18 as amount_original
        , raw_amount as amount_raw
        , 'ETH' as currency_symbol
        , '0x0000000000000000000000000000000000000000' as currency_contract
        , contract_address as nft_contract_address
        , project_contract_address
        , cast(null as varchar(5)) as aggregator_name
        , cast(null as varchar(5)) as aggregator_address
        , a.evt_block_number as block_number
        , a.evt_tx_hash as tx_hash
        , t.from as tx_from
        , t.to as tx_to
        , round((2.5 * (raw_amount) / 100), 7) as platform_fee_amount_raw
        , round((2.5 * (raw_amount/1e18) / 100), 7) as platform_fee_amount
        , round((2.5 * (raw_amount/1e18) * ep.price / 100), 7) as platform_fee_amount_usd
        , cast('2.5' as double) as platform_fee_percentage
        , round((12.5 * (raw_amount) / 100), 7) as royalty_fee_amount_raw
        , round((12.5 * (raw_amount/1e18) / 100), 7) as royalty_fee_amount
        , round((12.5 * (raw_amount/1e18) * ep.price / 100), 7) as royalty_fee_amount_usd
        , cast('12.5' as double) as royalty_fee_percentage
        , cast(null as varchar(5)) as royalty_fee_receive_address
        , 'ETH' as royalty_fee_currency_symbol
        , 'knownorigin' || '-' || a.evt_tx_hash || '-' || a.token_id || '-' ||  seller || '-' || a.evt_block_number || '-' || a.evt_index as unique_trade_id
from (select * from raw_ko_transactions where sale_type = 'Secondary') a
inner join {{ source('ethereum','transactions') }} t on a.evt_tx_hash = t.hash
left outer join
    (   select  minute
                , price
        from {{ source('prices','usd') }}
        where blockchain = 'ethereum'
            and symbol = 'WETH' 
            {% if is_incremental() %}
            and minute >= date_trunc("day", now() - interval '1 week')
            {% endif %}
    ) ep on date_trunc('minute', a.evt_block_time) = ep.minute