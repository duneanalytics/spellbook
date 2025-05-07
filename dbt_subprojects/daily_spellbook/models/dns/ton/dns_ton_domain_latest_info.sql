{{ config(
       schema = 'dns_ton'
       , alias = 'domain_latest_info'
       , materialized = 'table'
       , post_hook='{{ expose_spells(\'["ton"]\',
                                   "project",
                                   "dns_ton",
                                   \'["markysha"]\') }}'
   )
 }}

with 
_config as (
    select
        upper(ton_address_user_friendly_to_raw('EQC3dNlesgVD8YbAazcauIrXBPfiVhMMr5YYk2in0Mtsz0Bz')) as collection_address
),
_dns_events as (
    select
        *
    from
        ton.nft_events
    where
        collection_address = (select collection_address from _config)
),
_dns_mints as (
    select
        nft_item_address,
        prev_owner as owner
    from
        _dns_events
    where
        "type" = 'mint'
),
_latest_metadata as (
    select
        address,
        max_by(name, update_time_onchain) as name,
        max_by(content_onchain, update_time_onchain) as content_onchain,
        max_by(description, update_time_onchain) as description,
        max_by(image, update_time_onchain) as image
    from
        ton.nft_metadata NM
    group by
        1
),
_dns_movements as (
    select
        nft_item_address,
        owner,
        "type"
    from (
        select 
            nft_item_address,
            case
                when "type" = 'sale' then owner_address 
                when "type" = 'transfer' then owner_address
            end as owner,
            "type",
            rank() over (partition by nft_item_address order by timestamp desc, lt desc) as rnk
        from
            _dns_events
        where
            "type" = 'sale' or "type" = 'transfer'
    )
    where
        rnk = 1
),
_latest_delegations as (
    select 
        dns_nft_item_address as nft_item_address,
        delegation_initiator,
        delegated_to_wallet
    FROM (
        select 
            *,
            rank() over (partition by dns_nft_item_address order by block_time desc) as rnk
        FROM
            {{ ref('dns_ton_wallet_delegation_updates') }}
    )
    where 
        rnk = 1
)
select
    (cast(json_extract(metadata.content_onchain, '$.domain') as varchar) || '.ton') as domain,
    mints.nft_item_address as dns_nft_item_address,
    case
        when movs.nft_item_address is null then mints.owner
        else movs.owner
    end as dns_nft_item_owner,
    case
        when movs.nft_item_address is null then 'mint'
        else movs.type
    end as own_type,
    mints.owner as dns_nft_item_minter,
    delegations.delegated_to_wallet as delegated_to_wallet,
    delegations.delegation_initiator as delegation_initiator
from
    _dns_mints mints
left join
    _dns_movements movs on mints.nft_item_address = movs.nft_item_address
join
    _latest_metadata metadata on mints.nft_item_address = metadata.address
left join 
    _latest_delegations delegations on delegations.nft_item_address = mints.nft_item_address
    