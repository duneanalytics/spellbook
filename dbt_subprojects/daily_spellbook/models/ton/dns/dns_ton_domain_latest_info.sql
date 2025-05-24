{{ config(
       schema = 'dns_ton'
       , alias = 'domain_latest_info'
       , materialized = 'table'
       , post_hook='{{ expose_spells(\'["ton"]\',
                                   "project",
                                   "dns_ton",
                                   \'["markysha", "pshuvalov"]\') }}'
   )
 }}

with 
_config as (
    select
        '0:B774D95EB20543F186C06B371AB88AD704F7E256130CAF96189368A7D0CB6CCF' as collection_address
),
_dns_events as (
    select
        *
    from
        {{ source('ton', 'nft_events') }}
    where
        collection_address = (select collection_address from _config)
),
_dns_mints as (
    select
        nft_item_address,
        (cast(json_extract(content_onchain, '$.domain') as varchar) || '.ton') as domain,
        prev_owner as owner
    from
        _dns_events
    where
        "type" = 'mint'
),
_dns_last_owner as (
    select
        nft_item_address,
        owner
    from (
        select 
            nft_item_address,
            owner_address as owner,
            row_number() over (partition by nft_item_address order by timestamp desc, lt desc) as rnk
        from
            _dns_events
        --where
        --    "type" = 'sale' or "type" = 'transfer'
    )
    where
        rnk = 1
),
_dns_last_owner_type as (
    select
        nft_item_address,
        owner,
        "type"
    from (
        select 
            _dns_events.nft_item_address,
            _dns_events.owner_address as owner,
            "type",
            row_number() over (partition by _dns_events.nft_item_address order by timestamp desc, lt desc) as rnk
        from
            _dns_events
        join
            _dns_last_owner on _dns_events.nft_item_address = _dns_last_owner.nft_item_address and _dns_events.owner_address = _dns_last_owner.owner
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
            row_number() over (partition by dns_nft_item_address order by tx_lt desc) as rnk
        FROM
            {{ ref('dns_ton_delegation_updates') }}
    )
    where 
        rnk = 1
)
select
    domain,
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
    _dns_last_owner_type movs on mints.nft_item_address = movs.nft_item_address
left join 
    _latest_delegations delegations on delegations.nft_item_address = mints.nft_item_address
    