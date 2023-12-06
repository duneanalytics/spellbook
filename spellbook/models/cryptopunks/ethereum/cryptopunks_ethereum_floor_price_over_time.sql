{{ config(
        
        alias = 'floor_price_over_time',
        unique_key='day',
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                    "project",
                                    "cryptopunks",
                                    \'["cat"]\') }}'
        )
}}

with all_listing_events as (
    select  punk_id
            , event_type
            , case  when event_type = 'Offered' and to is null then 'Public Listing'
                    when event_type = 'Offered' and to is not null then 'Private Listing'
                else 'Listing Withdrawn' end as event_sub_type
            , eth_amount as listed_price
            , to as listing_offered_to
            , evt_block_number
            , evt_index
            , evt_block_time
            , evt_tx_hash
    from {{ ref('cryptopunks_ethereum_punk_offer_events') }}
)
, all_buys as (
    select  nft_token_id as punk_id
            , 'Punk Bought' as event_type
            , 'Punk Bought' as event_sub_type
            , cast(NULL as double) as listed_price
            , cast(NULL as varbinary) as listing_offered_to
            , block_number as evt_block_number
            , sub_tx_trade_id as evt_index
            , block_time as evt_block_time
            , tx_hash as evt_tx_hash
    from {{ ref('cryptopunks_ethereum_base_trades') }}
    where project = 'cryptopunks'
)
, all_transfers as (
    select  punk_id
            , 'Punk Transfer' as event_type
            , 'Punk Transfer' as event_sub_type
            , cast(NULL as double) as listed_price
            , cast(NULL as varbinary) as listing_offered_to
            , evt_block_number
            , evt_index
            , evt_block_time
            , evt_tx_hash
    from {{ ref('cryptopunks_ethereum_punk_transfers') }}
)
, base_data as (
    with all_days as (select col as day from unnest(sequence(date('2017-06-23'), date(now()), interval '1' day)) as _u(col))
    , all_punk_ids as (select cast(col as UINT256) as punk_id from unnest(sequence(0, 9999, 1)) as _u(col))

    select  day
            , punk_id
    from all_days
    full outer join all_punk_ids on true
)
, all_punk_events as (
    select *
          , row_number() over (partition by punk_id order by evt_block_number asc, evt_index asc ) as punk_event_index
    from
    (
    select * from all_listing_events
    union all select * from all_buys
    union all select * from all_transfers
    ) a
)
, aggregated_punk_on_off_data as (
    select date_trunc('day',a.evt_block_time) as day
            , a.punk_id
            , listed_price
            , case when event_sub_type = 'Public Listing' then 'Active' else 'Not Listed' end as listed_bool
    from all_punk_events a
    inner join (    select date_trunc('day', evt_block_time) as day
                            , punk_id
                            , max(punk_event_index) as max_event
                    from all_punk_events
                    group by 1,2
                ) b -- max event per punk per day
    on date_trunc('day',a.evt_block_time) = b.day and a.punk_id = b.punk_id and a.punk_event_index = b.max_event
)

select day
        , floor_price_eth
        , floor_price_eth*1.0*p.price as floor_price_usd
from
(   select day
            , min(price_fill_in) filter (where bool_fill_in = 'Active' and price_fill_in > 0) as floor_price_eth
    from
    (   select c.*
                , last_value(listed_price) over (partition by punk_id order by day asc ) as price_fill_in
                , last_value(listed_bool) over (partition by punk_id order by day asc ) as bool_fill_in
        from
        (   select a.day
                    , a.punk_id
                    , listed_price
                    , listed_bool
            from base_data  a
            left outer join aggregated_punk_on_off_data  b
            on a.day = b.day and a.punk_id = b.punk_id
        ) c
    ) d
    group by 1
) e

left join {{ source('prices', 'usd') }} p on p.minute = date_trunc('minute', e.day)
    and p.contract_address = 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2
    and p.blockchain = 'ethereum'

order by day desc
