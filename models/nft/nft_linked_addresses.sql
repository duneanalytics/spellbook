{{ config(
    alias = 'linked_addresses',
    partition_by = ['blockchain'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['blockchain', 'linked_address_id'],
    post_hook='{{ expose_spells(\'["ethereum","solana"]\',
                                "sector",
                                "nft",
                                \'["springzh"]\') }}'
    )
}}

with nft_trade_address as (
    select distinct blockchain, buyer as address_a, seller as address_b
    from {{ ref('nft_trades') }}
    where buyer is not null
        and seller is not null
        and blockchain is not null
    {% if is_incremental() %}
    and block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
    union all

    select distinct blockchain, seller as address_a, buyer as address_b
    from {{ ref('nft_trades') }}
    where buyer is not null
        and seller is not null
        and blockchain is not null
    {% if is_incremental() %}
    and block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
),

linked_address_nft_trade as (
    select blockchain,
        address_a,
        address_b,
        count(*) as cnt
    from nft_trade_address
    group by 1, 2, 3
    having count(*) > 1
),

linked_address_sorted as (
    -- Normalize linked addresses to master address
    select blockchain,
        (case when address_a > address_b then address_b else address_a end) as master_address,
        address_a as alternative_address
    from linked_address_nft_trade
    union
    select blockchain,
        (case when address_a > address_b then address_b else address_a end) as master_address,
        address_b as alternative_address
    from linked_address_nft_trade
),

linked_address_sorted_row_num as (
    select blockchain, master_address, alternative_address,
        master_address || '-' || alternative_address as linked_address_id,
        row_number() over (partition by blockchain, alternative_address order by master_address) as row_num
    from linked_address_sorted
)

select blockchain,
    master_address,
    alternative_address,
    linked_address_id
from linked_address_sorted_row_num
where row_num = 1