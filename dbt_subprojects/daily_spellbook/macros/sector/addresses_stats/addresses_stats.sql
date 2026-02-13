{% macro addresses_stats(blockchain) %}

with

{% if not is_incremental() %}

----- FULL-REFRESH LOGIC -----

first_funded as (
    select
        address,
        first_funded_by,
        block_time as first_funded_at
    from {{ source('addresses_events_' ~ blockchain, 'first_funded_by') }}
),

contract_addresses as (
    select
        address,
        min(block_time) as first_deployment_date,
        true as is_smart_contract
    from {{ source(blockchain, 'creation_traces') }}
    group by 1
),

addresses as (
    select address from first_funded
    union
    select address from contract_addresses
),

final as (
    select
        '{{ blockchain }}' as blockchain,
        a.address,
        f.first_funded_by,
        f.first_funded_at,
        coalesce(c.is_smart_contract, false) as is_smart_contract,
        not coalesce(c.is_smart_contract, false) as is_eoa,
        c.first_deployment_date
    from addresses a
    left join first_funded f
      on f.address = a.address
    left join contract_addresses c
      on c.address = a.address
),

-- Safety: ensure 1 row per address
final_deduped as (
    select
        blockchain,
        address,
        first_funded_by,
        first_funded_at,
        is_smart_contract,
        is_eoa,
        first_deployment_date
    from (
        select
            f.*,
            row_number() over (partition by address) as rn
        from final f
    )
    where rn = 1
)

{% else %}

---- INCREMENTAL LOGIC -----

contract_addresses_recent as (
    select
        address,
        min(block_time) as first_deployment_date,
        true as is_smart_contract
    from {{ source(blockchain, 'creation_traces') }}
    where {{ incremental_predicate('block_time') }}
    group by 1
),

-- allow flipping is_smart_contract false -> true, but ignore already-true rows (avoids redeploy churn)
contract_updates as (
    select
        c.address,
        c.first_deployment_date,
        c.is_smart_contract
    from contract_addresses_recent c
    left join {{ this }} t
      on t.address = c.address
    where
        t.address is null
        or t.is_smart_contract = false
),

-- fill funding for new addresses OR existing addresses missing first_funded_at
funding_updates as (
    select
        f.address,
        f.first_funded_by,
        f.block_time as first_funded_at
    from {{ source('addresses_events_' ~ blockchain, 'first_funded_by') }} f
    left join {{ this }} t
      on t.address = f.address
    where
        t.address is null
        or t.first_funded_at is null
),

recent_addresses as (
    select address from contract_updates
    union
    select address from funding_updates
),

final as (
    select
        '{{ blockchain }}' as blockchain,
        a.address,
        coalesce(t.first_funded_by, f.first_funded_by) as first_funded_by,
        coalesce(t.first_funded_at, f.first_funded_at) as first_funded_at,
        coalesce(t.first_deployment_date, c.first_deployment_date) as first_deployment_date,
        case
            when t.is_smart_contract = true then true
            when c.is_smart_contract = true then true
            else false
        end as is_smart_contract,
        case
            when t.is_smart_contract = true then false
            when c.is_smart_contract = true then false
            else true
        end as is_eoa
    from recent_addresses a
    left join {{ this }} t
      on t.address = a.address
    left join funding_updates f
      on f.address = a.address
    left join contract_updates c
      on c.address = a.address
),

-- Safety: ensure 1 row per (blockchain, address) for MERGE
final_deduped as (
    select
        blockchain,
        address,
        first_funded_by,
        first_funded_at,
        first_deployment_date,
        is_smart_contract,
        is_eoa
    from (
        select
            f.*,
            row_number() over (partition by blockchain, address) as rn
        from final f
    )
    where rn = 1
)

{% endif %}

select * from final_deduped

{% endmacro %}