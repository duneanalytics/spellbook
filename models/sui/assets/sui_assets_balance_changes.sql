{{ config(
    schema = 'sui',
    alias = 'assets_balance_changes',
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = ['checkpoint', 'transaction_digest', 'owner_address', 'coin_type']
) }}

with cutoff as (
    select
        {% if is_incremental() %}
            coalesce(max(checkpoint), 0) - 2000
        {% else %}
            0
        {% endif %} as min_checkpoint
    from {{ this }}
),

-- transactions touching coin objects
txo as (
    select
        checkpoint,
        transaction_digest,
        object_id,
        owner_address,
        coin_type,
        version,
        case
            when input_kind is null then 'out'
            else 'in'
        end as io_flag
    from {{ ref('sui__transaction_objects') }}
    where checkpoint >= (select min_checkpoint from cutoff)
),

-- object snapshots (multiple versions per object)
obj_versions as (
    select
        object_id,
        owner_address,
        coin_type,
        coin_balance,
        checkpoint,
        version
    from {{ ref('sui__objects') }}
),

-- match each tx object to the correct object snapshot
matched as (
    select
        txo.checkpoint,
        txo.transaction_digest,
        obj.owner_address,
        obj.coin_type,
        case
            when txo.io_flag = 'out' then obj.coin_balance
            else -obj.coin_balance
        end as amount,
        row_number() over (
            partition by
                txo.transaction_digest,
                txo.object_id,
                txo.io_flag
            order by
                obj.checkpoint desc,
                obj.version desc
        ) as rn
    from txo
    join obj_versions obj
        on txo.object_id = obj.object_id
        and (
            -- outputs: exact snapshot at tx checkpoint
            (txo.io_flag = 'out' and obj.checkpoint = txo.checkpoint)
            or
            -- inputs: latest snapshot at or before tx checkpoint
            (txo.io_flag = 'in' and obj.checkpoint <= txo.checkpoint)
        )
)

select
    checkpoint,
    transaction_digest,
    owner_address,
    coin_type,
    sum(amount) as amount
from matched
where rn = 1
group by
    checkpoint,
    transaction_digest,
    owner_address,
    coin_type

