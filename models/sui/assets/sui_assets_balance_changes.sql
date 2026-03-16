{{ 
  config(
    schema = 'sui',
    alias = 'assets_balance_changes',
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = ['checkpoint', 'transaction_digest', 'owner_address', 'coin_type']
  ) 
}}

-- ----------------------------------------------------
-- Determine incremental cutoff safely
-- ----------------------------------------------------
with cutoff as (
    {% if is_incremental() %}
        select max(checkpoint) - 2000 as min_checkpoint
        from {{ this }}
    {% else %}
        select 0 as min_checkpoint
    {% endif %}
),

-- ----------------------------------------------------
-- Transaction objects touched in recent checkpoints
-- ----------------------------------------------------
txo as (
    select
        checkpoint,
        transaction_digest,
        object_id,
        owner_address,
        input_kind,
        case 
            when input_kind is not null then 'in'
            else 'out'
        end as io_flag
    from {{ ref('sui_transaction_objects') }}
    where checkpoint >= (select min_checkpoint from cutoff)
),

-- ----------------------------------------------------
-- Object versions (coin objects only)
-- ----------------------------------------------------
obj_versions as (
    select
        object_id,
        owner_address,
        coin_type,
        cast(coin_balance as decimal(38,0)) as coin_balance,
        checkpoint,
        version
    from {{ ref('sui_objects') }}
    where coin_type is not null
),

-- ----------------------------------------------------
-- Join tx objects to correct object snapshot
-- ----------------------------------------------------
joined as (
    select
        txo.checkpoint,
        txo.transaction_digest,
        txo.owner_address,
        obj.coin_type,

        case
            -- Outputs: same-checkpoint snapshot
            when txo.io_flag = 'out'
                then obj.coin_balance

            -- Inputs: latest snapshot at or before tx checkpoint
            when txo.io_flag = 'in'
                then -obj.coin_balance
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
            (txo.io_flag = 'out' and obj.checkpoint = txo.checkpoint)
         or (txo.io_flag = 'in'  and obj.checkpoint <= txo.checkpoint)
       )
)

-- ----------------------------------------------------
-- Final aggregation
-- ----------------------------------------------------
select
    checkpoint,
    transaction_digest,
    owner_address,
    coin_type,
    sum(amount) as amount
from joined
where rn = 1
group by
    checkpoint,
    transaction_digest,
    owner_address,
    coin_type

