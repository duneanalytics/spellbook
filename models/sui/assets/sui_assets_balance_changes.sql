{{ config(
    schema = 'sui',
    alias = 'assets_balance_changes',
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = ['checkpoint','transaction_digest','owner_address','coin_type']
) }}

with bounds as (
    select
        {% if is_incremental() %}
          coalesce(max(checkpoint), 0) - 2000
        {% else %}
          0
        {% endif %} as min_checkpoint
    from {{ this }}
),

txo as (
    select
        checkpoint,
        transaction_digest,
        object_id,
        version,
        input_kind,
        case when input_kind is null then 'out' else 'in' end as io_flag
    from sui.transaction_objects
    where checkpoint >= (select min_checkpoint from bounds)
),

-- IMPORTANT: objects must include older checkpoints so inputs can find a pre-tx snapshot
obj as (
    select
        checkpoint,
        object_id,
        version,
        owner_address,
        coin_type,
        cast(coin_balance as decimal(38,0)) as coin_balance
    from sui.objects
    where coin_type is not null
      and checkpoint >= (select greatest((select min_checkpoint from bounds) - 20000, 0))
),

txo_with_obj as (
    select
        txo.checkpoint as tx_checkpoint,
        txo.transaction_digest,
        txo.object_id,
        txo.version as txo_version,
        txo.input_kind,
        txo.io_flag,

        obj.owner_address,
        obj.coin_type,
        obj.coin_balance,
        obj.checkpoint as obj_checkpoint,
        obj.version as obj_version,

        row_number() over (
            partition by txo.transaction_digest, txo.object_id, txo.io_flag
            order by obj.checkpoint desc, obj.version desc
        ) as rn
    from txo
    join obj
      on txo.object_id = obj.object_id
     and (
          -- outputs: same checkpoint
          (txo.io_flag = 'out' and obj.checkpoint = txo.checkpoint)
          -- inputs: latest snapshot at or before tx checkpoint (handles same-checkpoint create+spend)
       or (txo.io_flag = 'in' and obj.checkpoint <= txo.checkpoint)
     )
),

deltas as (
    select
        tx_checkpoint as checkpoint,
        transaction_digest,
        owner_address,
        coin_type,
        sum(
            case
                when io_flag = 'in' then -coin_balance
                else coin_balance
            end
        ) as amount
    from txo_with_obj
    where rn = 1
    group by 1,2,3,4
)

select *
from deltas
where amount <> 0;




