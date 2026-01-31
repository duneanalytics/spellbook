{{ config(
    schema = 'sui',
    alias = 'assets_balance_changes',
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = ['checkpoint','transaction_digest','owner_address','coin_type']
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

txo as (
    select
        checkpoint,
        transaction_digest,
        object_id,
        input_kind
    from sui.transaction_objects
    where checkpoint >= (select min_checkpoint from cutoff)
),

obj as (
    select
        checkpoint,
        object_id,
        owner_address,
        coin_type,
        cast(coin_balance as decimal(38,0)) as coin_balance
    from sui.objects
    where coin_type is not null
      and checkpoint >= (select min_checkpoint from cutoff)
),

-- Join rule:
-- - outputs (input_kind is null): join to obj at same checkpoint
-- - inputs  (input_kind is not null): join to latest obj checkpoint < tx checkpoint
txo_with_obj as (
    select
        txo.checkpoint as tx_checkpoint,
        txo.transaction_digest,
        txo.object_id,
        txo.input_kind,

        obj.owner_address,
        obj.coin_type,
        obj.coin_balance,
        obj.checkpoint as obj_checkpoint,

        row_number() over (
            partition by txo.transaction_digest, txo.object_id
            order by
                case
                    when txo.input_kind is null then obj.checkpoint            -- outputs: same checkpoint (only one)
                    else obj.checkpoint                                       -- inputs: choose latest before tx
                end desc
        ) as rn
    from txo
    join obj
      on txo.object_id = obj.object_id
     and (
          (txo.input_kind is null and obj.checkpoint = txo.checkpoint)
       or (txo.input_kind is not null and obj.checkpoint < txo.checkpoint)
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
                when input_kind is not null then -coin_balance
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


