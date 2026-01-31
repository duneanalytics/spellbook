{{ config(
    schema = 'sui',
    alias = 'assets_balance_changes',
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = ['checkpoint','transaction_digest','owner_address','coin_type']
) }}

with txo as (
    select
        checkpoint,
        transaction_digest,
        object_id,
        input_kind
    from sui.transaction_objects
    {% if is_incremental() %}
      where checkpoint >= (
        select coalesce(max(checkpoint), 0) - 2000
        from {{ this }}
      )
    {% endif %}
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
    {% if is_incremental() %}
      and checkpoint >= (
        select coalesce(max(checkpoint), 0) - 2000
        from {{ this }}
      )
    {% endif %}
),

deltas as (
    select
        txo.checkpoint,
        txo.transaction_digest,
        obj.owner_address,
        obj.coin_type,
        sum(
            case
                when txo.input_kind is not null then -obj.coin_balance
                else obj.coin_balance
            end
        ) as amount
    from txo
    join obj
      on txo.object_id = obj.object_id
     and txo.checkpoint = obj.checkpoint
    group by 1,2,3,4
)

select *
from deltas
where amount <> 0;

