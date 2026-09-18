{{
  config(
    schema = 'addresses_events_arc',
    alias = 'first_funded_by',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'append',
    unique_key = ['blockchain', 'address'],
    filtering_columns = []
  )
}}

-- Arc native transfers come from EIP-7708 logs. evt_index orders multiple
-- funding events within one transaction; trace_address is null on this chain.
select
  'arc' as blockchain,
  t.to as address,
  min_by(t."from", (t.block_number, t.tx_index, t.evt_index)) as first_funded_by,
  min(t.block_time) as block_time
from {{ source('tokens_arc', 'base_transfers') }} as t
where t.token_standard = 'native'
  and t.to is not null
{% if is_incremental() -%}
  and {{ incremental_predicate('t.block_time') }}
  and not exists (
    select 1
    from {{ this }} as existing
    where existing.address = t.to
  )
{% endif -%}
group by 2
