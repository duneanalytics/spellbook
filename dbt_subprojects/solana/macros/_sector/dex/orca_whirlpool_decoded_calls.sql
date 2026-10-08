{% macro orca_whirlpool_decoded_calls(legacy_name, current_name, fields, bounded=false, dedupe=true) %}
{#
    Normalize both IDL naming conventions and prefer the current decoder on overlap.
    Set dedupe=false when the caller already collapses duplicate instructions
    (e.g. a downstream DISTINCT or row_number over the instruction key); this
    skips a full-table window sort on the large swap sources.
#}
{% set columns %}
    call_block_time, call_block_date, call_block_slot, call_tx_id, call_tx_index,
    call_outer_instruction_index, call_inner_instruction_index, call_is_inner,
    call_tx_signer, call_outer_executing_account
    {% for legacy, current in fields %}, {{ legacy }}{% endfor %}
{% endset %}
{% set decoded %}
    {% for table, priority in [(legacy_name, 1), (current_name, 0)] %}
    select
        call_block_time, call_block_date, call_block_slot, call_tx_id, call_tx_index,
        call_outer_instruction_index, call_inner_instruction_index, call_is_inner,
        call_tx_signer, call_outer_executing_account
        {% for legacy, current in fields %}
        , {{ current if priority == 0 else legacy }} as {{ legacy }}
        {% endfor %}
        , {{ priority }} as decoder_priority
    from {{ source('whirlpool_solana', table) }}
    {% if bounded %}
    where 1=1
        {% if is_incremental() %}
        and {{ incremental_predicate('call_block_date') }}
        {% else %}
        and call_block_date >= date '2024-06-05'
        {% endif %}
    {% endif %}
    {% if not loop.last %}union all{% endif %}
    {% endfor %}
{% endset %}
{% if dedupe %}
select {{ columns }}
from (
    select *, row_number() over (
        partition by call_block_slot, call_tx_id, call_outer_instruction_index,
            coalesce(call_inner_instruction_index, 0), call_is_inner
        order by decoder_priority
    ) as decoder_rank
    from (
        {{ decoded }}
    ) decoded
) ranked
where decoder_rank = 1
{% else %}
select {{ columns }}
from (
    {{ decoded }}
) decoded
{% endif %}
{% endmacro %}
