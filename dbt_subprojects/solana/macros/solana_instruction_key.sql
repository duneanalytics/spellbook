{% macro solana_instruction_key(block_slot, tx_index, outer_idx, inner_idx) %}
(
      cast(coalesce({{ block_slot }}, 0) as decimal(38,0)) * cast(1000000000000000000000 as decimal(38,0))
    + cast(coalesce({{ tx_index }}, 0) as decimal(38,0)) * cast(1000000000000 as decimal(38,0))
    + cast(coalesce({{ outer_idx }}, 0) as decimal(38,0)) * cast(1000000 as decimal(38,0))
    + cast(coalesce({{ inner_idx }}, 0) as decimal(38,0))
)
{% endmacro %}