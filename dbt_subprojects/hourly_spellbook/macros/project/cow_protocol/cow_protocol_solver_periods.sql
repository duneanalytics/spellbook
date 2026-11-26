{% macro cow_protocol_solver_periods(blockchain) %}
-- one row per allow-list event; an active row is valid from start_block until end_block (exclusive, null = open)
select
    solver,
    is_active,
    evt_block_number as start_block,
    lead(evt_block_number) over (partition by solver order by evt_block_number, evt_index) as end_block
from (
    select solver, evt_block_number, evt_index, true as is_active
    from {{ source('gnosis_protocol_v2_' ~ blockchain, 'GPv2AllowListAuthentication_evt_SolverAdded') }}
    union all
    select solver, evt_block_number, evt_index, false as is_active
    from {{ source('gnosis_protocol_v2_' ~ blockchain, 'GPv2AllowListAuthentication_evt_SolverRemoved') }}
)
{% endmacro %}
