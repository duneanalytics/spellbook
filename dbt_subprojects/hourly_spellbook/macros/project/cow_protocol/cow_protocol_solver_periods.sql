{% macro cow_protocol_solver_periods(blockchain) %}
-- Returns one row per period in which an address is on the solver allow list.
-- A period starts at a SolverAdded event and ends (exclusively) at the next event of the same address.
-- end_block is null while the period is open.
select
    solver,
    evt_block_number as start_block,
    next_block_number as end_block
from (
    select
        solver,
        evt_block_number,
        activated,
        lead(evt_block_number) over (partition by solver order by evt_block_number, evt_index) as next_block_number
    from (
        select solver, evt_block_number, evt_index, true as activated
        from {{ source('gnosis_protocol_v2_' ~ blockchain, 'GPv2AllowListAuthentication_evt_SolverAdded') }}
        union all
        select solver, evt_block_number, evt_index, false as activated
        from {{ source('gnosis_protocol_v2_' ~ blockchain, 'GPv2AllowListAuthentication_evt_SolverRemoved') }}
    ) as events
) as ordered_events
where activated
{% endmacro %}
