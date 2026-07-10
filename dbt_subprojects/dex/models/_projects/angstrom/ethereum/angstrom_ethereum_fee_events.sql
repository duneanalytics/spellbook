{{
    config(
        schema = 'angstrom_ethereum',
        alias = 'fee_events',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['block_number', 'pool_id'],
        on_schema_change = 'sync_all_columns',
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

{% set angstrom_contract_addr = '0x0000000aa232009084Bd71A5797d089AA4Edfad4' %}
{% set controller_v1_contract_addr = '0x1746484EA5e11C75e009252c102C8C33e0315fD4' %}
{% set earliest_block = '22971781' %}
{% set blockchain = 'ethereum' %}
{% set controller_pool_configured_log_topic0 = '0xf325a037d71efc98bc41dc5257edefd43a1d1162e206373e53af271a7a3224e9' %}

-- Shared staging model for Angstrom pool fee-configuration (ConfigurePool) events on Ethereum.
-- Materializing this scan once breaks the CTE inlining that previously re-scanned ethereum.logs
-- 18x per angstrom_ethereum_base_trades build: angstrom_pool_info is called in three base_trades
-- lineage branches and the recursive/nested order macros expand its fee_events scan into 18 scan
-- operators, each reading the full ethereum.logs window (~34.9B rows / 105 GiB, ~99% of the model's
-- physical IO) only to find the same ~6 events. The contract_address + topic0 varbinary filter is
-- non-pushable, so nothing prunes. CUR2-2837 (follow-up to CUR2-2709 / PR #9797).
select fee_events.*
from (
    {{
        angstrom_fee_events_raw(
            angstrom_contract_addr = angstrom_contract_addr,
            controller_v1_contract_addr = controller_v1_contract_addr,
            earliest_block = earliest_block,
            blockchain = blockchain,
            controller_pool_configured_log_topic0 = controller_pool_configured_log_topic0
        )
    }}
) as fee_events
{% if is_incremental() %}
where {{ incremental_predicate('block_time') }}
{% endif %}
