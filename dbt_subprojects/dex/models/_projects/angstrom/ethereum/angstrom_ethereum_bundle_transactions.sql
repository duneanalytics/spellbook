{{
    config(
        schema = 'angstrom_ethereum',
        alias = 'bundle_transactions',
        materialized = 'incremental',
        partition_by = ['block_month'],
        unique_key = ['block_month', 'tx_hash'],
        on_schema_change = 'sync_all_columns',
        file_format = 'delta',
        incremental_strategy = 'merge',
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

{% set angstrom_contract_addr = '0x0000000aa232009084Bd71A5797d089AA4Edfad4' %}
{% set earliest_block = '22971781' %}
{% set blockchain = 'ethereum' %}

-- Shared staging model for Angstrom bundle transactions on Ethereum.
-- Materializing the bundle-calldata scan once breaks the CTE inlining that previously
-- re-scanned ethereum.transactions ~300x per angstrom_ethereum_base_trades build: the
-- recursive bundle decoder and nested order macros expand the tx_data CTE into ~300 scan
-- operators, and the varbinary_substring(data,1,4) selector filter is non-pushable so each
-- one read the full transactions window (~85% of the model's physical IO). CUR2-2709.
select
    bundle_txs.*,
    cast(date_trunc('month', block_time) as date) as block_month
from (
    {{
        angstrom_tx_data_raw(
            angstrom_contract_addr = angstrom_contract_addr,
            earliest_block = earliest_block,
            blockchain = blockchain
        )
    }}
) as bundle_txs
{% if target.name == 'ci' %}
-- CI-only floor: this state:new model would otherwise full-refresh scan ~1yr of
-- ethereum.transactions calldata and blow the 90-min CI timeout. Renders only under
-- --target ci, so prod (the one-time backfill + incremental runs) is unaffected. CUR2-2709.
where block_time >= current_date - interval '14' day
{% endif %}
