{% macro
    angstrom_tx_data(
        angstrom_contract_addr, 
        earliest_block,
        blockchain
    )
%}

{% if blockchain == 'ethereum' %}
-- Read the shared, pre-materialized bundle-transactions staging model instead of re-scanning
-- the raw transactions table. The varbinary_substring(data,1,4) selector filter is non-pushable,
-- so each inlined copy re-scanned the full transactions window: the recursive bundle decoder and
-- nested order macros expand this CTE into ~300 transactions scan operators per build
-- (~85% of the model's physical IO). See angstrom_ethereum_bundle_transactions / CUR2-2709.
SELECT
    block_number,
    block_time,
    tx_hash,
    tx_index,
    angstrom_address,
    tx_data
FROM {{ ref('angstrom_ethereum_bundle_transactions') }}
{% if is_incremental() %}
WHERE {{ incremental_predicate('block_time') }}
{% endif %}

{% else %}
{{ angstrom_tx_data_raw(angstrom_contract_addr, earliest_block, blockchain) }}
{% endif %}


{% endmacro %}
