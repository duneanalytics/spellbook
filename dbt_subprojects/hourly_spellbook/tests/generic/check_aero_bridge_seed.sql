{% test check_aero_bridge_seed(model, seed_file, direction) %}

{% set chain_column = 'deposit_chain' if direction == 'deposits' else 'withdrawal_chain' %}
{% set amount_column = 'deposit_amount_raw' if direction == 'deposits' else 'withdrawal_amount_raw' %}

SELECT seed.*
FROM {{ seed_file }} seed
LEFT JOIN {{ model }} actual
    ON actual.{{ chain_column }} = seed.{{ chain_column }}
    AND actual.tx_hash = seed.tx_hash
    AND actual.evt_index = seed.evt_index
WHERE actual.tx_hash IS NULL
    OR actual.deposit_chain IS DISTINCT FROM seed.deposit_chain
    OR actual.withdrawal_chain IS DISTINCT FROM seed.withdrawal_chain
    OR actual.bridge_transfer_id IS DISTINCT FROM seed.bridge_transfer_id
    OR actual.{{ amount_column }} IS DISTINCT FROM seed.amount_raw
    OR actual.recipient IS DISTINCT FROM seed.recipient

{% endtest %}
