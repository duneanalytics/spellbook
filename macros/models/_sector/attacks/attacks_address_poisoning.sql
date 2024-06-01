{% macro attacks_address_poisoning(blockchain, token_transfers) %}

WITH transfer_recipients AS (
    SELECT to AS address
    , varbinary_substring(to, 1, 3) AS address_start
    , varbinary_substring(to, 18, 3) AS address_end
    , COUNT(*) AS address_occurence
    FROM {{token_transfers}}
    {% if is_incremental() %}
    WHERE {{ incremental_predicate('block_time') }}
    {% endif %}
    GROUP BY 1, 2, 3
    )

, matching_addresses AS (
    SELECT CASE WHEN tr1.address_occurence < tr2.address_occurence THEN tr1.address ELSE tr2.address END AS address_attack
    , CASE WHEN tr1.address_occurence < tr2.address_occurence THEN tr2.address ELSE tr1.address END AS address_normal
    FROM transfer_recipients tr1
    INNER JOIN transfer_recipients tr2 ON tr1.address_start=tr2.address_start
        AND tr1.address_end=tr2.address_end
        AND tr1.address!=tr2.address
    GROUP BY 1, 2
    )

SELECT DISTINCT '{{blockchain}}' AS blockchain
, attack.block_time
, attack.block_number
, 'Address Poisoning' AS attack_type
, 'Integrity' AS attack_category
, ma.address_attack AS attacker
, attack."from" AS victim
, MIN_BY(ma.address_normal, normal.block_number) AS intended_recipient
, attack.amount_usd
, attack.amount
, attack.amount_raw
, attack.contract_address AS token_address
, attack.token_standard
, attack.symbol AS token_symbol
, attack.tx_hash
, attack.tx_index
, attack.evt_index
FROM matching_addresses ma
INNER JOIN {{token_transfers}} attack ON attack.to = ma.address_attack
    {% if is_incremental() %}
    WHERE {{ incremental_predicate('attack.block_time') }}
    {% endif %}
INNER JOIN {{token_transfers}} normal ON normal.to = ma.address_normal
    AND attack.tx_from=normal.tx_from
    {% if is_incremental() %}
    WHERE {{ incremental_predicate('attack.block_time') }} - interval '14' day
    {% endif %}
    AND attack.block_number > normal.block_number
INNER JOIN {{token_transfers}} attack_probe ON attack_probe.to = ma.address_attack
    AND attack_probe.tx_from=attack.tx_from
    AND attack_probe.block_number BETWEEN normal.block_number AND attack.block_number
GROUP BY 2, 3, 6, 7, 9, 10, 11, 12, 13, 14, 15, 16, 17
{% endmacro %}
