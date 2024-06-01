{% macro attacks_address_poisoning(blockchain, token_transfers, first_funded_by) %}

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

, results AS (
        SELECT DISTINCT '{{blockchain}}' AS blockchain
    , block_time
    , block_number
    , 'Address Poisoning' AS attack_type
    , 'Integrity' AS attack_category
    , ma.address_attack AS attacker
    , "from" AS victim
    , MIN_BY(ma.address_normal, normal.block_number) AS intended_recipient
    , amount_usd
    , amount
    , amount_raw
    , contract_address AS token_address
    , token_standard
    , symbol AS token_symbol
    , tx_hash
    , tx_index
    , evt_index
    FROM matching_addresses ma
    INNER JOIN {{token_transfers}} attack ON to = ma.address_attack
        {% if is_incremental() %}
        AND {{ incremental_predicate('block_time') }}
        {% endif %}
    INNER JOIN {{token_transfers}} normal ON normal.to = ma.address_normal
        AND tx_from=normal.tx_from
        {% if is_incremental() %}
        AND {{ incremental_predicate('block_time') }} - interval '14' day
        {% endif %}
        AND block_number > normal.block_number
    INNER JOIN {{token_transfers}} attack_probe ON attack_probe.to = ma.address_attack
        AND attack_probe.tx_from=tx_from
        AND attack_probe.block_number BETWEEN normal.block_number AND block_number
    GROUP BY 2, 3, 6, 7, 9, 10, 11, 12, 13, 14, 15, 16, 17
    )

, remove_high_funded_counts AS (
    SELECT victim
    FROM matching_addresses ma
    INNER JOIN {{first_funded_by}} ffb ON ffb.first_funded_by=ma.victim
    HAVING COUNT(DISTINCT ffb.address) < 1000
    )

SELECT blockchain
, block_time
, block_number
, attack_type
, attack_category
, attacker
, victim
, intended_recipient
, amount_usd
, amount
, amount_raw
, token_address
, token_standard
, token_symbol
, tx_hash
, tx_index
, evt_index
FROM results
INNER JOIN remove_high_funded_counts USING (victim)

{% endmacro %}