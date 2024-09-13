{% macro attacks_address_poisoning(blockchain, first_funded_by) %}

WITH transfer_recipients AS (
    SELECT to AS address
    , varbinary_substring(to, 1, 3) AS address_start
    , varbinary_substring(to, 18, 3) AS address_end
    , COUNT(*) AS address_occurence
    FROM {{ source('tokens_'~blockchain, 'transfers')}}
    WHERE block_time > NOW() - interval '3' month
    {% if is_incremental() %}
    AND {{ incremental_predicate('block_time') }}
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
    INNER JOIN {{ source('tokens_'~blockchain, 'transfers')}} attack ON attack.to = ma.address_attack
        AND attack.tx_from=attack."from"
        AND attack.block_time > NOW() - interval '3' month
        {% if is_incremental() %}
        AND {{ incremental_predicate('attack.block_time') }}
        {% endif %}
    INNER JOIN {{ source('tokens_'~blockchain, 'transfers')}} normal ON normal.to = ma.address_normal
        AND normal.tx_from=normal."from"
        AND normal.block_time > NOW() - interval '3' month
        {% if is_incremental() %}
        AND {{ incremental_predicate('normal.block_time') }} - interval '14' day
        {% endif %}
        AND attack.block_number > normal.block_number
    INNER JOIN {{ source('tokens_'~blockchain, 'transfers')}} attack_probe ON attack_probe.to = ma.address_attack
        AND attack_probe.block_time > NOW() - interval '1' month
        AND attack_probe.tx_from<>attack_probe."from"
        AND attack_probe.block_time > NOW() - interval '3' month
        AND attack_probe.block_number BETWEEN normal.block_number AND attack.block_number
    GROUP BY 2, 3, 6, 7, 9, 10, 11, 12, 13, 14, 15, 16, 17
    )

, remove_high_funded_counts AS (
    SELECT ma.address_normal AS victim
    FROM matching_addresses ma
    INNER JOIN {{first_funded_by}} ffb ON ffb.first_funded_by=ma.address_normal
    GROUP BY 1
    HAVING COUNT(DISTINCT ffb.address) > 100
    )

SELECT DISTINCT r.blockchain
, r.block_time
, r.block_number
, r.attack_type
, r.attack_category
, r.attacker
, r.victim
, r.intended_recipient
, r.amount_usd
, r.amount
, r.amount_raw
, r.token_address
, r.token_standard
, r.token_symbol
, r.tx_hash
, r.tx_index
, r.evt_index
FROM results r
{#
LEFT JOIN remove_high_funded_counts rhfc ON rhfc.victim=r.victim
    AND rhfc.victim IS NULL
#}

{% endmacro %}