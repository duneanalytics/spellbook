{% macro attacks_address_poisoning(blockchain, token_transfers, cex_addresses) %}

WITH transfered_to_similar_addresses AS (
    SELECT attack.block_time
    , attack.block_number
    , normal.tx_from AS victim
    , normal.amount_usd AS amount_usd
    , normal.amount AS amount
    , normal.amount_raw AS amount_raw
    , attack.token_standard AS token_standard
    , attack.contract_address AS token_address
    , attack.symbol AS token_symbol
    , attack.tx_to AS attacker
    , normal.tx_to AS intended_recipient
    -- Uncomment if to be made a purely crosschain spell to catch crosschain phishing:
    --, attack.blockchain AS phished_blockchain
    --, normal.blockchain AS original_blockchain
    , attack.tx_hash
    , attack.tx_index
    , attack.evt_index
    FROM {{token_transfers}} attack
    INNER JOIN {{token_transfers}} normal ON normal.block_time BETWEEN attack.block_time - interval '1' day AND attack.block_time -- To tweak, ideally 3 days
        AND attack.tx_from=normal.tx_from
        AND attack.tx_to!=normal.tx_to
        AND "LEFT"(CAST(attack.tx_to AS varchar), 4)="LEFT"(CAST(normal.tx_to AS varchar), 4)
        AND "RIGHT"(CAST(attack.tx_to AS varchar), 4)="RIGHT"(CAST(normal.tx_to AS varchar), 4)
        AND normal.amount_raw > 0
    WHERE attack.amount_raw > 0
    {% if is_incremental() %}
    AND {{ incremental_predicate('attack.block_time') }}
    {% endif %}
    )
    
SELECT '{{blockchain}}' AS blockchain
, ttsa.block_time
, ttsa.block_number
, ttsa.victim
, ttsa.amount_usd
, ttsa.amount
, ttsa.amount_raw
, ttsa.token_standard
, ttsa.token_address
, ttsa.token_symbol
, ttsa.original_to_address
, ttsa.attacker
, ttsa.tx_hash
, ttsa.tx_index
, ttsa.evt_index
FROM transfered_to_similar_addresses ttsa
LEFT JOIN {{cex_addresses}} ca ON ca.address=ttsa.victim -- Exclude CEXs
    AND ca.address IS NULL

{% endmacro %}
