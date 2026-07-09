{{ config(

    schema = 'staking_ethereum',
    alias = 'deposits',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    unique_key = ['tx_hash', 'evt_index'],
    post_hook='{{ expose_spells(\'["ethereum"]\',
                                "sector",
                                "staking",
                                \'["hildobby"]\') }}')
}}

WITH deposit_events AS (
    SELECT d.evt_block_time AS block_time
    , d.evt_block_number AS block_number
    , d.evt_index
    , d.evt_tx_hash AS tx_hash
    , bytearray_to_bigint(reverse(d.amount))/1e9 AS amount
    , d.contract_address
    , bytearray_to_bigint(reverse(d.index)) AS deposit_index
    , d.pubkey
    , d.signature
    , bytearray_substring(d.withdrawal_credentials, 1, 1) AS withdrawal_credentials_type
    , CASE WHEN bytearray_substring(d.withdrawal_credentials, 1, 1) IN (0x01, 0x02) THEN bytearray_substring(d.withdrawal_credentials, 13)
        ELSE NULL
        END AS withdrawal_address
    , d.withdrawal_credentials
    , ROW_NUMBER() OVER (PARTITION BY d.evt_block_number, d.evt_tx_hash, from_big_endian_64(reverse(d.amount)) ORDER BY d.evt_index) AS table_merging_deposits_id
    FROM {{ source('eth2_ethereum', 'DepositContract_evt_DepositEvent') }} d
    {% if not is_incremental() %}
    WHERE d.evt_block_time >= TIMESTAMP '2020-10-13' -- SHOULD BE 2020-10-14 BUT CHANGED TO 2020-10-13 TO TRIGGER TABLE RERUN
    {% endif %}
    {% if is_incremental() %}
    WHERE  {{ incremental_predicate('d.evt_block_time') }}
    {% endif %}
    )

, traces AS (
    SELECT t.block_number
    , t.tx_hash AS tx_hash
    , t.value / POWER(10, 18) AS amount
    , t."from" AS depositor_address
    , ROW_NUMBER() OVER (PARTITION BY t.block_number, t.tx_hash, t.value ORDER BY t.trace_address) AS table_merging_traces_id
    FROM {{ source('ethereum', 'traces') }} t
    WHERE t.to = 0x00000000219ab540356cbb839cbe05303d7705fa
    AND (call_type NOT IN ('delegatecall', 'callcode', 'staticcall') OR call_type IS NULL)
    AND t.value > UINT256 '0'
    AND success
    {% if not is_incremental() %}
    AND t.block_time >= TIMESTAMP '2020-10-13' -- SHOULD BE 2020-10-14 BUT CHANGED TO 2020-10-13 TO TRIGGER TABLE RERUN
    {% endif %}
    {% if is_incremental() %}
    AND  {{ incremental_predicate('t.block_time') }}
    {% endif %}
    )

, base AS (
    SELECT d.block_time
    , d.block_number
    , d.amount AS amount_staked
    , ett.depositor_address
    , et."from" AS tx_from
    , d.tx_hash
    , d.evt_index
    , d.deposit_index
    , d.pubkey
    , d.signature
    , d.withdrawal_address
    , d.withdrawal_credentials_type
    , d.withdrawal_credentials
    FROM deposit_events d
    INNER JOIN {{ source('ethereum', 'transactions') }} et ON et.block_number=d.block_number
        AND et.hash=d.tx_hash
        {% if not is_incremental() %}
        AND et.block_time >= TIMESTAMP '2020-10-13' -- SHOULD BE 2020-10-14 BUT CHANGED TO 2020-10-13 TO TRIGGER TABLE RERUN
        {% endif %}
        {% if is_incremental() %}
        AND  {{ incremental_predicate('et.block_time') }}
        {% endif %}
    INNER JOIN traces ett ON ett.block_number=d.block_number
        AND ett.tx_hash=d.tx_hash
        AND ett.amount=d.amount
        AND ett.table_merging_traces_id=d.table_merging_deposits_id
    )

-- A deposit can match more than one row in staking_ethereum_entities when its
-- depositor_address, tx_from, pubkey, and withdrawal_credentials each identify a
-- different entity (e.g. an operator depositing on behalf of a CEX). Rank matches so
-- exactly one entity is chosen deterministically: withdrawal_credentials (beneficial
-- owner) takes precedence over depositor_address/tx_from (operator), which in turn
-- takes precedence over pubkey.
, entity_matches AS (
    SELECT b.tx_hash
    , b.evt_index
    , ete.entity
    , ete.entity_unique_name
    , ete.category AS entity_category
    , ROW_NUMBER() OVER (
        PARTITION BY b.tx_hash, b.evt_index
        ORDER BY CASE
            WHEN ete.withdrawal_credentials IS NOT NULL THEN 1
            WHEN ete.depositor_address IS NOT NULL THEN 2
            WHEN ete.tx_from IS NOT NULL THEN 3
            WHEN ete.pubkey IS NOT NULL THEN 4
            ELSE 5
            END
        ) AS entity_rank
    FROM base b
    LEFT JOIN {{ ref('staking_ethereum_entities')}} ete
        ON ((ete.depositor_address IS NOT NULL AND b.depositor_address=ete.depositor_address)
        OR (ete.tx_from IS NOT NULL AND b.tx_from=ete.tx_from)
        OR (ete.pubkey IS NOT NULL AND b.pubkey=ete.pubkey)
        OR (ete.withdrawal_credentials IS NOT NULL AND b.withdrawal_credentials=ete.withdrawal_credentials))
        AND ete.entity IS NOT NULL
    )

, sub_entity_matches AS (
    SELECT b.tx_hash
    , b.evt_index
    , etes.sub_entity
    , etes.sub_entity_unique_name
    , etes.sub_entity_category
    , ROW_NUMBER() OVER (
        PARTITION BY b.tx_hash, b.evt_index
        ORDER BY CASE
            WHEN etes.withdrawal_credentials IS NOT NULL THEN 1
            WHEN etes.depositor_address IS NOT NULL THEN 2
            WHEN etes.tx_from IS NOT NULL THEN 3
            WHEN etes.pubkey IS NOT NULL THEN 4
            ELSE 5
            END
        ) AS entity_rank
    FROM base b
    LEFT JOIN {{ ref('staking_ethereum_entities')}} etes
        ON ((etes.depositor_address IS NOT NULL AND b.depositor_address=etes.depositor_address)
        OR (etes.tx_from IS NOT NULL AND b.tx_from=etes.tx_from)
        OR (etes.pubkey IS NOT NULL AND b.pubkey=etes.pubkey)
        OR (etes.withdrawal_credentials IS NOT NULL AND b.withdrawal_credentials=etes.withdrawal_credentials))
        AND etes.sub_entity IS NOT NULL
    )

SELECT b.block_time
, b.block_number
, b.amount_staked
, b.depositor_address
, em.entity
, em.entity_unique_name
, em.entity_category
, sem.sub_entity
, sem.sub_entity_unique_name
, sem.sub_entity_category
, b.tx_hash
, b.tx_from
, b.deposit_index
, b.pubkey
, b.signature
, b.withdrawal_address
, b.withdrawal_credentials_type
, b.withdrawal_credentials
, b.evt_index
FROM base b
LEFT JOIN entity_matches em ON em.tx_hash=b.tx_hash AND em.evt_index=b.evt_index AND em.entity_rank=1
LEFT JOIN sub_entity_matches sem ON sem.tx_hash=b.tx_hash AND sem.evt_index=b.evt_index AND sem.entity_rank=1
