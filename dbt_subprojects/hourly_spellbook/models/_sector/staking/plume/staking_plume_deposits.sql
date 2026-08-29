{{ config(

    schema = 'staking_plume',
    alias = 'deposits',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    unique_key = ['tx_hash', 'evt_index'],
    post_hook = '{{ expose_spells(\'["plume"]\',
                                "sector",
                                "staking",
                                \'["lukasrozado"]\') }}')
}}

/*
    Plume staking deposits.

    Fully derived from plume.logs (contract is not yet ABI-decoded on Dune, so
    indexed/non-indexed params are decoded manually by topic position -- see
    docs/general/best_practices.md and the precedent in
    dbt_subprojects/daily_spellbook/models/toroperp/sei/toroperp_sei_deposits.sql
    and .../chainlink/*/chainlink_*_ccip_nop_paid_logs.sql for the same pattern).

    Staking contract: 0x30c791E4654EdAc575FA1700eD8633CB2FEDE871
    Stake event topic0: 0x521d5961e1d8e7e104af28f00e1f7e11655e7cc7e8d7a9b7a07e959a1598e215

    Decoded fields:
    - topic1 (indexed): depositor address (right-aligned in the 32-byte topic,
      hence bytearray_substring(topic1, 13) to take the trailing 20 bytes)
    - topic2 (indexed): validator id (read as a full-width uint256 topic --
      no substring needed, matching the pattern used for uint topics elsewhere
      in this repo, e.g. varbinary_to_uint256(topic3) in
      toroperp_sei_deposits.sql)
    - data (non-indexed): amount staked (first 32-byte word)

    Verified on Dune (2026-08-29) against plume.logs for this contract_address +
    topic0: 2,108,504 events since 2025-05-31, 138,784 distinct topic1 values
    (consistent with a per-depositor address) and 12 distinct topic2 values
    (consistent with a small, fixed validator set) -- confirms the topic0 and
    the topic1/topic2 field mapping match the issue's description.
*/

{% set blockchain = 'plume' %}
{% set staking_contract = '0x30c791E4654EdAc575FA1700eD8633CB2FEDE871' %}
{% set stake_evt_topic0 = '0x521d5961e1d8e7e104af28f00e1f7e11655e7cc7e8d7a9b7a07e959a1598e215' %}
{% set project_start_date = '2025-05-31' %} -- verified via Dune query against plume.logs (first_seen)

SELECT
    '{{ blockchain }}' AS blockchain
    , l.block_time
    , l.block_number
    , l.tx_hash
    , l.index AS evt_index
    , bytearray_substring(l.topic1, 13) AS depositor_address
    , bytearray_to_uint256(l.topic2) AS validator_id
    , bytearray_to_uint256(bytearray_substring(l.data, 1, 32)) AS amount_staked
    , l.contract_address
FROM {{ source('plume', 'logs') }} l
WHERE l.contract_address = {{ staking_contract }}
    AND l.topic0 = {{ stake_evt_topic0 }}
    {% if is_incremental() %}
    AND {{ incremental_predicate('l.block_time') }}
    {% else %}
    AND l.block_time >= TIMESTAMP '{{ project_start_date }}'
    {% endif %}
