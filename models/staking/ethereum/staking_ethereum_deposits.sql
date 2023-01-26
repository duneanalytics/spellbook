{{ config(
    alias = 'deposits',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_time', 'tx_hash', 'depositor_address'],
    post_hook='{{ expose_spells(\'["ethereum"]\',
                                "sector",
                                "staking",
                                \'["hildobby"]\') }}')
}}

SELECT 
    et.block_time
    , et.block_number
    , et.from AS depositor_address
    , SUM(et.value/POWER(10, 18)) AS amount_staked
    , ete.entity AS depositor_entity
    , ete.entity_unique_name AS depositor_entity_unique_name
    , ete.category AS depositor_entity_category
    --, eth2.pubkey
    --, eth2.signature
    --, eth2.withdrawal_credentials
    , et.tx_hash
FROM {{ source('ethereum', 'traces') }} et
LEFT JOIN {{ ref('staking_ethereum_entities')}} ete
    ON et.from=ete.address
--LEFT JOIN {{ source ('eth2_ethereum', 'DepositContract_evt_DepositEvent')}} eth2
--    ON et.block_time=eth2.evt_block_time
--    AND et.tx_hash=eth2.evt_tx_hash
--    AND et.evt_index=eth2.evt_index
WHERE et.to='0x00000000219ab540356cbb839cbe05303d7705fa'
    {% if not is_incremental() %}
    AND et.block_time >= '2020-10-14'
    {% endif %}
    {% if is_incremental() %}
    AND et.block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
    AND et.value/POWER(10, 18) > 0
GROUP BY block_time, et.block_number, et.from, ete.entity, ete.entity_unique_name, ete.category, et.tx_hash