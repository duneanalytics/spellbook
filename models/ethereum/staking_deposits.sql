{{ config(
    alias = 'events',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['tx_hash'],
    post_hook='{{ expose_spells(\'["ethereum"]\',
                                "project",
                                "ethereum",
                                \'["hildobby"]\') }}')
}}

SELECT et.block_time
, et.block_number
, et.value/POWER(10, 18) AS amount_staked
, et.from AS depositor_address
, ete.entity AS depositor_entity
, ete.category AS depositor_entity_category
, et.tx_hash
FROM ethereum.traces et
LEFT JOIN {{ ref('ethereum_staking_entities')}} ete ON et.from=ete.address
WHERE et.to='0x00000000219ab540356cbb839cbe05303d7705fa'
AND et.block_time >= '2020-10-14'
AND et.value/POWER(10, 18) > 0
AND et.block_time > NOW() - interval '1 week'
{% if is_incremental() %}
AND et.block_time >= date_trunc("day", now() - interval '1 week')
{% endif %}