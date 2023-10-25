{{ config(
    schema = 'staking_ethereum',
    alias = 'entities_bitcoin_suisse',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['depositor_address'])
}}

WITH hardcoded AS (
    SELECT depositor_address, date(first_tx) AS first_tx
    FROM
    (VALUES
    (0x2a7077399b3e90f5392d55a1dc7046ad8d152348, '2020-11-19')
    , (0xdd9663bd979f1ab1bada85e1bc7d7f13cafe71f8, '2020-11-20')
    , (0x622de9bb9ff8907414785a633097db438f9a2d86, '2020-11-20')
    , (0x3837ea2279b8e5c260a78f5f4181b783bbe76a8b, '2020-11-20')
    , (0xec70e3c8afe212039c3f6a2df1c798003bf7cfe9, '2020-11-21')
    , (0xc2288b408dc872a1546f13e6ebfa9c94998316a2, '2020-11-25')
    , (0x4ebf51689228236ec55bcafef9d79663992a7fb6, '2023-08-24')
    )
    x (depositor_address, first_tx)
    )

, all_deposits AS (
    SELECT depositor_address, first_tx
    FROM hardcoded
    UNION ALL
    SELECT txs."from" AS depositor_address, MIN(evt_block_time) AS first_tx
    FROM {{ source('eth2_ethereum', 'DepositContract_evt_DepositEvent') }} dep
    INNER JOIN {{ source('ethereum', 'transactions') }} txs ON txs.block_number=dep.evt_block_number
        AND txs.hash=dep.evt_tx_hash
        AND withdrawal_credentials=0x010000000000000000000000d1026749530a15c20cb7b30368d8c15e200fe1d6
        {% if not is_incremental() %}
        AND txs.block_time >= DATE '2023-05-25'
        AND dep.evt_block_time >= DATE '2023-05-25'
        {% endif %}
        {% if is_incremental() %}
        AND txs.block_time >= date_trunc('day', now() - interval '7' day)
        AND dep.evt_block_time >= date_trunc('day', now() - interval '7' day)
        {% endif %}
    GROUP BY 1
    )

SELECT depositor_address
, 'Bitcoin Suisse' AS entity
, CONCAT('Bitcoin Suisse ', CAST(ROW_NUMBER() OVER (ORDER BY MIN(first_tx)) AS VARCHAR)) AS entity_unique_name
, 'CEX' AS category
FROM all_deposits
GROUP BY 1