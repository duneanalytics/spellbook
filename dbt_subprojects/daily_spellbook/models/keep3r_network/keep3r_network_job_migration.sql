{{ config(
    schema = 'keep3r_network'
    , alias = 'job_migration'
    , post_hook = '{{ expose_spells(\'["ethereum", "optimism", "polygon"]\',
                                "project", 
                                "keep3r",
                                 \'["0xr3x"]\') }}'
) }}


    SELECT
        evt_block_time,
        evt_tx_hash,
        evt_index,
        _fromJob,
        _toJob,
        contract_address,
        'ethereum' as blockchain
    FROM
      {{ source(
        'keep3r_network_ethereum',
        'Keep3r_evt_JobMigrationSuccessful'
      ) }}
    UNION
    SELECT
        evt_block_time,
        evt_tx_hash,
        evt_index,
        _fromJob,
        _toJob,
        contract_address,
        'ethereum' as blockchain
    FROM
      {{ source(
        'keep3r_network_ethereum',
        'Keep3r_v2_evt_JobMigrationSuccessful'
      ) }}
    UNION
    SELECT
        evt_block_time,
        evt_tx_hash,
        evt_index,
        _fromJob,
        _toJob,
        contract_address,
        'optimism' as blockchain
    FROM
      {{ source(
        'keep3r_network_optimism',
        'Keep3rSidechain_evt_JobMigrationSuccessful'
      ) }}
    UNION
    SELECT
        evt_block_time,
        evt_tx_hash,
        evt_index,
        _fromJob,
        _toJob,
        contract_address,
        'polygon' as blockchain
    FROM
      {{ source(
        'keep3r_network_polygon',
        'Keep3rSidechain_evt_JobMigrationSuccessful'
      ) }}
