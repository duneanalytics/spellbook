{{
    config(
        tags = ['dunesql']
        , alias= alias('edition_metadata')
        ,materialized = 'incremental'
        ,file_format = 'delta'
        ,incremental_strategy = 'merge'
        ,unique_key = ['edition_address']
        ,post_hook='{{ expose_spells(\'["optimism"]\',
                                    "project",
                                    "sound_xyz",
                                    \'["msilb7"]\') }}'
    )
}}

SELECT
     i.edition_ as edition_address
    , c.evt_tx_hash AS created_tx_hash
    , c.evt_block_number AS created_block_number
    , c.evt_block_time AS created_block_time
    , c.contracts AS edition_contracts
    , c.deployer AS deployer_address
    , i.baseURI_ AS base_uri
    , i.contractURI_ AS contract_uri
    , editionCutoffTime_ AS edition_cutoff_time
    , editionMaxMintableLower_ AS edition_max_mints_lower
    , editionMaxMintableUpper_ AS edition_max_mints_upper
    , fundingRecipient_ AS funding_recipient
    , metadataModule_ AS metadata_module
    , cast( royaltyBPS_ as double)/1e5 AS royalty_pct
    , name_ AS edition_name
    , symbol_ AS edition_symbol


FROM {{ source('sound_xyz_optimism', 'SoundCreatorV1_evt_SoundEditionCreated') }} c
    LEFT JOIN {{ source('sound_xyz_optimism', 'SoundEditionV1_2_evt_SoundEditionInitialized') }} i
        ON i.edition_ = c.soundEdition
        AND i.evt_block_number = c.evt_block_number
        AND i.evt_tx_hash = c.evt_tx_hash
        {% if is_incremental() %}
        AND i.evt_block_time >= NOW() - interval '7' day
        {% endif %}
WHERE 1=1
{% if is_incremental() %}
AND c.evt_block_time >= NOW() - interval '7' day
{% endif %}