{{
    config(
        alias='edition_metadata'
        ,materialized = 'incremental'
        ,file_format = 'delta'
        ,incremental_strategy = 'merge'
        ,unique_key = ['edition_address']
        ,post_hook='{{ expose_spells(\'["optimism"]\',
                                    "project",
                                    "zora",
                                    \'["msilb7"]\') }}'
    )
}}

SELECT
output_0 AS edition_address,
call_tx_hash AS created_tx_hash,
call_block_number AS created_block_number,
call_block_time AS created_block_time,

name AS edition_name,
description AS edition_description,
CASE 
    WHEN SUBSTRING(symbol, 1, 1) = '$' THEN SUBSTRING(symbol, 2)
    ELSE symbol
END AS edition_symbol,
editionSize AS edition_size,
cast( royaltyBPS as double)/1e5 AS royalty_pct,
imageURI AS image_uri,
animationURI AS animation_uri,
CAST(NULL AS STRING) AS metadata_contract_uri,
CAST(NULL AS STRING) AS metadata_uri_base,

fundsRecipient AS funds_recipient,
defaultAdmin AS default_admin,

'Edition' as mint_type,
saleConfig AS sale_config

FROM {{ source('zora_optimism','ZoraNFTCreatorProxy_call_createEdition') }} ce

WHERE call_success = true
{% if is_incremental() %}
AND ce.call_block_time >= NOW() - interval '7' day
{% endif %}

UNION ALL

SELECT
output_0 AS edition_address,
call_tx_hash AS created_tx_hash,
call_block_number AS created_block_number,
call_block_time AS created_block_time,

name AS edition_name,
CAST(NULL AS STRING) AS edition_description,
CASE 
    WHEN SUBSTRING(symbol, 1, 1) = '$' THEN SUBSTRING(symbol, 2)
    ELSE symbol
END AS edition_symbol,
editionSize AS edition_size,
cast( royaltyBPS as double)/1e5 AS royalty_pct,
CAST(NULL AS STRING) AS image_uri,
CAST(NULL AS STRING)  AS animation_uri,
metadataContractURI AS metadata_contract_uri,
metadataURIBase AS metadata_uri_base,

fundsRecipient AS funds_recipient,
defaultAdmin AS default_admin,

'Drop' as mint_type,
saleConfig AS sale_config


FROM {{ source('zora_optimism','ZoraNFTCreatorProxy_call_createDrop') }} cd
WHERE call_success = true
{% if is_incremental() %}
AND cd.call_block_time >= NOW() - interval '7' day
{% endif %}

-- UNION ALL

-- 1155 Factory -- Pending re:decoding