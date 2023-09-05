{{
    config(
         tags = ['dunesql']
        , alias = alias('edition_metadata')
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
CAST(NULL as varchar) AS metadata_contract_uri,
CAST(NULL as varchar) AS metadata_uri_base,

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
CAST(NULL as varchar) AS edition_description,
CASE 
    WHEN SUBSTRING(symbol, 1, 1) = '$' THEN SUBSTRING(symbol, 2)
    ELSE symbol
END AS edition_symbol,
editionSize AS edition_size,
cast( royaltyBPS as double)/1e5 AS royalty_pct,
CAST(NULL as varchar) AS image_uri,
CAST(NULL as varchar)  AS animation_uri,
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

UNION ALL

SELECT
    edition_address, created_tx_hash, created_block_number, created_block_time
    , edition_name, edition_description, edition_symbol, edition_size
    , royalty_pct, image_uri, animation_uri, metadata_contract_uri, metadata_uri_base
    , funds_recipient, default_admin, mint_type, sale_config
FROM (
    SELECT
    output_0 AS edition_address,
    call_tx_hash AS created_tx_hash,
    call_block_number AS created_block_number,
    call_block_time AS created_block_time,
    
    name AS edition_name,
    CAST(NULL AS varchar) AS edition_description,
    name AS edition_symbol,
    CAST(NULL AS uint256) AS edition_size,
    cast( json_extract_scalar(defaultRoyaltyConfiguration, '$.royaltyBPS') as double)/1e5 AS royalty_pct,
    CAST(NULL as varchar) AS image_uri,
    CAST(NULL as varchar) AS animation_uri,
    newContractURI AS metadata_contract_uri,
    CAST(NULL as varchar) AS metadata_uri_base,
    
    cast( json_extract_scalar(defaultRoyaltyConfiguration, '$.royaltyRecipient') as varbinary) AS funds_recipient,
    defaultAdmin AS default_admin,
    
    '1155 Edition' as mint_type,
    CAST(NULL AS varchar) AS sale_config,
    ROW_NUMBER() OVER (PARTITION BY output_0 ORDER BY call_trace_address DESC) as rn
    
    FROM {{ source('zora_optimism','ZoraCreator1155Factory_call_createContract') }}
    
    WHERE call_success = true
    AND output_0 IS NOT NULL
    {% if is_incremental() %}
    AND call_block_time >= NOW() - interval '7' day
    {% endif %}
    ) a
WHERE rn = 1