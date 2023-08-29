{{
    config(
        alias = alias('edition_metadata')
        ,tags = ['dunesql']
        ,materialized = 'incremental'
        ,file_format = 'delta'
        ,incremental_strategy = 'merge'
        ,unique_key = ['nft_contract_address']
        ,post_hook='{{ expose_spells(\'["optimism"]\',
                                    "project",
                                    "decent",
                                    \'["msilb7"]\') }}'
    )
}}

SELECT 
contract_address AS sdk_contract_address,
call_tx_hash AS created_tx_hash,
call_block_time AS created_block_time,
call_block_number AS created_block_number,

output_clone AS nft_contract_address,

json_extract_scalar(_editionConfig, '$.name') AS name,
json_extract_scalar(_editionConfig, '$.symbol') AS symbol,
json_extract_scalar(_editionConfig, '$.hasAdjustableCap') AS hasAdjustableCap,
json_extract_scalar(_editionConfig, '$.isSoulbound') AS isSoulbound,
json_extract_scalar(_editionConfig, '$.maxTokens') AS maxTokens,
json_extract_scalar(_editionConfig, '$.maxTokenPurchase') AS maxTokenPurchase,
json_extract_scalar(_editionConfig, '$.presaleStart') AS presaleStart,
json_extract_scalar(_editionConfig, '$.presaleEnd') AS presaleEnd,
json_extract_scalar(_editionConfig, '$.saleStart') AS saleStart,
json_extract_scalar(_editionConfig, '$.saleEnd') AS saleEnd,
cast(json_extract_scalar(_editionConfig, '$.royaltyBPS') as double)/1e5 AS royalty_pct,
json_extract_scalar(_editionConfig, '$.tokenPrice') AS tokenPrice,
json_extract_scalar(_editionConfig, '$.payoutAddress') AS payoutAddress,

json_extract_scalar(_metadataConfig, '$.contractURI') AS contractURI,
json_extract_scalar(_metadataConfig, '$.metadataURI') AS metadataURI,
json_extract_scalar(_metadataConfig, '$.metadataRendererInit') AS metadataRendererInit,
json_extract_scalar(_metadataConfig, '$.parentIP') AS parentIP,

json_extract_scalar(_tokenGateConfig, '$.tokenAddress') AS tokenAddress,
json_extract_scalar(_tokenGateConfig, '$.minBalance') AS minBalance,
json_extract_scalar(_tokenGateConfig, '$.saleType') AS saleType,

'DCNT721A' AS mint_type

FROM {{ source('decent_optimism','DCNTSDK_call_deployDCNT721A') }} ce
WHERE call_success = true
{% if is_incremental() %}
AND ce.call_block_time >= NOW() - interval '7' day
{% endif %}

UNION ALL

SELECT 
contract_address AS sdk_contract_address,
call_tx_hash AS created_tx_hash,
call_block_time AS created_block_time,
call_block_number AS created_block_number,

output_clone AS nft_contract_address,

json_extract_scalar(_editionConfig, '$.name') AS name,
json_extract_scalar(_editionConfig, '$.symbol') AS symbol,
json_extract_scalar(_editionConfig, '$.hasAdjustableCap') AS hasAdjustableCap,
json_extract_scalar(_editionConfig, '$.isSoulbound') AS isSoulbound,
json_extract_scalar(_editionConfig, '$.maxTokens') AS maxTokens,
json_extract_scalar(_editionConfig, '$.maxTokenPurchase') AS maxTokenPurchase,
json_extract_scalar(_editionConfig, '$.presaleStart') AS presaleStart,
json_extract_scalar(_editionConfig, '$.presaleEnd') AS presaleEnd,
json_extract_scalar(_editionConfig, '$.saleStart') AS saleStart,
json_extract_scalar(_editionConfig, '$.saleEnd') AS saleEnd,
cast(json_extract_scalar(_editionConfig, '$.royaltyBPS') as double)/1e5 AS royalty_pct,
json_extract_scalar(_editionConfig, '$.tokenPrice') AS tokenPrice,
json_extract_scalar(_editionConfig, '$.payoutAddress') AS payoutAddress,

json_extract_scalar(_metadataConfig, '$.contractURI') AS contractURI,
json_extract_scalar(_metadataConfig, '$.metadataURI') AS metadataURI,
json_extract_scalar(_metadataConfig, '$.metadataRendererInit') AS metadataRendererInit,
json_extract_scalar(_metadataConfig, '$.parentIP') AS parentIP,

json_extract_scalar(_tokenGateConfig, '$.tokenAddress') AS tokenAddress,
json_extract_scalar(_tokenGateConfig, '$.minBalance') AS minBalance,
json_extract_scalar(_tokenGateConfig, '$.saleType') AS saleType,

'DCNT4907A' AS mint_type

FROM {{ source('decent_optimism','DCNTSDK_call_deployDCNT4907A') }} ce
WHERE call_success = true
{% if is_incremental() %}
AND ce.call_block_time >= NOW() - interval '7' day
{% endif %}


UNION ALL

SELECT 
contract_address AS sdk_contract_address,
call_tx_hash AS created_tx_hash,
call_block_time AS created_block_time,
call_block_number AS created_block_number,

output_clone AS nft_contract_address,

json_extract_scalar(_config, '$.name') AS name,
json_extract_scalar(_config, '$.symbol') AS symbol,
json_extract_scalar(_config, '$.hasAdjustableCap') AS hasAdjustableCap,
json_extract_scalar(_config, '$.isSoulbound') AS isSoulbound,
json_extract_scalar(_config, '$.maxTokens') AS maxTokens,
json_extract_scalar(_config, '$.maxTokenPurchase') AS maxTokenPurchase,
json_extract_scalar(_config, '$.presaleStart') AS presaleStart,
json_extract_scalar(_config, '$.presaleEnd') AS presaleEnd,
json_extract_scalar(_config, '$.saleStart') AS saleStart,
json_extract_scalar(_config, '$.saleEnd') AS saleEnd,
cast(json_extract_scalar(_config, '$.royaltyBPS') as double)/1e5 AS royalty_pct,
json_extract_scalar(_config, '$.tokenPrice') AS tokenPrice,
json_extract_scalar(_config, '$.payoutAddress') AS payoutAddress,

json_extract_scalar(_metadataConfig, '$.contractURI') AS contractURI,
json_extract_scalar(_metadataConfig, '$.metadataURI') AS metadataURI,
json_extract_scalar(_metadataConfig, '$.metadataRendererInit') AS metadataRendererInit,
json_extract_scalar(_metadataConfig, '$.parentIP') AS parentIP,

NULL AS tokenAddress,
NULL AS minBalance,
NULL AS saleType,

'DCNTCrescendo' AS mint_type

FROM {{ source('decent_optimism','DCNTSDK_call_deployDCNTCrescendo') }} ce
WHERE call_success = true
{% if is_incremental() %}
AND ce.call_block_time >= NOW() - interval '7' day
{% endif %}

UNION ALL

SELECT 
contract_address AS sdk_contract_address,
call_tx_hash AS created_tx_hash,
call_block_time AS created_block_time,
call_block_number AS created_block_number,

output_clone AS nft_contract_address,

json_extract_scalar(_config, '$.name') AS name,
json_extract_scalar(_config, '$.symbol') AS symbol,
json_extract_scalar(_config, '$.hasAdjustableCap') AS hasAdjustableCap,
json_extract_scalar(_config, '$.isSoulbound') AS isSoulbound,
json_extract_scalar(_config, '$.maxTokens') AS maxTokens,
json_extract_scalar(_config, '$.maxTokenPurchase') AS maxTokenPurchase,
json_extract_scalar(_config, '$.presaleStart') AS presaleStart,
json_extract_scalar(_config, '$.presaleEnd') AS presaleEnd,
json_extract_scalar(_config, '$.saleStart') AS saleStart,
json_extract_scalar(_config, '$.saleEnd') AS saleEnd,
cast(json_extract_scalar(_config, '$.royaltyBPS') as double)/1e5 AS royalty_pct,
json_extract_scalar(_config, '$.tokenPrice') AS tokenPrice,
json_extract_scalar(_config, '$.payoutAddress') AS payoutAddress,

json_extract_scalar(_config, '$.contractURI') AS contractURI,
json_extract_scalar(_config, '$.metadataURI') AS metadataURI,
json_extract_scalar(_config, '$.metadataRendererInit') AS metadataRendererInit,
json_extract_scalar(_config, '$.parentIP') AS parentIP,

NULL AS tokenAddress,
NULL AS minBalance,
NULL AS saleType,

'DCNTSeries' AS mint_type

FROM {{ source('decent_optimism','DCNTSDK_call_deployDCNTSeries') }} ce
WHERE call_success = true
{% if is_incremental() %}
AND ce.call_block_time >= NOW() - interval '7' day
{% endif %}