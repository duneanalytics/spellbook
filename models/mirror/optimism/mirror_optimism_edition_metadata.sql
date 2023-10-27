{{
    config(
          alias = 'edition_metadata'
        ,materialized = 'incremental'
        ,file_format = 'delta'
        ,incremental_strategy = 'merge'
        ,unique_key = ['edition_address']
        ,post_hook='{{ expose_spells(\'["optimism"]\',
                                    "project",
                                    "mirror",
                                    \'["msilb7"]\') }}'
    )
}}

SELECT
output_0 AS edition_address,
contract_address AS deployer_contract,
call_tx_hash AS created_tx_hash,
call_block_number AS created_block_number,
call_block_time AS created_block_time,
json_extract_scalar(params,'$.admin') AS admin,
json_extract_scalar(params,'$._name') as name,
json_extract_scalar(params,'$._symbol') as symbol,
NULL AS description,
NULL AS imageURI,
NULL AS contentURI,
NULL as price,
NULL as "limit",
NULL AS fundingRecipient,
NULL AS owner,
json_extract_scalar(params,'$._contractURI') as contractURI,
json_extract_scalar(params,'$._uri') as uri,
json_extract_scalar(params,'$._trustedForwarders') as trustedForwarders,
json_extract_scalar(params,'$._primarySaleRecipient') as primarySaleRecipient,
json_extract_scalar(params,'$._royaltyRecipient') as royaltyRecipient,
cast(json_extract_scalar(params,'$._royaltyBps') as double)/1e5 as royalty_pct,
json_extract_scalar(params,'$._platformFee') as platformFee,
json_extract_scalar(params,'$._platformFeeRecipient') as platformFeeRecipient,
json_extract_scalar(params,'$.salt') AS salt

FROM {{ source('mirror_optimism','SignatureDropDeployer_call_deploy') }} ce

WHERE call_success = true
{% if is_incremental() %}
AND ce.call_block_time >= NOW() - interval '7' day
{% endif %}

UNION ALL

SELECT
output_clone AS edition_address,
contract_address AS deployer_contract,
call_tx_hash AS created_tx_hash,
call_block_number AS created_block_number,
call_block_time AS created_block_time,
json_extract_scalar(edition,'$.admin') AS admin,
json_extract_scalar(edition,'$.name') as name,
json_extract_scalar(edition,'$.symbol') as symbol,
json_extract_scalar(edition,'$.description') as description,
json_extract_scalar(edition,'$.imageURI') as imageURI,
json_extract_scalar(edition,'$.contentURI') as contentURI,
json_extract_scalar(edition,'$.price') as price,
json_extract_scalar(edition,'$.limit') as "limit",
json_extract_scalar(edition,'$.fundingRecipient') as fundingRecipient,
owner,
NULL as contractURI,
NULL as uri,
NULL as trustedForwarders,
NULL as primarySaleRecipient,
NULL as royaltyRecipient,
NULL as royalty_pct,
NULL as platformFee,
NULL as platformFeeRecipient,
NULL AS salt

FROM {{ source('mirror_optimism','WritingEditionsFactory_call_createWithSignature') }} ce
INNER JOIN {{ source('optimism','creation_traces') }} tr
    ON tr.block_time = ce.call_block_time
    AND tr.block_number = ce.call_block_number
    AND tr.tx_hash = ce.call_tx_hash
    AND tr.address = ce.output_clone
    {% if is_incremental() %}
    AND tr.block_time >= NOW() - interval '7' day
    {% endif %}

WHERE call_success = true
{% if is_incremental() %}
AND ce.call_block_time >= NOW() - interval '7' day
{% endif %}