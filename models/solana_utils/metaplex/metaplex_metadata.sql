 {{
  config(
        alias='daily_balances',
        materialized='incremental',
        file_format = 'delta',
        incremental_strategy='merge',
        unique_key = ['token_mint_address', 'address','day'],
        post_hook='{{ expose_spells(\'["solana"]\',
                                    "sector",
                                    "solana_utils",
                                    \'["ilemi"]\') }}')
}}

-- SELECT
--     false as is_compressed
--     , json_extract_scalar(createArgs, '$.CreateArgs.V1.asset_data.AssetData.tokenStandard.TokenStandard') as token_standard
--     , json_extract_scalar(createArgs, '$.CreateArgs.V1.asset_data.AssetData.tokenStandard.TokenStandard') as token_standard
--     , json_extract_scalar(createArgs, '$.CreateArgs.V1.asset_data.AssetData.creators[1].Creator.address') as creator_address
--     , call_tx_id
--     , call_block_time
--     , call_block_slot
-- FROM mpl_token_metadata_solana.mpl_token_metadata_call_Create

SELECT
    true as is_compressed
    , method
    , account_leafOwner
    --thing that is owned ?
    , json_value(metadataArgs, 'strict $.MetadataArgs.tokenStandard.TokenStandard') as token_standard
    , json_value(metadataArgs, 'strict $.MetadataArgs.name') as token_name 
    , json_value(metadataArgs, 'strict $.MetadataArgs.symbol') as token_symbol 
    , json_value(metadataArgs, 'strict $.MetadataArgs.uri') as token_uri
    , cast(json_value(metadataArgs, 'strict $.MetadataArgs.sellerFeeBasisPoints') as double) as seller_fee_basis_points
    , json_value(metadataArgs, 'strict $.MetadataArgs.primarySaleHappened') as primary_sale_happened
    , json_value(metadataArgs, 'strict $.MetadataArgs.isMutable') as is_mutable
    , cast(json_value(metadataArgs, 'strict $.MetadataArgs.editionNonce') as double) as edition_nonce
    , json_value(metadataArgs, 'strict $.MetadataArgs.collection.Collection.verified') as collection_verified
    , collection_mint
    , json_value(metadataArgs, 'strict $.MetadataArgs.uses') as uses
    , json_value(metadataArgs, 'strict $.MetadataArgs.tokenProgramVersion.TokenProgramVersion') as token_program_version
    , json_query(metadataArgs, 'strict $.MetadataArgs.creators') as creators
    , call_tx_id
    , call_block_time
    , call_block_slot
FROM (
SELECT 
call_block_time
, 'mintV1' as method
, call_block_slot
, call_tx_id
, message as metadataArgs
, replace(replace(json_value(message, 'strict $.MetadataArgs.collection.Collection.collection'), 'PublicKey(', ''), ')','') as collection_mint
FROM bubblegum_solana.bubblegum_call_mintV1
UNION ALL 
SELECT 
call_block_time
, 'mintToCollectionV1' as method
, call_block_slot
, call_tx_id
, metadataArgs
, account_collectionMint as collection_mint
FROM bubblegum_solana.bubblegum_call_mintToCollectionV1
)
limit 100