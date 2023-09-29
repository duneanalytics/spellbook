{{ config
(        
  schema = 'tokens_solana',
  alias = alias('nft'),
  tags = ['dunesql'],
  materialized='table',
  post_hook='{{ expose_spells(\'["solana"]\',
                                  "sector",
                                  "tokens",
                                  \'["ilemi"]\') }}'
)
}}

with 
    token_metadata as (
        SELECT
            joined_m.account_mintAuthority as account_mint_authority
            , joined_m.account_masterEdition as account_master_edition
            , joined_m.account_metadata
            , joined_m.account_mint
            , joined_m.version
            , json_value(args, 'strict $.tokenStandard.TokenStandard') as token_standard 
            , json_value(args, 'strict $.name') as token_name 
            , json_value(args, 'strict $.symbol') as token_symbol 
            , json_value(args, 'strict $.uri') as token_uri
            , cast(json_value(args, 'strict $.sellerFeeBasisPoints') as double) as seller_fee_basis_points
            , COALESCE(replace(replace(json_value(args, 'strict $.collection.Collection.key'), 'PublicKey(', ''), ')','')
                , v.account_collectionMint
                ) as collection_mint
            , replace(replace(json_value(args, 'strict $.creators[0].Creator.address'), 'PublicKey(', ''), ')','') as verified_creator
            , json_query(args, 'strict $.creators') as creators_struct
            , joined_m.call_tx_id
            , joined_m.call_block_time
            , joined_m.call_block_slot
            , COALESCE(v.call_block_time, joined_m.call_block_time) as verify_block_time
            , joined_m.call_tx_signer
            , row_number() over (partition by joined_m.account_metadata order by COALESCE(v.call_block_time, joined_m.call_block_time) desc) as recent_update
        FROM (
            SELECT 
                call_tx_id
                , call_block_slot
                , call_block_time
                , json_query(createArgs, 'lax $.CreateArgs.V1.asset_data.AssetData') as args
                , account_authority as account_mintAuthority
                , account_masterEdition
                , account_metadata
                , account_mint
                , call_tx_signer
                , 'Token Metadata' as version 
            FROM {{ source('mpl_token_metadata_solana','mpl_token_metadata_call_Create') }}
            UNION ALL 
            SELECT 
                m.call_tx_id
                , m.call_block_slot
                , m.call_block_time
                , m.args
                , master.account_mintAuthority
                , master.account_edition as account_masterEdition
                , m.account_metadata
                , m.account_mint
                , m.call_tx_signer
                , m.version 
            FROM (
                SELECT 
                    call_tx_id
                    , call_outer_instruction_index
                    , call_inner_instruction_index
                    , call_block_slot
                    , call_block_time
                    , json_query(createMetadataAccountArgs, 'lax $.CreateMetadataAccountArgs.data.Data') as args
                    , account_metadata
                    , account_mint
                    , call_tx_signer
                    , 'Token Metadata' as version 
                FROM {{ source('mpl_token_metadata_solana','mpl_token_metadata_call_CreateMetadataAccount') }}
                UNION ALL 
                SELECT 
                    call_tx_id
                    , call_outer_instruction_index
                    , call_inner_instruction_index
                    , call_block_slot
                    , call_block_time
                    , json_query(createMetadataAccountArgsV2, 'lax $.CreateMetadataAccountArgsV2.data.DataV2') as args
                    , account_metadata
                    , account_mint
                    , call_tx_signer
                    , 'Token Metadata' as version
                FROM {{ source('mpl_token_metadata_solana','mpl_token_metadata_call_CreateMetadataAccountV2') }}
                UNION ALL 
                SELECT  
                    call_tx_id
                    , call_outer_instruction_index
                    , call_inner_instruction_index                    
                    , call_block_slot
                    , call_block_time
                    , json_query(createMetadataAccountArgsV3, 'lax $.CreateMetadataAccountArgsV3.data.DataV2') as args
                    , account_metadata
                    , account_mint
                    , call_tx_signer
                    , 'Token Metadata' as version 
                FROM {{ source('mpl_token_metadata_solana','mpl_token_metadata_call_CreateMetadataAccountV3') }}
                
            ) m 
            --we don't want it if it doesn't have a master edition
            INNER JOIN (
                SELECT account_mintAuthority, account_edition, account_metadata 
                FROM {{ source('mpl_token_metadata_solana','mpl_token_metadata_call_CreateMasterEdition') }}
                UNION ALL
                SELECT account_mintAuthority, account_edition, account_metadata 
                FROM {{ source('mpl_token_metadata_solana','mpl_token_metadata_call_CreateMasterEditionV3') }}
                ) master ON master.account_metadata = m.account_metadata
            ) joined_m
            LEFT JOIN {{ source('mpl_token_metadata_solana','mpl_token_metadata_call_Verify') }} v
                ON v.account_metadata = joined_m.account_metadata
                and v.account_collectionMint != 'metaqbxxUerdq28cj1RbAWkYQm3ybzjb6a8bt518x1s' --if it is this then collection was null in the update
    )
  
SELECT
*
FROM token_metadata tk 
WHERE recent_update = 1