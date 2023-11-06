{{ config
(        
  schema = 'tokens_solana',
  alias = 'nft',
  
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
            , joined_m.account_payer
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
                , account_payer
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
                , account_payer
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
                    , account_payer
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
                    , account_payer
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
                    , account_payer
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

    , cnfts as (
        with 
        mint_collection_v1 as (
            SELECT
                account_merkleTree 
                , json_value(metadataArgs, 'strict $.MetadataArgs.name') as token_name
                , json_value(metadataArgs, 'strict $.MetadataArgs.symbol') as token_symbol
                , json_value(metadataArgs, 'strict $.MetadataArgs.tokenStandard.TokenStandard') as token_standard
                , replace(replace(json_value(metadataArgs, 'strict $.MetadataArgs.collection.Collection.key'), 'PublicKey(', ''), ')','') as collection_mint
                , replace(replace(json_value(metadataArgs, 'strict $.MetadataArgs.creators[*].Creator.address'), 'PublicKey(', ''), ')','') as verified_creator
                , json_value(metadataArgs, 'strict $.MetadataArgs.uri') as token_uri
                , cast(json_value(metadataArgs, 'strict $.MetadataArgs.sellerFeeBasisPoints') as double) as seller_fee_basis_points
                , json_query(metadataArgs, 'strict $.MetadataArgs.creators') as creators_struct
                , account_leafOwner
                , call_block_slot
                , call_block_time
                , call_outer_instruction_index
                , call_inner_instruction_index
                , call_tx_id
                , call_tx_signer
                -- , metadataArgs
            FROM {{ source('bubblegum_solana','bubblegum_call_mintToCollectionV1') }}
            WHERE 1=1 
        )
        
        , mint_v1 as (
            SELECT
                account_merkleTree 
                , json_value(message, 'strict $.MetadataArgs.name') as token_name
                , json_value(message, 'strict $.MetadataArgs.symbol') as token_symbol
                , json_value(message, 'strict $.MetadataArgs.tokenStandard.TokenStandard') as token_standard
                , replace(replace(json_value(message, 'strict $.MetadataArgs.collection.Collection.key'), 'PublicKey(', ''), ')','') as collection_mint
                , replace(replace(json_value(message, 'strict $.MetadataArgs.creators[*].Creator.address'), 'PublicKey(', ''), ')','') as verified_creator
                , json_value(message, 'strict $.MetadataArgs.uri') as token_uri
                , cast(json_value(message, 'strict $.MetadataArgs.sellerFeeBasisPoints') as double) as seller_fee_basis_points
                , json_query(message, 'strict $.MetadataArgs.creators') as creators_struct
                , account_leafOwner
                , call_block_slot
                , call_block_time
                , call_outer_instruction_index
                , call_inner_instruction_index
                , call_tx_id
                , call_tx_signer
                -- , message
            FROM {{ source('bubblegum_solana','bubblegum_call_mintV1') }}
        )

        SELECT 
        *
        , row_number() over (partition by account_merkleTree
            order by call_block_slot asc, call_outer_instruction_index asc, COALESCE(call_inner_instruction_index,0) asc)
            as leaf_id
        FROM (
            SELECT * FROM mint_collection_v1
            UNION ALL 
            SELECT * FROM mint_v1
        )
    )
  
SELECT
    account_mint_authority
    , cast(null as bigint) as leaf_id
    , cast(null as varchar) as account_merkle_tree
    , account_master_edition
    , account_metadata
    , account_mint
    , account_payer as minter
    , version
    , token_standard 
    , token_name 
    , token_symbol 
    , token_uri
    , seller_fee_basis_points
    , collection_mint
    , verified_creator
    , creators_struct
    , call_tx_id
    , call_block_time
    , call_block_slot
    , call_tx_signer
FROM token_metadata tk 
WHERE recent_update = 1

UNION ALL 

SELECT 
    cast(null as varchar) as account_mint_authority
    , cast(leaf_id as bigint) as leaf_id
    , account_merkleTree as account_merkle_tree
    , cast(null as varchar) as account_master_edition
    , cast(null as varchar) as account_metadata
    , cast(null as varchar) as account_mint
    , account_leafOwner as minter
    , 'cNFT' as version
    , token_standard 
    , token_name 
    , token_symbol 
    , token_uri
    , seller_fee_basis_points
    , collection_mint
    , verified_creator
    , creators_struct
    , call_tx_id
    , call_block_time
    , call_block_slot
    , call_tx_signer
FROM cnfts 