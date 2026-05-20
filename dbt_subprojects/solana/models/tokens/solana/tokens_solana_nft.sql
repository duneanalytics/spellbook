{{ config
(
  schema = 'tokens_solana',
  alias = 'nft',
  materialized = 'incremental',
  file_format = 'delta',
  incremental_strategy = 'merge',
  unique_key = ['unique_key', 'block_date'],
  partition_by = ['block_date'],
  post_hook='{{ hide_spells() }}'
)
}}

{#
  Merge keys:
    `unique_key` is a surrogate built from stable source-row identifiers
    per row type (see CASE at the bottom).
      - Token Metadata rows: namespaced by version + account_metadata
        (call_tx_id/instruction indices intentionally excluded because
        recent_update=1 can pick a different Create winning row when a
        new Verify lands, but the row's identity follows account_metadata).
      - cNFT rows: namespaced by version + (account_merkle_tree, call_tx_id,
        call_outer_instruction_index, call_inner_instruction_index) — the
        immutable mint-instruction identifier.
    `block_date` is the Create-time/mint-time partition; stable per row
    across runs. Including it in unique_key lets Delta merge prune target
    files by partition.

  Note: we intentionally do NOT set `incremental_predicates`. For the
  cNFT half that would be safe (mints are append-only), but the Token
  Metadata half can re-emit a row years after its block_date when a late
  Verify event lands — a target-side block_date predicate would exclude
  the existing row and the merge would insert a duplicate. Full-target
  merge scan is the price of strict parity for late Verify events.
#}


with
{% if is_incremental() %}
    -- account_metadata keys touched in the incremental window: any new Create*/Verify
    affected_metadata as (
        select distinct account_metadata from (
            select account_metadata
            from {{ source('mpl_token_metadata_solana','mpl_token_metadata_call_Create') }}
            where {{ incremental_predicate('call_block_time') }}
            union all
            select account_metadata
            from {{ source('mpl_token_metadata_solana','mpl_token_metadata_call_CreateMetadataAccount') }}
            where {{ incremental_predicate('call_block_time') }}
            union all
            select account_metadata
            from {{ source('mpl_token_metadata_solana','mpl_token_metadata_call_CreateMetadataAccountV2') }}
            where {{ incremental_predicate('call_block_time') }}
            union all
            select account_metadata
            from {{ source('mpl_token_metadata_solana','mpl_token_metadata_call_CreateMetadataAccountV3') }}
            where {{ incremental_predicate('call_block_time') }}
            union all
            select account_metadata
            from {{ source('mpl_token_metadata_solana','mpl_token_metadata_call_Verify') }}
            where {{ incremental_predicate('call_block_time') }}
        )
    ),
    -- prior max leaf_id per merkle tree so new cNFT mints continue numbering
    prior_max_leaf as (
        select account_merkle_tree, max(leaf_id) as prior_max_leaf
        from {{ this }}
        where account_merkle_tree is not null
        group by 1
    ),
{% endif %}
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
            {% if is_incremental() %}
            WHERE account_metadata in (select account_metadata from affected_metadata)
            {% endif %}
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
                {% if is_incremental() %}
                WHERE account_metadata in (select account_metadata from affected_metadata)
                {% endif %}
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
                {% if is_incremental() %}
                WHERE account_metadata in (select account_metadata from affected_metadata)
                {% endif %}
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
                {% if is_incremental() %}
                WHERE account_metadata in (select account_metadata from affected_metadata)
                {% endif %}

            ) m
            --we don't want it if it doesn't have a master edition
            INNER JOIN (
                SELECT account_mintAuthority, account_edition, account_metadata
                FROM {{ source('mpl_token_metadata_solana','mpl_token_metadata_call_CreateMasterEdition') }}
                {% if is_incremental() %}
                WHERE account_metadata in (select account_metadata from affected_metadata)
                {% endif %}
                UNION ALL
                SELECT account_mintAuthority, account_edition, account_metadata
                FROM {{ source('mpl_token_metadata_solana','mpl_token_metadata_call_CreateMasterEditionV3') }}
                {% if is_incremental() %}
                WHERE account_metadata in (select account_metadata from affected_metadata)
                {% endif %}
                ) master ON master.account_metadata = m.account_metadata
            ) joined_m
            LEFT JOIN {{ source('mpl_token_metadata_solana','mpl_token_metadata_call_Verify') }} v
                ON v.account_metadata = joined_m.account_metadata
                and v.account_collectionMint != 'metaqbxxUerdq28cj1RbAWkYQm3ybzjb6a8bt518x1s' --if it is this then collection was null in the update
                {% if is_incremental() %}
                and v.account_metadata in (select account_metadata from affected_metadata)
                {% endif %}
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
            FROM {{ source('bubblegum_solana','bubblegum_call_mintToCollectionV1') }}
            WHERE 1=1
            {% if is_incremental() %}
                AND {{ incremental_predicate('call_block_time') }}
            {% endif %}
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
            FROM {{ source('bubblegum_solana','bubblegum_call_mintV1') }}
            WHERE 1=1
            {% if is_incremental() %}
                AND {{ incremental_predicate('call_block_time') }}
            {% endif %}
        )

        , new_mints as (
            SELECT * FROM (
                SELECT * FROM mint_collection_v1
                UNION ALL
                SELECT * FROM mint_v1
            ) src
            {% if is_incremental() %}
            -- Idempotency: skip mints already in {{ this }} so the per-tree
            -- leaf_id row_number() doesn't double-assign positions that the
            -- prior run already used. A mint's identity is the immutable
            -- (account_merkle_tree, call_tx_id, instruction indices) tuple.
            WHERE NOT EXISTS (
                SELECT 1 FROM {{ this }} t
                WHERE t.version = 'cNFT'
                  AND t.account_merkle_tree = src.account_merkleTree
                  AND t.call_tx_id = src.call_tx_id
                  AND coalesce(t.call_outer_instruction_index, -1) = coalesce(src.call_outer_instruction_index, -1)
                  AND coalesce(t.call_inner_instruction_index, -1) = coalesce(src.call_inner_instruction_index, -1)
            )
            {% endif %}
        )

        SELECT
            n.*
            {% if is_incremental() %}
            , coalesce(pm.prior_max_leaf, 0)
              + row_number() over (partition by n.account_merkleTree
                  order by n.call_block_slot asc, n.call_outer_instruction_index asc, COALESCE(n.call_inner_instruction_index,0) asc)
              as leaf_id
            {% else %}
            , row_number() over (partition by n.account_merkleTree
                order by n.call_block_slot asc, n.call_outer_instruction_index asc, COALESCE(n.call_inner_instruction_index,0) asc)
              as leaf_id
            {% endif %}
        FROM new_mints n
        {% if is_incremental() %}
        LEFT JOIN prior_max_leaf pm on pm.account_merkle_tree = n.account_merkleTree
        {% endif %}
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
    -- additive columns (do not change existing ones above)
    , cast(null as integer) as call_outer_instruction_index
    , cast(null as integer) as call_inner_instruction_index
    , cast(date_trunc('day', call_block_time) as date) as block_date
    , {{ dbt_utils.generate_surrogate_key(['version', 'account_metadata']) }} as unique_key
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
    -- additive columns
    , call_outer_instruction_index
    , call_inner_instruction_index
    , cast(date_trunc('day', call_block_time) as date) as block_date
    , {{ dbt_utils.generate_surrogate_key(['version', 'account_merkleTree', 'call_tx_id', 'call_outer_instruction_index', 'call_inner_instruction_index']) }} as unique_key
FROM cnfts
