{{ config
(
  alias = 'fungible',
  
  post_hook='{{ expose_spells(\'["solana"]\',
                                  "sector",
                                  "tokens",
                                  \'["ilemi"]\') }}'
)
}}


with 
    tokens as (
        SELECT
        bytearray_to_bigint(bytearray_reverse(bytearray_substring(call_data, 2, 1))) as decimals
        , call_data
        , account_mint
        , call_tx_id
        , call_block_time
        FROM (
            SELECT call_data, account_mint, call_tx_id, call_block_time FROM {{ source('spl_token_solana', 'spl_token_call_initializeMint') }}
            UNION ALL 
            SELECT call_data, account_mint, call_tx_id, call_block_time FROM {{ source('spl_token_solana', 'spl_token_call_initializeMint2') }}
        )
        {% if is_incremental() %}
        where call_block_time >= date_trunc('day', now() - interval '7' day)
        {% endif %}
    )
    
    , metadata as (
        SELECT 
            meta.call_tx_id
            , meta.call_block_slot
            , meta.call_block_time
            , meta.args
            , meta.account_metadata
            , meta.account_mint
            , meta.call_block_time
            , master.account_edition as master_edition
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
            FROM  {{ source('mpl_token_metadata_solana', 'mpl_token_metadata_call_CreateMetadataAccount') }}
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
            FROM {{ source('mpl_token_metadata_solana', 'mpl_token_metadata_call_CreateMetadataAccountV2') }}
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
            FROM {{ source('mpl_token_metadata_solana', 'mpl_token_metadata_call_CreateMetadataAccountV3') }} 
        ) meta 
        LEFT JOIN (
            SELECT account_mintAuthority, account_edition, account_metadata FROM {{ source('mpl_token_metadata_solana', 'mpl_token_metadata_call_CreateMasterEdition') }} 
            UNION ALL
            SELECT account_mintAuthority, account_edition, account_metadata FROM {{ source('mpl_token_metadata_solana', 'mpl_token_metadata_call_CreateMasterEditionV3') }}
            ) master ON master.account_metadata = meta.account_metadata
        {% if is_incremental() %}
        WHERE meta.call_block_time >= date_trunc('day', now() - interval '7' day)
        {% endif %}
    )

SELECT
    tk.account_mint as token_mint_address
    , tk.decimals
    , trim(json_value(args, 'strict $.name'))as name 
    , trim(json_value(args, 'strict $.symbol')) as symbol 
    , trim(json_value(args, 'strict $.uri')) as token_uri
    , tk.call_block_time as created_at
FROM tokens tk
LEFT JOIN metadata m ON tk.account_mint = m.account_mint
WHERE m.master_edition is null

UNION ALL

--wrapped sol is special and doesn't have a init tx (that I can find)
SELECT 
  trim(token_mint_address) as token_mint_address
  , decimals
  , trim(name) as name
  , trim(symbol) as symbol
  , token_uri
  , cast(created_at as timestamp) created_at
FROM 
(
  VALUES
(
  'So11111111111111111111111111111111111111112',
  9,
  'wrapped SOL',
  'SOL',
  null,
  '2021-01-31 00:00:00'
)
) AS temp_table (token_mint_address, decimals, name, symbol, token_uri, created_at)
