{{ config
(
  alias = 'fungible',
  schema = 'tokens_solana',
  materialized = 'table',
  post_hook='{{ expose_spells(\'["solana"]\',
                                  "sector",
                                  "tokens_solana",
                                  \'["ilemi"]\') }}'
)
}}


with
    tokens as (
        SELECT
        bytearray_to_bigint(bytearray_reverse(bytearray_substring(call_data, 2, 1))) as decimals
        , call_data
        , account_mint
        , token_version
        , call_tx_id
        , call_block_time
        , row_number() over (partition by account_mint order by call_block_time desc) as latest
        FROM (
            SELECT call_data, account_mint, call_tx_id, call_block_time, 'spl_token' as token_version FROM {{ source('spl_token_solana', 'spl_token_call_initializeMint') }}
            UNION ALL
            SELECT call_data, account_mint, call_tx_id, call_block_time, 'spl_token' as token_version FROM {{ source('spl_token_solana', 'spl_token_call_initializeMint2') }}
            UNION ALL
            SELECT call_data, account_mint, call_tx_id, call_block_time, 'token2022' as token_version FROM {{ source('spl_token_2022_solana', 'spl_token_2022_call_initializeMint') }}
            UNION ALL
            SELECT call_data, account_mint, call_tx_id, call_block_time, 'token2022' as token_version FROM {{ source('spl_token_2022_solana', 'spl_token_2022_call_initializeMint2') }}
        )
        WHERE account_mint != '2b1kV6DkPAnxd5ixfnxCpjxmKwqjjaYmCZfHsFu24GXo' --pyusd do manually
        {% if is_incremental() %}
        AND {{ incremental_predicate('call_block_time') }}
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
            , metadata_program
            , row_number() over (partition by meta.account_mint order by meta.call_block_time desc) as latest
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
                , call_executing_account as metadata_program
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
                , call_executing_account as metadata_program
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
                , call_executing_account as metadata_program
            FROM {{ source('mpl_token_metadata_solana', 'mpl_token_metadata_call_CreateMetadataAccountV3') }}
        ) meta
        LEFT JOIN (
            SELECT account_mintAuthority, account_edition, account_metadata FROM {{ source('mpl_token_metadata_solana', 'mpl_token_metadata_call_CreateMasterEdition') }}
            UNION ALL
            SELECT account_mintAuthority, account_edition, account_metadata FROM {{ source('mpl_token_metadata_solana', 'mpl_token_metadata_call_CreateMasterEditionV3') }}
            ) master ON master.account_metadata = meta.account_metadata
        {% if is_incremental() %}
        WHERE {{ incremental_predicate('meta.call_block_time') }}
        {% endif %}
    )

    , token2022_metadata as (
        --token2022 direct metadata extension
        SELECT
            from_utf8(bytearray_substring(data,1+8+4,bytearray_to_bigint(bytearray_reverse(bytearray_substring(data,1+8,4))))) as name
            , from_utf8(bytearray_substring(data,1+8+4+bytearray_to_bigint(bytearray_reverse(bytearray_substring(data,1+8,4))) + 4 --start from end of name and end of length of symbol
                , bytearray_to_bigint(bytearray_reverse(bytearray_substring(data,1+8+4+bytearray_to_bigint(bytearray_reverse(bytearray_substring(data,1+8,4))),4))) --get length of symbol from end of name
                )) as symbol
            , from_utf8(bytearray_substring(data,1+8+4+bytearray_to_bigint(bytearray_reverse(bytearray_substring(data,1+8,4))) + 4 --end of name and end of length of symbol
                    + bytearray_to_bigint(bytearray_reverse(bytearray_substring(data,1+8+4+bytearray_to_bigint(bytearray_reverse(bytearray_substring(data,1+8,4))),4))) + 4 --start from end of symbol and end of length of uri
                , bytearray_to_bigint(bytearray_reverse(bytearray_substring(data,1+8+4+bytearray_to_bigint(bytearray_reverse(bytearray_substring(data,1+8,4))) + 4
                    + bytearray_to_bigint(bytearray_reverse(bytearray_substring(data,1+8+4+bytearray_to_bigint(bytearray_reverse(bytearray_substring(data,1+8,4))),4))),4))) --get length of uri from end of symbol
                )) as uri
            , tx_id as metadata_tx
            , account_arguments[3] as account_mint
            , block_time
            , executing_account as metadata_program
            , row_number() over (partition by account_arguments[3] order by block_time desc) as latest
        FROM {{ source('solana','instruction_calls') }}
        WHERE executing_account = 'TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb'
        AND bytearray_substring(data,1,1) = 0xd2 --deal with updateField later 0xdd
        AND tx_success
        {% if is_incremental() %}
        AND {{ incremental_predicate('block_time') }}
        {% endif %}
    )

    , token_metadata_other as (
        --some other metadata program (idk the owner)
        SELECT
            from_utf8(bytearray_substring(data,1+1+4,bytearray_to_bigint(bytearray_reverse(bytearray_substring(data,1+1,4))))) as name
            , from_utf8(bytearray_substring(data,1+1+4+bytearray_to_bigint(bytearray_reverse(bytearray_substring(data,1+1,4))) + 4 --start from end of name and end of length of symbol
                , bytearray_to_bigint(bytearray_reverse(bytearray_substring(data,1+1+4+bytearray_to_bigint(bytearray_reverse(bytearray_substring(data,1+1,4))),4))) --get length of symbol from end of name
                )) as symbol
            , from_utf8(bytearray_substring(data,1+1+4+bytearray_to_bigint(bytearray_reverse(bytearray_substring(data,1+1,4))) + 4 --end of name and end of length of symbol
                    + bytearray_to_bigint(bytearray_reverse(bytearray_substring(data,1+1+4+bytearray_to_bigint(bytearray_reverse(bytearray_substring(data,1+1,4))),4))) + 4 --start from end of symbol and end of length of uri
                , bytearray_to_bigint(bytearray_reverse(bytearray_substring(data,1+1+4+bytearray_to_bigint(bytearray_reverse(bytearray_substring(data,1+1,4))) + 4
                    + bytearray_to_bigint(bytearray_reverse(bytearray_substring(data,1+1+4+bytearray_to_bigint(bytearray_reverse(bytearray_substring(data,1+1,4))),4))),4))) --get length of uri from end of symbol
                )) as uri
            , tx_id as metadata_tx
            , account_arguments[2] as account_mint
            , block_time
            , executing_account as metadata_program
            , row_number() over (partition by account_arguments[2] order by block_time desc) as latest
        FROM {{ source('solana','instruction_calls') }}
        WHERE executing_account = 'META4s4fSmpkTbZoUsgC1oBnWB31vQcmnN8giPw51Zu'
        AND bytearray_substring(data,1,1) = 0x21
        AND tx_success
        {% if is_incremental() %}
        AND {{ incremental_predicate('block_time') }}
        {% endif %}
    )

SELECT
    tk.account_mint as token_mint_address
    , tk.decimals
    , coalesce(m22.name,mo.name,trim(json_value(args, 'strict $.name'))) as name
    , coalesce(m22.symbol,mo.symbol,trim(json_value(args, 'strict $.symbol'))) as symbol
    , coalesce(m22.uri,mo.uri,trim(json_value(args, 'strict $.uri'))) as token_uri
    , tk.call_block_time as created_at
    , coalesce(m22.metadata_program,mo.metadata_program,m.metadata_program) as metadata_program
    , tk.token_version
    , tk.call_tx_id as init_tx
FROM tokens tk
LEFT JOIN token2022_metadata m22 ON tk.account_mint = m22.account_mint AND m22.latest = 1
LEFT JOIN token_metadata_other mo ON tk.account_mint = mo.account_mint AND mo.latest = 1
LEFT JOIN metadata m ON tk.account_mint = m.account_mint AND m.latest = 1
WHERE m.master_edition is null
AND tk.latest = 1

UNION ALL

--token2022 wrapped sol https://solscan.io/tx/2L1o7sDMCMJ6PYqfNrnY6ozJC1DEx61pRYiLdfCCggxw81naQXsmHKDLn6EhJXmDmDSQ2eCKjUMjZAQuUsyNnYUv
SELECT
  trim(token_mint_address) as token_mint_address
  , decimals
  , trim(name) as name
  , trim(symbol) as symbol
  , token_uri
  , cast(created_at as timestamp) created_at
  , metadata_program
  , token_version
  , init_tx
FROM
(
  VALUES
  (
    '9pan9bMn5HatX4EJdBwg9VgCa7Uz5HL8N1m5D3NdXejP',
    9,
    'wrapped SOL',
    'SOL',
    null,
    '2023-08-02 00:00:00',
    null,
    'token2022',
    '2L1o7sDMCMJ6PYqfNrnY6ozJC1DEx61pRYiLdfCCggxw81naQXsmHKDLn6EhJXmDmDSQ2eCKjUMjZAQuUsyNnYUv'
  )
  , (
  '2b1kV6DkPAnxd5ixfnxCpjxmKwqjjaYmCZfHsFu24GXo',
  6,
  'Paypal USD',
  'PyUSD',
  'https://www.paypal.com/us/digital-wallet/manage-money/crypto/pyusd',
  '2024-04-22 19:58:01',
  null,
  'token2022',
  '4D1xonRin6LKLnJ6YoJ5qiSw7wSE57XMee7DnmS1CWP9hSzZXzWDCyDnRLE2Rf83TxGXMMdBYV35ZVG3kVBTnXnz'
  )
) AS temp_table (token_mint_address, decimals, name, symbol, token_uri, created_at, metadata_program, token_version, init_tx)

UNION ALL

--old wrapped sol is special and doesn't have a init tx (that I can find)
SELECT
  trim(token_mint_address) as token_mint_address
  , decimals
  , trim(name) as name
  , trim(symbol) as symbol
  , token_uri
  , cast(created_at as timestamp) created_at
  , metadata_program
  , token_version
  , init_tx
FROM
(
  VALUES
(
  'So11111111111111111111111111111111111111112',
  9,
  'wrapped SOL',
  'SOL',
  null,
  '2021-01-31 00:00:00',
  null,
  'spl_token',
  null
)
) AS temp_table (token_mint_address, decimals, name, symbol, token_uri, created_at, metadata_program, token_version, init_tx)
