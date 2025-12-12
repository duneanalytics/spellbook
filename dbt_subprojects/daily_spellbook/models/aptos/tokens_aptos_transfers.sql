{{config(
    schema = 'tokens_aptos'
    , alias = 'transfers'
    , partition_by = ['block_month']
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , unique_key = ['block_date', 'tx_hash', 'block_number', 'from', 'to', 'contract_address']
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_date')]
    , post_hook='{{ expose_spells(\'["aptos"]\',
                                "sector",
                                "tokens",
                                \'["krishhh"]\') }}'
)}}

-- Price data for APT
WITH aptos_prices AS (
    SELECT
        date_trunc('day', minute) as block_date
        , avg(price) as price_usd
    FROM {{ source('prices', 'usd') }}
    WHERE symbol = 'APT' 
        AND blockchain IS NULL
    GROUP BY 1
),

-- Extract coin metadata from CoinInfo resources with proper date filtering
coin_metadata AS (
    SELECT DISTINCT
        move_address
        , json_extract_scalar(move_data, '$.name') as coin_name
        , json_extract_scalar(move_data, '$.symbol') as symbol
        , CAST(json_extract_scalar(move_data, '$.decimals') AS int) as decimals
        , block_date
    FROM {{ source('aptos', 'move_resources') }}
    WHERE move_resource_name = 'CoinInfo'
        AND block_date >= TIMESTAMP'2023-01-01'
),

-- Get the most recent metadata for each token
latest_coin_metadata AS (
    SELECT 
        move_address
        , coin_name
        , symbol
        , decimals
        , ROW_NUMBER() OVER (PARTITION BY move_address ORDER BY block_date DESC) as rn
    FROM coin_metadata
),

final_coin_metadata AS (
    SELECT 
        move_address
        , coin_name
        , symbol
        , decimals
    FROM latest_coin_metadata
    WHERE rn = 1
),

-- Extract all coin events (withdraw and deposit)
coin_events AS (
    SELECT
        e.tx_hash
        , e.event_type
        , e.event_index
        , e.event_sequence_number
        , e.guid_account_address
        , CAST(json_extract_scalar(e.data, '$.amount') AS decimal(38,0)) as amount_raw
        , e.block_date
        , e.block_time
        , e.block_height
        , e.tx_index
        , e.tx_success
        
    FROM {{ source('aptos', 'events') }} e
    WHERE e.event_type IN ('0x1::coin::WithdrawEvent', '0x1::coin::DepositEvent')
        AND e.tx_success = true
        AND CAST(json_extract_scalar(e.data, '$.amount') AS bigint) > 0
        {% if is_incremental() %}
        AND {{ incremental_predicate('e.block_date') }}
        {% endif %}
        AND e.block_date >= current_date - interval '7' day

),

-- Match withdraw and deposit events in same transaction
coin_transfers AS (
    SELECT
        w.tx_hash
        , w.block_date
        , w.block_time
        , w.block_height
        , w.tx_index
        , w.event_index as evt_index
        , w.amount_raw
        , w.guid_account_address as from_address
        , d.guid_account_address as to_address
        , COALESCE(ut.type_arguments[1], '"0x1::aptos_coin::AptosCoin"') as contract_address_raw
        , 'coin' as token_standard
        
    FROM (
        SELECT * FROM coin_events WHERE event_type = '0x1::coin::WithdrawEvent'
    ) w
    JOIN (
        SELECT * FROM coin_events WHERE event_type = '0x1::coin::DepositEvent'  
    ) d ON w.tx_hash = d.tx_hash
    LEFT JOIN {{ source('aptos', 'user_transactions') }} ut 
    ON w.tx_hash = ut.hash 
        AND ut.type_arguments IS NOT NULL
        AND cardinality(ut.type_arguments) > 0
),

-- Token (NFT) events
token_events AS (
    SELECT
        e.tx_hash
        , e.event_type
        , e.event_index
        , e.event_sequence_number
        , e.guid_account_address
        , CAST(json_extract_scalar(e.data, '$.amount') AS decimal(38,0)) as amount_raw
        , json_extract_scalar(e.data, '$.id.token_data_id.creator') as creator_address
        , json_extract_scalar(e.data, '$.id.token_data_id.collection') as collection_name
        , json_extract_scalar(e.data, '$.id.token_data_id.name') as token_name
        , e.block_date
        , e.block_time
        , e.block_height
        , e.tx_index
        , e.tx_success
        
    FROM {{ source('aptos', 'events') }} e
    WHERE e.event_type IN ('0x3::token::WithdrawEvent', '0x3::token::DepositEvent')
        AND e.tx_success = true
        AND CAST(json_extract_scalar(e.data, '$.amount') AS bigint) > 0
        {% if is_incremental() %}
        AND {{ incremental_predicate('e.block_date') }}
        {% endif %}
        AND e.block_date >= current_date - interval '7' day

),

-- Match token transfers
token_transfers AS (
    SELECT
        w.tx_hash
        , w.block_date
        , w.block_time
        , w.block_height
        , w.tx_index
        , w.event_index as evt_index
        , w.amount_raw
        , w.guid_account_address as from_address
        , d.guid_account_address as to_address
        , w.creator_address as contract_address_raw
        , w.collection_name
        , w.token_name
        , 'token' as token_standard
        
    FROM (
        SELECT * FROM token_events WHERE event_type = '0x3::token::WithdrawEvent'
    ) w
    JOIN (
        SELECT * FROM token_events WHERE event_type = '0x3::token::DepositEvent'
    ) d ON w.tx_hash = d.tx_hash
        AND w.creator_address = d.creator_address
        AND w.collection_name = d.collection_name
        AND w.token_name = d.token_name
        AND w.amount_raw = d.amount_raw
),

-- Fungible Asset events (newer standard)
fa_events AS (
    SELECT
        e.tx_hash
        , e.event_type
        , e.event_index
        , e.event_sequence_number
        , e.guid_account_address
        , CAST(json_extract_scalar(e.data, '$.amount') AS decimal(38,0)) as amount_raw
        , json_extract_scalar(e.data, '$.metadata') as fa_metadata
        , e.block_date
        , e.block_time
        , e.block_height
        , e.tx_index
        , e.tx_success
        
    FROM {{ source('aptos', 'events') }} e
    WHERE e.event_type IN ('0x1::fungible_asset::WithdrawEvent', '0x1::fungible_asset::DepositEvent')
        AND e.tx_success = true
        AND CAST(json_extract_scalar(e.data, '$.amount') AS bigint) > 0
        {% if is_incremental() %}
        AND {{ incremental_predicate('e.block_date') }}
        {% endif %}
        AND e.block_date >= current_date - interval '7' day

),

-- Match FA transfers
fa_transfers AS (
    SELECT
        w.tx_hash
        , w.block_date
        , w.block_time
        , w.block_height
        , w.tx_index
        , w.event_index as evt_index
        , w.amount_raw
        , w.guid_account_address as from_address
        , d.guid_account_address as to_address
        , w.fa_metadata as contract_address_raw
        , 'fungible_asset' as token_standard
        
    FROM (
        SELECT * FROM fa_events WHERE event_type = '0x1::fungible_asset::WithdrawEvent'
    ) w
    JOIN (
        SELECT * FROM fa_events WHERE event_type = '0x1::fungible_asset::DepositEvent'
    ) d ON w.tx_hash = d.tx_hash
        AND w.fa_metadata = d.fa_metadata
        AND w.amount_raw = d.amount_raw
)

-- Final output combining all transfer types
SELECT
    'aptos' as blockchain
    , date_trunc('month', all_transfers.block_date) as block_month
    , all_transfers.block_date
    , all_transfers.block_time
    , all_transfers.block_height as block_number
    , all_transfers.tx_hash
    , all_transfers.tx_index
    , all_transfers.evt_index
    , CAST(null AS array(bigint)) as trace_address
    , all_transfers.token_standard
    , 'transfer' as transaction_type
    , CAST(null AS varchar) as tx_from
    , CAST(null AS varchar) as tx_to
    , all_transfers.from_address as "from"
    , all_transfers.to_address as "to"
    
    -- Clean contract address
    , CASE 
        WHEN all_transfers.contract_address_raw LIKE '"%"' THEN trim('"' FROM all_transfers.contract_address_raw)
        ELSE all_transfers.contract_address_raw 
    END as contract_address
    
    -- Symbol extraction
    , CASE 
        WHEN all_transfers.token_standard = 'coin' THEN 
            COALESCE(
                cm.symbol
                , CASE 
                    WHEN all_transfers.contract_address_raw LIKE '%aptos_coin%' THEN 'APT'
                    ELSE split_part(trim('"' FROM all_transfers.contract_address_raw), '::', 3)
                END
            )
        WHEN all_transfers.token_standard = 'token' THEN all_transfers.collection_name
        WHEN all_transfers.token_standard = 'fungible_asset' THEN 'FA'
        ELSE 'UNKNOWN'
    END as symbol
    
    -- Token decimals
    , CASE 
        WHEN all_transfers.token_standard = 'coin' THEN 
            COALESCE(
                cm.decimals
                , CASE WHEN all_transfers.contract_address_raw LIKE '%aptos_coin%' THEN 8 ELSE 8 END
            )
        WHEN all_transfers.token_standard = 'token' THEN 0  -- NFTs have 0 decimals
        ELSE 8  -- Default for fungible assets
    END as token_decimals
    
    , all_transfers.amount_raw
    
    -- Calculate display amount
    , all_transfers.amount_raw / power(10, 
        CASE 
            WHEN all_transfers.token_standard = 'coin' THEN 
                COALESCE(
                    cm.decimals
                    , CASE WHEN all_transfers.contract_address_raw LIKE '%aptos_coin%' THEN 8 ELSE 8 END
                )
            WHEN all_transfers.token_standard = 'token' THEN 0
            ELSE 8
        END
    ) as amount
    
    -- Price calculations (only for APT for now)
    , CASE 
        WHEN all_transfers.contract_address_raw LIKE '%aptos_coin%'
        THEN ap.price_usd
        ELSE null 
    END as price_usd
    
    -- USD amount calculation
    , CASE 
        WHEN all_transfers.contract_address_raw LIKE '%aptos_coin%'
        THEN (all_transfers.amount_raw / power(10, 8)) * ap.price_usd
        ELSE null 
    END as amount_usd

FROM (
    -- Combine all transfer types
    SELECT 
        tx_hash
        , block_date
        , block_time
        , block_height
        , tx_index
        , evt_index
        , amount_raw
        , from_address
        , to_address
        , contract_address_raw
        , token_standard
        , CAST(null AS varchar) as collection_name
        , CAST(null AS varchar) as token_name
    FROM coin_transfers
    
    UNION ALL
    
    SELECT 
        tx_hash
        , block_date
        , block_time
        , block_height
        , tx_index
        , evt_index
        , amount_raw
        , from_address
        , to_address
        , contract_address_raw
        , token_standard
        , collection_name
        , token_name
    FROM token_transfers
    
    UNION ALL
    
    SELECT 
        tx_hash
        , block_date
        , block_time
        , block_height
        , tx_index
        , evt_index
        , amount_raw
        , from_address
        , to_address
        , contract_address_raw
        , token_standard
        , CAST(null AS varchar) as collection_name
        , CAST(null AS varchar) as token_name
    FROM fa_transfers
) all_transfers
LEFT JOIN final_coin_metadata cm ON 
    from_hex(
        CASE 
            WHEN length(substring(split_part(trim('"' FROM all_transfers.contract_address_raw), '::', 1), 3)) % 2 = 1 
            THEN '0' || substring(split_part(trim('"' FROM all_transfers.contract_address_raw), '::', 1), 3)
            ELSE substring(split_part(trim('"' FROM all_transfers.contract_address_raw), '::', 1), 3)
        END
    ) = cm.move_address
LEFT JOIN aptos_prices ap ON all_transfers.block_date = ap.block_date
WHERE from_address IS NOT NULL 
    AND to_address IS NOT NULL
    AND amount_raw > 0