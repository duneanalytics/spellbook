{% macro solana_spl_transfers_macro(start_date, end_date) %}

WITH transfers_raw AS (
    SELECT
        call_block_time as block_time
        , cast(date_trunc('day', call_block_time) as date) as block_date
        , cast(date_trunc('month', call_block_time) as date) as block_month
        , call_block_slot as block_slot
        , 'transfer' as action
        , amount
        , account_source as from_token_account
        , account_destination as to_token_account
        , 'spl_token' as token_version
        , call_tx_signer as tx_signer
        , call_tx_id as tx_id
        , call_tx_index as tx_index
        , call_outer_instruction_index as outer_instruction_index
        , COALESCE(call_inner_instruction_index,0) as inner_instruction_index
        , call_outer_executing_account as outer_executing_account
    FROM 
        {{ source('spl_token_solana','spl_token_call_transferChecked') }}
    WHERE 
        1=1
        {% if is_incremental() or true -%}
        AND {{incremental_predicate('call_block_time')}}
        {% else -%}
        AND call_block_time >= {{start_date}}
        AND call_block_time < {{end_date}}
        {% endif -%}

    UNION ALL

    SELECT
        call_block_time as block_time
        , cast (date_trunc('day', call_block_time) as date) as block_date
        , cast (date_trunc('month', call_block_time) as date) as block_month
        , call_block_slot as block_slot
        , 'mint' as action
        , amount
        , cast(null as varchar) as from_token_account
        , account_account as to_token_account
        , 'spl_token' as token_version
        , call_tx_signer as tx_signer
        , call_tx_id as tx_id
        , call_tx_index as tx_index
        , call_outer_instruction_index as outer_instruction_index
        , COALESCE(call_inner_instruction_index,0) as inner_instruction_index
        , call_outer_executing_account as outer_executing_account
    FROM 
        {{ source('spl_token_solana','spl_token_call_mintTo') }}
    WHERE 
        1=1
        {% if is_incremental() or true -%}
        AND {{incremental_predicate('call_block_time')}}
        {% else -%}
        AND call_block_time >= {{start_date}}
        AND call_block_time < {{end_date}}
        {% endif -%}

    UNION ALL

    SELECT
        call_block_time as block_time
        , cast (date_trunc('day', call_block_time) as date) as block_date
        , cast (date_trunc('month', call_block_time) as date) as block_month
        , call_block_slot as block_slot
        , 'mint' as action
        , amount
        , cast(null as varchar) as from_token_account
        , account_account as to_token_account
        , 'spl_token' as token_version
        , call_tx_signer as tx_signer
        , call_tx_id as tx_id
        , call_tx_index as tx_index
        , call_outer_instruction_index as outer_instruction_index
        , COALESCE(call_inner_instruction_index,0) as inner_instruction_index
        , call_outer_executing_account as outer_executing_account
    FROM 
        {{ source('spl_token_solana','spl_token_call_mintToChecked') }}
    WHERE 
        1=1
        {% if is_incremental() or true -%}
        AND {{incremental_predicate('call_block_time')}}
        {% else -%}
        AND call_block_time >= {{start_date}}
        AND call_block_time < {{end_date}}
        {% endif -%}

    UNION ALL

    SELECT
        call_block_time as block_time
        , cast (date_trunc('day', call_block_time) as date) as block_date
        , cast (date_trunc('month', call_block_time) as date) as block_month
        , call_block_slot as block_slot
        , 'burn' as action
        , amount
        , account_account as from_token_account
        , cast(null as varchar) as to_token_account
        , 'spl_token' as token_version
        , call_tx_signer as tx_signer
        , call_tx_id as tx_id
        , call_tx_index as tx_index
        , call_outer_instruction_index as outer_instruction_index
        , COALESCE(call_inner_instruction_index,0) as inner_instruction_index
        , call_outer_executing_account as outer_executing_account
    FROM 
        {{ source('spl_token_solana','spl_token_call_burn') }}
    WHERE 
        1=1
        {% if is_incremental() or true -%}
        AND {{incremental_predicate('call_block_time')}}
        {% else -%}
        AND call_block_time >= {{start_date}}
        AND call_block_time < {{end_date}}
        {% endif -%}

    UNION ALL

    SELECT
        call_block_time as block_time
        , cast (date_trunc('day', call_block_time) as date) as block_date
        , cast (date_trunc('month', call_block_time) as date) as block_month
        , call_block_slot as block_slot
        , 'burn' as action
        , amount
        , account_account as from_token_account
        , cast(null as varchar) as to_token_account
        , 'spl_token' as token_version
        , call_tx_signer as tx_signer
        , call_tx_id as tx_id
        , call_tx_index as tx_index
        , call_outer_instruction_index as outer_instruction_index
        , COALESCE(call_inner_instruction_index,0) as inner_instruction_index
        , call_outer_executing_account as outer_executing_account
    FROM 
        {{ source('spl_token_solana','spl_token_call_burnChecked') }}
    WHERE 
        1=1
        {% if is_incremental() or true -%}
        AND {{incremental_predicate('call_block_time')}}
        {% else -%}
        AND call_block_time >= {{start_date}}
        AND call_block_time < {{end_date}}
        {% endif -%}
)
, transfers AS (
    SELECT
        block_time
        , block_date
        , block_month
        , block_slot
        , action
        , amount
        , substring(from_token_account, 1, 2) as from_token_account_prefix
        , from_token_account
        , substring(to_token_account, 1, 2) as to_token_account_prefix
        , to_token_account
        , token_version
        , tx_signer
        , tx_id
        , tx_index
        , outer_instruction_index
        , inner_instruction_index
        , outer_executing_account
        , concat(
            lpad(cast(block_slot as varchar), 12, '0'), '-',
            lpad(cast(tx_index as varchar), 6, '0'), '-',
            lpad(cast(outer_instruction_index as varchar), 4, '0'), '-',
            lpad(cast(inner_instruction_index as varchar), 4, '0')
        ) as unique_instruction_key --block time is not granular enough, build unique key from block_slot, tx_index, outer_instruction_index, inner_instruction_index
    FROM
        transfers_raw
)
, prices AS (
    SELECT
        contract_address
        , minute
        , price
        , decimals
        , symbol
    FROM 
        {{ source('prices', 'usd_forward_fill') }}
    WHERE 
        blockchain = 'solana'
        AND minute >= TIMESTAMP '2020-10-02 00:00' --solana start date
        {% if is_incremental() or true -%}
        AND {{incremental_predicate('minute')}}
        {% else -%}
        AND minute >= {{start_date}}
        AND minute < {{end_date}}
        {% endif -%}
)
SELECT
    t.block_time
    , t.block_date
    , t.block_month
    , t.block_slot
    , t.action
    , t.amount
    , CASE  
        WHEN p.decimals is null THEN null
        WHEN p.decimals = 0 THEN t.amount
        ELSE t.amount / power(10, p.decimals)
      END as amount_display
    , t.from_token_account
    , t.to_token_account
    , tk_s.token_balance_owner as from_owner
    , tk_d.token_balance_owner as to_owner
    , t.token_version
    , t.tx_signer
    , t.tx_id
    , t.tx_index
    , t.outer_instruction_index
    , t.inner_instruction_index
    , t.outer_executing_account
    , COALESCE(tk_s.token_mint_address, tk_d.token_mint_address) as token_mint_address
    , p.price as price_usd
    , CASE 
        WHEN p.decimals is null THEN null
        WHEN p.decimals = 0 THEN p.price * t.amount
        ELSE p.price * t.amount / power(10, p.decimals)
      END as amount_usd
    , p.symbol as symbol
FROM transfers as t
LEFT JOIN 
    {{ ref('solana_utils_token_accounts_state_history') }} tk_s 
    ON t.from_token_account_prefix = tk_s.address_prefix
    and t.from_token_account = tk_s.address
    and t.unique_instruction_key >= tk_s.valid_from_unique_instruction_key
    and t.unique_instruction_key < tk_s.valid_to_unique_instruction_key
LEFT JOIN 
    {{ ref('solana_utils_token_accounts_state_history') }} tk_d 
    ON t.to_token_account_prefix = tk_d.address_prefix
    and t.to_token_account = tk_d.address
    and t.unique_instruction_key >= tk_d.valid_from_unique_instruction_key
    and t.unique_instruction_key < tk_d.valid_to_unique_instruction_key
LEFT JOIN 
    {{ ref('solana_utils_token_address_mapping') }} tk_m
    ON tk_m.base58_address = COALESCE(tk_s.token_mint_address, tk_d.token_mint_address)
LEFT JOIN prices p
    ON p.contract_address = tk_m.binary_address
    AND p.minute = date_trunc('minute', t.block_time)
{%- endmacro %}