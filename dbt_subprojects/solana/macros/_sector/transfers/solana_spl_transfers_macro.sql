{% macro solana_spl_transfers_macro(start_date, end_date) %}

WITH base AS (
    SELECT
        call_block_time as block_time
        , cast(date_trunc('day', call_block_time) as date) as block_date
        , cast(date_trunc('month', call_block_time) as date) as block_month
        , call_block_slot as block_slot
        , 'transfer' as action
        , amount
        , cast(null as double) as fee
        , account_source as from_token_account
        , account_destination as to_token_account
        , 'spl_token' as token_version
        , call_tx_signer as tx_signer
        , call_tx_id as tx_id
        , call_outer_instruction_index as outer_instruction_index
        , COALESCE(call_inner_instruction_index,0) as inner_instruction_index
        , call_outer_executing_account as outer_executing_account
    FROM 
        {{ source('spl_token_solana','spl_token_call_transferChecked') }}
    WHERE 
        1=1
        {% if is_incremental() %}
        AND {{incremental_predicate('call_block_time')}}
        {% else %}
        AND call_block_time >= {{start_date}}
        AND call_block_time < date_add('day', 1, {{ start_date }})
        {% endif %}

    UNION ALL

    SELECT
        call_block_time as block_time
        , cast (date_trunc('day', call_block_time) as date) as block_date
        , cast (date_trunc('month', call_block_time) as date) as block_month
        , call_block_slot as block_slot
        , 'mint' as action
        , amount
        , cast(null as double) as fee
        , cast(null as varchar) as from_token_account
        , account_mintTo as to_token_account
        , 'spl_token' as token_version
        , call_tx_signer as tx_signer
        , call_tx_id as tx_id
        , call_outer_instruction_index as outer_instruction_index
        , COALESCE(call_inner_instruction_index,0) as inner_instruction_index
        , call_outer_executing_account as outer_executing_account
    FROM 
        {{ source('spl_token_solana','spl_token_call_mintTo') }}
    WHERE 
        1=1
        {% if is_incremental() %}
        AND {{incremental_predicate('call_block_time')}}
        {% else %}
        AND call_block_time >= {{start_date}}
        AND call_block_time < date_add('day', 1, {{ start_date }})
        {% endif %}

    UNION ALL

    SELECT
        call_block_time as block_time
        , cast (date_trunc('day', call_block_time) as date) as block_date
        , cast (date_trunc('month', call_block_time) as date) as block_month
        , call_block_slot as block_slot
        , 'mint' as action
        , amount
        , cast(null as double) as fee
        , cast(null as varchar) as from_token_account
        , account_mintTo as to_token_account
        , 'spl_token' as token_version
        , call_tx_signer as tx_signer
        , call_tx_id as tx_id
        , call_outer_instruction_index as outer_instruction_index
        , COALESCE(call_inner_instruction_index,0) as inner_instruction_index
        , call_outer_executing_account as outer_executing_account
    FROM 
        {{ source('spl_token_solana','spl_token_call_mintToChecked') }}
    WHERE 
        1=1
        {% if is_incremental() %}
        AND {{incremental_predicate('call_block_time')}}
        {% else %}
        AND call_block_time >= {{start_date}}
        AND call_block_time < date_add('day', 1, {{ start_date }})
        {% endif %}

    UNION ALL

    SELECT
        call_block_time as block_time
        , cast (date_trunc('day', call_block_time) as date) as block_date
        , cast (date_trunc('month', call_block_time) as date) as block_month
        , call_block_slot as block_slot
        , 'burn' as action
        , amount
        , cast(null as double) as fee
        , account_burnAccount as from_token_account
        , cast(null as varchar) as to_token_account
        , 'spl_token' as token_version
        , call_tx_signer as tx_signer
        , call_tx_id as tx_id
        , call_outer_instruction_index as outer_instruction_index
        , COALESCE(call_inner_instruction_index,0) as inner_instruction_index
        , call_outer_executing_account as outer_executing_account
    FROM 
        {{ source('spl_token_solana','spl_token_call_burn') }}
    WHERE 
        1=1
        {% if is_incremental() %}
        AND {{incremental_predicate('call_block_time')}}
        {% else %}
        AND call_block_time >= {{start_date}}
        AND call_block_time < date_add('day', 1, {{ start_date }})
        {% endif %}

    UNION ALL

    SELECT
        call_block_time as block_time
        , cast (date_trunc('day', call_block_time) as date) as block_date
        , cast (date_trunc('month', call_block_time) as date) as block_month
        , call_block_slot as block_slot
        , 'burn' as action
        , amount
        , cast(null as double) as fee
        , account_burnAccount as from_token_account
        , cast(null as varchar) as to_token_account
        , 'spl_token' as token_version
        , call_tx_signer as tx_signer
        , call_tx_id as tx_id
        , call_outer_instruction_index as outer_instruction_index
        , COALESCE(call_inner_instruction_index,0) as inner_instruction_index
        , call_outer_executing_account as outer_executing_account
    FROM 
        {{ source('spl_token_solana','spl_token_call_burnChecked') }}
    WHERE 
        1=1
        {% if is_incremental() %}
        AND {{incremental_predicate('call_block_time')}}
        {% else %}
        AND call_block_time >= {{start_date}}
        AND call_block_time < date_add('day', 1, {{ start_date }})
        {% endif %}
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
        {% if is_incremental() %}
        AND {{incremental_predicate('minute')}}
        {% else %}
        AND minute >= {{start_date}}
        AND minute < date_add('day', 1, {{ start_date }})
        {% endif %}
)
SELECT
    b.block_time
    , b.block_date
    , b.block_month
    , b.block_slot
    , b.action
    , b.amount
    , CASE  
        WHEN p.decimals is null THEN null
        WHEN p.decimals = 0 THEN b.amount
        ELSE b.amount / power(10, p.decimals)
      END as amount_display
    , b.fee
    , b.from_token_account
    , b.to_token_account
    , tk_s.token_balance_owner as from_owner
    , tk_d.token_balance_owner as to_owner
    , b.token_version
    , b.tx_signer
    , b.tx_id
    , b.outer_instruction_index
    , b.inner_instruction_index
    , b.outer_executing_account
    , COALESCE(tk_s.token_mint_address, tk_d.token_mint_address) as token_mint_address
    , p.price as price_usd
    , CASE 
        WHEN p.decimals is null THEN null
        WHEN p.decimals = 0 THEN p.price * b.amount
        ELSE p.price * b.amount / power(10, p.decimals)
      END as amount_usd
    , p.symbol as symbol
FROM base b
LEFT JOIN 
    {{ ref('solana_utils_token_accounts') }} tk_s 
    ON tk_s.address = b.from_token_account
LEFT JOIN 
    {{ ref('solana_utils_token_accounts') }} tk_d 
    ON tk_d.address = b.to_token_account
LEFT JOIN 
    {{ ref('solana_utils_token_address_mapping') }} tk_m
    ON tk_m.base58_address = COALESCE(tk_s.token_mint_address, tk_d.token_mint_address)
LEFT JOIN prices p
    ON p.contract_address = tk_m.binary_address
    AND p.minute = date_trunc('minute', b.block_time)

{% endmacro %}