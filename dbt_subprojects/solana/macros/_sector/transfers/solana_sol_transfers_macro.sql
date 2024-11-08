{% macro solana_sol_transfers_macro(start_date, end_date) %}

WITH transfers AS (
    SELECT
        'solana' as blockchain,
        call_block_time as block_time,
        cast(date_trunc('day', call_block_time) AS date) AS block_date,
        cast(date_trunc('month', call_block_time) AS date) AS block_month,
        call_block_slot as block_slot,
        call_tx_id as tx_id,
        call_tx_index as tx_index,
        call_inner_instruction_index as inner_instruction_index,
        call_outer_instruction_index as outer_instruction_index,
        call_tx_signer as tx_signer,
        COALESCE(tk_from.token_balance_owner, call_account_arguments[1]) AS from_owner, -- if the token account exists, use the owner of that, otherwise if should be an account
        COALESCE(tk_to.token_balance_owner, call_account_arguments[2]) AS to_owner,
        CASE WHEN tk_from.address IS NOT NULL THEN tk_from.address ELSE null END as from_token_account, -- if the token account exists, use the address of that, otherwise no token accounts are involved
        CASE WHEN tk_to.address IS NOT NULL THEN tk_to.address ELSE null END as to_token_account,
        'native' as token_version,
        'SOL' as symbol,
        lamports as amount,
        lamports / 1e9 as amount_display,
        'So11111111111111111111111111111111111111112' as token_mint_address,
        call_outer_executing_account as outer_executing_account,
        call_inner_executing_account as inner_executing_account,
        CASE WHEN tk_to.address IS NOT NULL THEN 'wrap' ELSE 'transfer' END as action -- if the token account exists, it's a wrap, otherwise it's a transfer
    FROM 
        {{ source('system_program_solana', 'system_program_call_Transfer') }} t
    LEFT JOIN 
        {{ ref('solana_utils_token_accounts') }} tk_from 
        ON tk_from.address = t.call_account_arguments[1]
    LEFT JOIN 
        {{ ref('solana_utils_token_accounts') }} tk_to 
        ON tk_to.address = t.call_account_arguments[2]
    WHERE
        1=1
        {% if is_incremental() %}
        AND {{incremental_predicate('call_block_time')}}
        {% else %}
        AND call_block_time >= {{start_date}}
        AND call_block_time < {{end_date}}
        {% endif %}
)
, prices AS (
    SELECT
        contract_address,
        minute,
        price,
        decimals
    FROM 
        {{ source('prices', 'usd_forward_fill') }}
    WHERE
        blockchain = 'solana'
        AND contract_address =  0x069b8857feab8184fb687f634618c035dac439dc1aeb3b5598a0f00000000001 -- SOL address
        AND minute >= TIMESTAMP '2020-10-02 00:00' --solana start date
        {% if is_incremental() %}
        AND {{incremental_predicate('minute')}}
        {% else %}
        AND minute >= {{start_date}}
        AND minute < {{end_date}}
        {% endif %}
)
SELECT
    t.blockchain
    , t.block_month
    , t.block_date
    , t.block_time
    , t.block_slot
    , t.tx_id
    , t.tx_index
    , t.inner_instruction_index
    , coalesce(t.inner_instruction_index, 0) as key_inner_instruction_index
    , t.outer_instruction_index
    , t.tx_signer
    , t.from_owner
    , t.to_owner
    , t.from_token_account
    , t.to_token_account
    , t.token_mint_address
    , t.symbol
    , t.amount_display
    , t.amount
    , p.price * (t.amount_display) as amount_usd
    , p.price as price_usd
    , t.action
    , t.outer_executing_account
    , t.inner_executing_account
    , t.token_version
FROM
    transfers t
LEFT JOIN
    prices p
    ON p.minute = date_trunc('minute', t.block_time)

{% endmacro %}