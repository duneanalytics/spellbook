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
        call_account_arguments[1] AS from_owner,
        call_account_arguments[2] AS to_owner,
        'native' as token_version,
        'SOL' as symbol,
        lamports as amount,
        lamports / 1e9 as amount_display,
        'So11111111111111111111111111111111111111112' as token_mint_address,
        call_outer_executing_account as outer_executing_account,
        call_inner_executing_account as inner_executing_account,
        'transfer' as action
    FROM 
        {{ source('system_program_solana', 'system_program_call_Transfer') }}
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
    , coalesce(t.inner_instruction_index, 0) as inner_instruction_index
    , t.outer_instruction_index
    , t.tx_signer
    , cast(null as varchar) as from_token_account
    , cast(null as varchar) as to_token_account
    , t.from_owner
    , t.to_owner
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