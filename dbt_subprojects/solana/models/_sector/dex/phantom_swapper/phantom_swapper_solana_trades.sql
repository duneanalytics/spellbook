{{ config(
    alias = 'trades',
    schema = 'phantom_swapper_solana',
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    unique_key = ['block_month', 'block_slot', 'tx_index', 'outer_instruction_index', 'inner_instruction_index']
   )
}}

WITH fee_accounts AS ( 
    -- Jupiter AND OKX transfer partner fee directly to a tokenaccount owned by 25mY,9Yj,tzv or 8ps
    SELECT fee_receiver
    FROM (
        VALUES
        ('25mYnjJ2MXHZH6NvTTdA63JvjgRVcuiaj6MRiEQNs1Dq')
        , ('9yj3zvLS3fDMqi1F8zhkaWfq8TZpZWHe6cz1Sgt7djXf')
        , ('tzvXws1qhmfdPkPcprezULCDQPAJqzPhbZ3SMrqRPNE')
        , ('8psNvWTrdNTiVRNzAgsou9kETXNJm2SXZyaKuJraVRtf')
    ) AS x (fee_receiver)
    UNION ALL 

    -- Jupiter transfers partner fee to a ReferralTokenAccount owned by a ReferralAccount owned by 25mY
    SELECT referraltokenaccount AS fee_receiver
    FROM (
        SELECT
        account_arguments[1] AS creator
        , account_arguments[3] AS referralaccount
        , account_arguments[4] AS referraltokenaccount
        FROM {{ source('solana','instruction_calls') }}
        WHERE executing_account = 'REFER4ZgmyYx9c6He5XfaTMiGfdLwRnkV4RPp9t9iF3'
        AND account_arguments[2] = '45ruCyfdRkWpRNGEqWzjCiXRHkZs8WXCLQ67Pnpye7Hp'
        AND TRY_CAST(data AS VARCHAR) like '%7d12465f%'
        AND block_time >= TIMESTAMP '2023-11-30' -- First date phantom ReferralTokenAccount created
        AND block_time < TIMESTAMP '2025-02-12' -- Last date phantom ReferralTokenAccount created
        )
    WHERE referralaccount IN (
        'CnmA6Zb8hLrG33AT4RTzKdGv1vKwRBKQQr8iNckvv8Yg'
        , '2rQZb9xqQGwoCMDkpabbzDB9wyPTjSPj9WNhJodTaRHm'
        , '9gnLg6NtVxaASvxtADLFKZ9s8yHft1jXb1Vu6gVKvh1J'
        , 'wtpXRqKLdGc7vpReogsRugv6EFCw4HBHcxm8pFcR84a'
        , 'D1NJy3Qq3RKBG29EDRj28ozbGwnhmM5yBUp8PonSYUnm'
    )
)
        
, allFeePayments AS (
    SELECT
    tx_id
    , token_mint_address AS fee_token_mint_address
    FROM {{ source('solana','account_activity') }} a
    INNER JOIN fee_accounts f ON f.fee_receiver = a.address
    WHERE tx_success
    {% if is_incremental() %} 
    AND {{ incremental_predicate('a.block_time') }} 
    {% else %}
    AND a.block_time >= TIMESTAMP '2024-10-01' -- Query times out if I go back farther
    {% endif %}
    AND (balance_change > 0 OR token_balance_change > 0) -- Phantom accepts fees in both SOL and alt tokens
)
   
SELECT
    t.block_time
    , CAST(date_trunc('day', t.block_time) AS date) AS block_date
    , CAST(date_trunc('month', t.block_time) AS date) AS block_month
    , 'solana' AS blockchain
    , amount_usd
    , token_bought_amount
    , token_bought_symbol
    , token_bought_mint_address AS token_bought_address
    , token_sold_amount
    , token_sold_symbol
    , token_sold_mint_address AS token_sold_address
    , project
    , t.version
    , token_pair
    , project_program_id AS project_contract_address
    , trader_id AS user
    , t.tx_id
    , tx_index
    , outer_instruction_index
    , inner_instruction_index
FROM {{ ref('dex_solana_trades') }} t
    INNER JOIN allFeePayments fp ON t.tx_id = fp.tx_id
    INNER JOIN {{ source('solana', 'transactions') }} tx ON t.tx_id = tx.id 
    {% if is_incremental() %} 
    AND {{ incremental_predicate('tx.block_time') }} 
    {% else %}
    AND tx.block_time >= TIMESTAMP '2024-10-01'
    {% endif %}
    LEFT JOIN fee_accounts fa1 ON fa1.fee_receiver = t.trader_id
    LEFT JOIN fee_accounts fa2 ON fa2.fee_receiver = tx.signer
WHERE fa1.fee_receiver IS NULL -- Exclude trades signed by FeeWallet
   AND fa2.fee_receiver IS NULL -- Exclude trades signed by FeeWallet 
   {% if is_incremental() %} 
   AND {{ incremental_predicate('t.block_time') }} 
   {% else %}
   AND t.block_time >= TIMESTAMP '2024-10-01'
   {% endif %}