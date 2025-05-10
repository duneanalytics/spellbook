{{ config
(        
  tags=[ 'static'],
  schema = 'phantom_swapper_solana',
  alias = 'fee_addresses',
  materialized='table'
)
}}

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