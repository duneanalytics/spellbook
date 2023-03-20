{{ config(
    alias = 'vesting',
    post_hook='{{ expose_spells(\'["bnb"]\',
                                "project",
                                "impossible_finance",
                                \'["kartod"]\') }}'
    )
}}

{% set project_start_date = '2022-07-11' %}


SELECT
    
    'binance' AS blockchain,

    block_time,

    tr.`from` AS user,

    hash AS evt_tx_hash,

    b.value/1e18 AS amount,

    CASE
        WHEN substr(data,1,10) = lower('0xa694fc3a') AND b.to = lower('0x6610f572057018843bD64ADCf7c47787EB7ba8B2') THEN 'Stake vIDIA'
        WHEN substr(data,1,10) = lower('0xaf34f36c') THEN 'Unstake vIDIA with 20% penalty'--
        WHEN substr(data,1,10) = lower('0xd276290d') THEN 'Unstake vIDIA after 2 weeks waiting'--
        WHEN substr(data,1,10) = lower('0xd279c191') OR substr(data,1,10) = lower('0x2e17de78') THEN 'Rewards'--
        WHEN substr(data,1,10) = lower('0xd5293bad') THEN 'Cancel Unstake vIDIA'--
        ELSE 'Reward'
    END AS state,

    (CASE
        WHEN substr(data,1,10) = lower('0xa694fc3a') AND b.to = lower('0x6610f572057018843bD64ADCf7c47787EB7ba8B2') THEN 0
        WHEN substr(data,1,10) = lower('0xaf34f36c') THEN 20
        WHEN substr(data,1,10) = lower('0xd276290d') THEN 0
        WHEN substr(data,1,10) = lower('0xd279c191') OR substr(data,1,10) = lower('0x2e17de78') THEN 0
        WHEN substr(data,1,10) = lower('0xd5293bad') THEN 2
        ELSE 0
    END)*(b.value/1e18)/100 AS fees

FROM {{ source('bnb', 'transactions') }} tr
LEFT JOIN {{ source('erc20_bnb', 'evt_Transfer') }} b 
    ON tr.hash = b.evt_tx_hash
    AND tr.block_number = b.evt_block_number
    AND b.contract_address = lower('0x0b15Ddf19D47E6a86A56148fb4aFFFc6929BcB89')
    AND b.evt_block_time >= '{{project_start_date}}'
WHERE tr.to = lower('0x6610f572057018843bD64ADCf7c47787EB7ba8B2')
    AND tr.block_time >= '{{project_start_date}}'
;