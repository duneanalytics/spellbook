{{ config(
        alias ='accounting',
        partition_by = ['period'],
        materialized = 'table',
        file_format = 'delta',
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                "project",
                                "lido_accounting",
                                \'["ppclunghe", "gregshestakovlido "]\') }}'
        )
}}

with tokens AS (
select * from (values 
    (LOWER('0x5A98FcBEA516Cf06857215779Fd812CA3beF1B32')), --LDO
    (LOWER('0x6B175474E89094C44Da98b954EedeAC495271d0F')),   --DAI
    (LOWER('0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48')),   --USDC
    (LOWER('0xdAC17F958D2ee523a2206206994597C13D831ec7')), -- USDT
    (LOWER('0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2')),   --WETH
    (LOWER('0x7D1AfA7B718fb893dB30A3aBc0Cfc608AaCfeBB0')),   --MATIC
    (LOWER('0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84'))  --stETH
) as tokens(address)),
 
eth_prices as (
 SELECT  DATE_TRUNC('day', minute) AS period, 
        contract_address AS token,
        symbol,
        decimals,
        price
    FROM {{source('prices','usd')}}
    WHERE blockchain = 'ethereum'
    AND contract_address = LOWER('0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2') 
    AND EXTRACT(hour FROM minute) = 23
    AND EXTRACT(minute FROM minute) = 59
    AND minute >= '2020-10-01'
),

tokens_prices AS (

SELECT  DATE_TRUNC('day', prices.usd.minute) AS period, 
        prices.usd.contract_address AS token,
        prices.usd.symbol,
        prices.usd.decimals,
        prices.usd.price,
        eth_prices.price as eth_usd_price,
        prices.usd.price/eth_prices.price as token_eth_price
    FROM {{source('prices','usd')}}
    left join eth_prices on DATE_TRUNC('day', prices.usd.minute) =  eth_prices.period
    WHERE prices.usd.blockchain = 'ethereum'
    AND prices.usd.contract_address IN (SELECT address FROM tokens)
    AND EXTRACT(hour FROM prices.usd.minute) = 23
    AND EXTRACT(minute FROM prices.usd.minute) = 59
    AND minute >= '2020-10-01'
union all

SELECT  DATE_TRUNC('day', prices.usd.minute) AS period, 
        prices.usd.contract_address, --stSOL
        'stSOL',
        0,
        prices.usd.price,
        prices.usd.price as eth_usd_price,
        prices.usd.price/eth_prices.price as token_eth_price
    FROM {{source('prices','usd')}}
    left join eth_prices on DATE_TRUNC('day', prices.usd.minute) =  eth_prices.period
    WHERE prices.usd.symbol = 'stSOL' 
    AND EXTRACT(hour FROM prices.usd.minute) = 23
    AND EXTRACT(minute FROM prices.usd.minute) = 59

)



    SELECT  accounts.period, 
            accounts.evt_tx_hash as hash,
            accounts.primary_label,
            accounts.secondary_label,
            accounts.account,
            accounts.category,
            
            SUM(coalesce(accounts.token_amount, 0))/coalesce(POWER(10,coalesce(tokens_prices.decimals, pt.decimals)),1) AS value_base_token,
            
            CASE WHEN pt.symbol = 'WETH' THEN 'ETH' 
                 WHEN tokens_prices.token = '7dHbWXmci3dT8UFYWYZweBLXgycu7Y3iL6trKn1Y7ARj' THEN 'stSOL'
                 ELSE pt.symbol END AS base_token,
            coalesce(tokens_prices.token, accounts.token) AS base_token_address,
            
            coalesce(SUM(accounts.token_amount*tokens_prices.price)/POWER(10,coalesce(tokens_prices.decimals, pt.decimals)), 0) AS value_usd,
            case when coalesce(tokens_prices.token, accounts.token) = lower('0xae7ab96520de3a18e5e111b5eaab095312d7fe84') 
                 then SUM(coalesce(accounts.token_amount, 0))/coalesce(POWER(10,coalesce(tokens_prices.decimals, pt.decimals)),1) 
                 else coalesce(SUM(accounts.token_amount*tokens_prices.token_eth_price)/POWER(10,coalesce(tokens_prices.decimals, pt.decimals)), 0) 
            end AS value_eth,
            coalesce(tokens_prices.price, 0) as token_price,
            coalesce(tokens_prices.token_eth_price, 0) as token_eth_price

    FROM (

    -- Staked ETH
    SELECT  period,
            tx_hash as evt_tx_hash,
            '1. Assets' AS primary_label,
            '1.1. Staked Assets' AS secondary_label,
            '1.1.1. Staked ETH' AS account,
            '-' AS category,
            COALESCE(amount_staked,0) AS token_amount,
            coalesce(token, LOWER('0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2')) as token --ETH
    FROM {{ref('lido_accounting_ethereum_deposits')}}
    
    
    UNION ALL
    
    select  period,
            tx_hash,
            '2. Liabilities' AS primary_label,
            '2.1. Staked Assets' AS secondary_label,
            '2.1.1. stETH in Circulation' AS account,
            '-' AS category,
            COALESCE(amount_staked,0) AS token_amount,
            LOWER('0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84') AS token
    FROM {{ref('lido_accounting_ethereum_deposits')}}
    
    -- ========================================================= Gross staking rewards with the corresponding increase in asset accounts
    UNION ALL
    
    SELECT  period,
            evt_tx_hash,
            '3. Surplus' AS primary_label,
            '3.2. Operating Performance' AS secondary_label,
            '3.2.1. Net Revenue' AS account,
            '3.2.1.1. Gross staking rewards (+)' AS category,
            COALESCE(total,0) AS token_amount,
            token
    FROM {{ref('lido_accounting_ethereum_revenue')}}
    
    UNION ALL
    
    SELECT  period,
            evt_tx_hash,
            '1. Assets' AS primary_label,
            '1.3. Protocol Assets' AS secondary_label,
            '1.3.1. Protocol Assets' AS account,
            '1.3.1.1. stETH' AS category,
            COALESCE(total,0),
            coalesce(token,LOWER('0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84')) AS token
    FROM {{ref('lido_accounting_ethereum_revenue')}}
    
    
    
    UNION ALL
    
    SELECT  period, 
            evt_tx_hash,
            '1. Assets' AS primary_label,
            '1.1. Staked Assets' AS secondary_label,
            '1.1.1. Staked ETH' AS account,
            '-' AS category,
            COALESCE(total,0),
            coalesce(token,LOWER('0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84')) AS token
    FROM {{ref('lido_accounting_ethereum_revenue')}}
    
    UNION ALL
    
    SELECT  period,
            evt_tx_hash,
            '2. Liabilities' AS primary_label,
            '2.1. Staked Assets' AS secondary_label,
            '2.1.1. stETH in Circulation' AS account,
            '-' AS category,
            COALESCE(total,0),
            coalesce(token,LOWER('0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84')) AS token
    FROM {{ref('lido_accounting_ethereum_revenue')}}
    
    UNION ALL
    
    -- ========================================================= Gross staking rewards sent to holders with the corresponding decrease in assets
    
    SELECT  period,
            evt_tx_hash,
            '3. Surplus' AS primary_label,
            '3.2. Operating Performance' AS secondary_label,
            '3.2.1. Net Revenue' AS account,
            '3.2.1.2. Staking rewards to holders (-)' AS category,            
            -COALESCE(depositors_revenue,0),
            coalesce(token,LOWER('0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84')) AS token
    FROM {{ref('lido_accounting_ethereum_revenue')}}
    
    UNION ALL
    
    SELECT  period,
            evt_tx_hash,
            '1. Assets' AS primary_label,
            '1.3. Protocol Assets' AS secondary_label,
            '1.3.1. Protocol Assets' AS account,
            '1.3.1.1. stETH' AS category,
            -COALESCE(depositors_revenue,0),
            coalesce(token,LOWER('0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84')) AS token
    FROM {{ref('lido_accounting_ethereum_revenue')}}
    
    UNION ALL
    
    -- ========================================================= Cost of revenue to node operators
    
    SELECT  period,
            evt_tx_hash,
            '3. Surplus' AS primary_label,
            '3.2. Operating Performance' AS secondary_label,
            '3.2.2. Cost of Revenue' AS account,
            '3.2.2.1. Staking rewards to node operators (-)' AS category,            
            -COALESCE(operators_revenue,0),
            coalesce(token,LOWER('0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84')) AS token
    FROM {{ref('lido_accounting_ethereum_revenue')}}
    
    UNION ALL
    
    SELECT  period,
            evt_tx_hash,
            '1. Assets' AS primary_label,
            '1.3. Protocol Assets' AS secondary_label,
            '1.3.1. Protocol Assets' AS account,
            '1.3.1.1. stETH' AS category,
            -COALESCE(operators_revenue,0),
            coalesce(token,LOWER('0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84')) AS token
    FROM {{ref('lido_accounting_ethereum_revenue')}}
    
    UNION ALL
    
    -- ========================================================= Provisions for slashing set aside 
    -- On the same side of the balance sheet, a decrease in equity must be associated with an increase in liabilities to balance
    -- Slashing provision should go to an effective liability account through the following steps:
    -- 1. Recognize the expense and the contra asset account 
    -- 2. Recognize the slashing liability and the associated asset account
    
    SELECT  period,
            evt_tx_hash,
            '3. Surplus' AS primary_label,
            '3.2. Operating Performance' AS secondary_label,
            '3.2.2. Cost of Revenue' AS account,
            '3.2.2.2. Provision for slashing (-)' AS category,            
            -COALESCE(insurance_revenue,0),
            coalesce(token,LOWER('0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84')) AS token
    FROM {{ref('lido_accounting_ethereum_revenue')}}
    
    UNION ALL
    
    SELECT  period,
            evt_tx_hash,
            '1. Assets' AS primary_label,
            '1.2. Slashing Provision' AS secondary_label,
            '1.2.2. Slashing Provision Contra Assets' AS account,
            '-' AS category,
            -COALESCE(insurance_revenue,0),
            coalesce(token,LOWER('0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84')) AS token
    FROM {{ref('lido_accounting_ethereum_revenue')}}
    
    UNION ALL
    
    SELECT  period,
            evt_tx_hash,
            '2. Liabilities' AS primary_label,
            '2.2. Slashing Provision' AS secondary_label,
            '2.2.1. Slashing Provision' AS account,
            '-' AS category,
            COALESCE(insurance_revenue,0),
            coalesce(token,LOWER('0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84')) AS token
    FROM {{ref('lido_accounting_ethereum_revenue')}}
    
    UNION ALL
    
    SELECT  period,
            evt_tx_hash,
            '1. Assets' AS primary_label,
            '1.2. Slashing Provision' AS secondary_label,
            '1.2.1. Slashing Provision' AS account,
            '-' AS category,
            COALESCE(insurance_revenue,0),
            coalesce(token,LOWER('0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84')) AS token
    FROM {{ref('lido_accounting_ethereum_revenue')}}
    
    UNION ALL
    
    -- ========================================================= Other expenses
    
    SELECT  period,
            evt_tx_hash,
            '3. Surplus' AS primary_label,
            '3.2. Operating Performance' AS secondary_label,
            '3.2.2. Cost of Revenue' AS account,
            '3.2.2.3. Other costs of revenue (-)' AS category,
            -0,
            LOWER('0xae7ab96520de3a18e5e111b5eaab095312d7fe84') AS base_token
    FROM {{ref('lido_accounting_ethereum_revenue')}}
    
    UNION ALL
    
    SELECT  period,
            evt_tx_hash,
            '1. Assets' AS primary_label,
            '1.3. Protocol Assets' AS secondary_label,
            '1.3.1. Protocol Assets' AS account,
            '1.3.1.1. stETH' AS category,            
            -0,
            LOWER('0xae7ab96520de3a18e5e111b5eaab095312d7fe84') AS base_token
    FROM {{ref('lido_accounting_ethereum_revenue')}}
    
    -- ========================================================= LDO denominated Deposit Referrals
    -- This only works for LDO denominated expenses.
    -- Hypothetical DAI or stETH expenses in the same category would have to hit the Surplus and need a separate think
    UNION ALL
    
    SELECT  period,
            evt_tx_hash,
            '3. Surplus' AS primary_label,
            '3.2. Operating Performance' AS secondary_label,
            '3.2.3. Sales & Marketing Incentives' AS account,
            '3.2.3.2. Deposit Referrals' AS category,
            -COALESCE(CAST(amount_token AS DOUBLE), 0),
            token
    FROM {{ref('lido_accounting_ethereum_ldo_referral_payment')}}
    
    UNION ALL
    
    SELECT  period,
            evt_tx_hash,
            '3. Surplus' AS primary_label,
            '3.3. LDO token transactions' AS secondary_label,
            '3.3.1. LDO Contra Equity' AS account,
            '3.3.1.2. Deposit Referrals' AS category,
            COALESCE(CAST(amount_token AS DOUBLE), 0),
            token
    FROM {{ref('lido_accounting_ethereum_ldo_referral_payment')}}
    
    UNION ALL
    
    SELECT  period,
            evt_tx_hash,
            '3. Surplus' AS primary_label,
            '3.2. Operating Performance' AS secondary_label,
            '3.2.3. Sales & Marketing Incentives' AS account,
            '3.2.3.2. Deposit Referrals' AS category,
            -COALESCE(CAST(amount_token AS DOUBLE), 0),
            token
    FROM {{ref('lido_accounting_ethereum_dai_referral_payment')}}
    
    UNION ALL 
    
    SELECT  period,
            evt_tx_hash,
            '1. Assets' as primary_label,
            '1.3. Protocol Assets' as secondary_label,
            '1.3.1. Protocol Assets' as account,
            '1.3.1.2. DAI' AS category,
            -COALESCE(CAST(amount_token AS DOUBLE), 0),
            token
    FROM {{ref('lido_accounting_ethereum_dai_referral_payment')}}
    
    
    UNION ALL
    
    -- ========================================================= LDO denominated Liquidity Incentives
    -- This only works for LDO denominated expenses.
    -- Hypothetical DAI or stETH expenses in the same category would have to hit the Surplus and need a separate think
    
    SELECT  period,
            evt_tx_hash,
            '3. Surplus' AS primary_label,
            '3.2. Operating Performance' AS secondary_label,
            '3.2.3. Sales & Marketing Incentives' AS account,
            '3.2.3.1. Liquidity Rewards' AS category,
            -COALESCE(amount_token,0),
            token
    FROM {{ref('lido_accounting_ethereum_liquidity_incentives')}}
    
    UNION ALL
    
    SELECT  period,
            evt_tx_hash,
            '3. Surplus' as primary_label,
            '3.3. LDO token transactions' as secondary_label,
            '3.3.1. LDO Contra Equity' as account,
            '3.3.1.1. Liquidity Rewards' as category,
            COALESCE(amount_token,0),
            token
    FROM {{ref('lido_accounting_ethereum_liquidity_incentives')}}
    WHERE token IN (LOWER('0x5a98fcbea516cf06857215779fd812ca3bef1b32'))
    
    
    UNION ALL
    
    SELECT  period,
            evt_tx_hash,
            '1. Assets' as primary_label,
            '1.3. Protocol Assets' as secondary_label,
            '1.3.1. Protocol Assets' as account,
            CASE
                WHEN token = LOWER('0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84') THEN '1.3.1.1. stETH'
                WHEN token = LOWER('0x6B175474E89094C44Da98b954EedeAC495271d0F') THEN '1.3.1.2. DAI'
                WHEN token = LOWER('0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2') THEN '1.3.1.3. ETH'
                ELSE '1.3.1.4. Other'
            END AS category,            
            COALESCE(amount_token,0),
            token
            
    FROM {{ref('lido_accounting_ethereum_liquidity_incentives')}}
    WHERE token NOT IN (LOWER('0x5a98fcbea516cf06857215779fd812ca3bef1b32'))
    
    -- ========================================================= LDO denominated Domain Incentives
    -- This only works for LDO denominated expenses.
    -- Hypothetical DAI or stETH expenses in the same category would have to hit the Surplus and need a separate think
    UNION ALL
    
    SELECT  period,
            evt_tx_hash,
            '3. Surplus' AS primary_label,
            '3.2. Operating Performance' AS secondary_label,
            '3.2.3. Sales & Marketing Incentives' AS account,
            '3.2.3.3. Domain Incentives' AS category,
            -amount_token AS value,
            token
    FROM {{ref('lido_accounting_ethereum_lox_incentives')}}

    
    UNION ALL
    
    SELECT  period,
            evt_tx_hash,
            '3. Surplus' AS primary_label,
            '3.3. LDO token transactions' AS secondary_label,
            '3.3.1. LDO Contra Equity' AS account,
            '3.3.1.3. Domain Incentives' AS category,
            amount_token AS value,
            token
    FROM {{ref('lido_accounting_ethereum_lox_incentives')}}
    WHERE token IN (LOWER('0x5a98fcbea516cf06857215779fd812ca3bef1b32'))
    
    
    UNION ALL
    
    SELECT  period,
            evt_tx_hash,
            '1. Assets' AS primary_label,
            '1.3. Protocol Assets' AS secondary_label,
            '1.3.1. Protocol Assets' AS account,
            CASE
                WHEN token = LOWER('0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84') THEN '1.3.1.1. stETH'
                WHEN token = LOWER('0x6B175474E89094C44Da98b954EedeAC495271d0F') THEN '1.3.1.2. DAI'
                WHEN token = LOWER('0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2') THEN '1.3.1.3. ETH'
                ELSE '1.3.1.4. Other'
            END AS category,
            amount_token AS value,
            token
    FROM {{ref('lido_accounting_ethereum_lox_incentives')}}
    WHERE token NOT IN (LOWER('0x5a98fcbea516cf06857215779fd812ca3bef1b32'))
    
    
    -- ========================================================= LEGO Grants
    UNION ALL
    
    SELECT  period,
            evt_tx_hash,
            '3. Surplus' AS primary_label,
            '3.2. Operating Performance' AS secondary_label,
            '3.2.4. Operating expenses' AS account,
            '3.2.4.2. LEGO grants' AS category,
            -amount_token,
            token 
    FROM {{ref('lido_accounting_ethereum_lego_expenses')}}
    
    
    UNION ALL
    
    select  period,
            evt_tx_hash,
            '1. Assets' AS primary_label,
            '1.3. Protocol Assets' AS secondary_label,
            '1.3.1. Protocol Assets' AS account,
            CASE
                WHEN token = LOWER('0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84') THEN '1.3.1.1. stETH'
                WHEN token = LOWER('0x6B175474E89094C44Da98b954EedeAC495271d0F') THEN '1.3.1.2. DAI'
                WHEN token = LOWER('0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2') THEN '1.3.1.3. ETH'
                ELSE '1.3.1.4. Other'
            END AS category,
            -amount_token,
            token
    FROM {{ref('lido_accounting_ethereum_lego_expenses')}}
    WHERE token NOT IN (LOWER('0x5a98fcbea516cf06857215779fd812ca3bef1b32'))
    
    UNION ALL 
    
    SELECT  period,
            evt_tx_hash,
            '3. Surplus' AS primary_label,
            '3.3. LDO token transactions' AS secondary_label,
            '3.3.1. LDO Contra Equity' AS account,
            '3.3.1.5. LEGO' AS category,
            amount_token,
            token
    FROM {{ref('lido_accounting_ethereum_lego_expenses')}}
    WHERE token IN (LOWER('0x5a98fcbea516cf06857215779fd812ca3bef1b32'))
    
    UNION ALL
    -- ========================================================= Operating expenses from service entities
    
    SELECT  period,
            evt_tx_hash,
            '3. Surplus' AS primary_label,
            '3.2. Operating Performance' AS secondary_label,
            '3.2.4. Operating expenses' AS account,
            '3.2.4.1. Operating expenses' AS category,
            -amount_token AS value,
            token 
    FROM {{ref('lido_accounting_ethereum_operating_expenses')}}
    
    
    UNION ALL
    
    SELECT  period,
            evt_tx_hash,
            '1. Assets' AS primary_label,
            '1.3. Protocol Assets' AS secondary_label,
            '1.3.1. Protocol Assets' AS account,
            CASE
                WHEN token = LOWER('0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84') THEN '1.3.1.1. stETH'
                WHEN token = LOWER('0x6B175474E89094C44Da98b954EedeAC495271d0F') THEN '1.3.1.2. DAI'
                WHEN token = LOWER('0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2') THEN '1.3.1.3. ETH'
                ELSE '1.3.1.4. Other'
            END AS category,
            -amount_token,
            token
    FROM {{ref('lido_accounting_ethereum_operating_expenses')}}
    WHERE token NOT IN (LOWER('0x5a98fcbea516cf06857215779fd812ca3bef1b32'))
    
    UNION ALL
    
    SELECT  period,
            evt_tx_hash,
            '3. Surplus' AS primary_label,
            '3.3. LDO token transactions' AS secondary_label,
            '3.3.1. LDO Contra Equity' AS account,
            '3.3.1.6. Other' AS category,
            amount_token,
            token
    FROM {{ref('lido_accounting_ethereum_operating_expenses')}}
    WHERE token IN (LOWER('0x5a98fcbea516cf06857215779fd812ca3bef1b32'))
    
    
    UNION ALL
    
    -- ========================================================= All other operating expenses
    --not-LDO expenses
    
    SELECT  period,
            evt_tx_hash,
            '3. Surplus' AS primary_label,
            '3.2. Operating Performance' AS secondary_label,
            '3.2.4. Operating expenses' AS account,
            '3.2.4.4. Other' AS category,
            -amount_token,
            token 
    FROM {{ref('lido_accounting_ethereum_other_expenses')}}
    WHERE token NOT IN (LOWER('0x5a98fcbea516cf06857215779fd812ca3bef1b32'))
    
    
    
    UNION ALL
    
    SELECT  period,
            evt_tx_hash,
            '1. Assets' AS primary_label,
            '1.3. Protocol Assets' AS secondary_label,
            '1.3.1. Protocol Assets' AS account,
            CASE
                WHEN token = LOWER('0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84') THEN '1.3.1.1. stETH'
                WHEN token = LOWER('0x6B175474E89094C44Da98b954EedeAC495271d0F') THEN '1.3.1.2. DAI'
                WHEN token = LOWER('0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2') THEN '1.3.1.3. ETH'
                ELSE '1.3.1.4. Other'
            END AS category,
            -amount_token,
            token
    FROM {{ref('lido_accounting_ethereum_other_expenses')}}
    WHERE token NOT IN (LOWER('0x5a98fcbea516cf06857215779fd812ca3bef1b32'))
    
    --LDO expenses
    UNION ALL
    
    SELECT  period,
            evt_tx_hash,
            '3. Surplus' AS primary_label,
            '3.2. Operating Performance' AS secondary_label,
            '3.2.4. Operating expenses' AS account,
            '3.2.4.1. Operating expenses' AS category,
            -amount_token,
            token 
    FROM {{ref('lido_accounting_ethereum_other_expenses')}}
    WHERE token IN (LOWER('0x5a98fcbea516cf06857215779fd812ca3bef1b32'))
    
    UNION ALL
    
    
    SELECT  period,
            evt_tx_hash,
            '3. Surplus' AS primary_label,
            '3.3. LDO token transactions' AS secondary_label,
            '3.3.1. LDO Contra Equity' AS account,
            '3.3.1.6. Other' AS category,
            amount_token,
            token
    FROM   {{ref('lido_accounting_ethereum_other_expenses')}}
    WHERE token IN (LOWER('0x5a98fcbea516cf06857215779fd812ca3bef1b32'))
    
    
    UNION ALL
    
    SELECT  period,
            evt_tx_hash,
            '3. Surplus' AS primary_label,
            '3.2. Operating Performance' AS secondary_label,
            '3.2.4. Operating expenses' AS account,
            '3.2.4.3. TRP grants' AS category,
            -amount_token,
            token 
    FROM {{ref('lido_accounting_ethereum_trp_expenses')}}
    WHERE token IN (LOWER('0x5a98fcbea516cf06857215779fd812ca3bef1b32'))
    
    
    
    UNION ALL
    
    
    SELECT  period,
            evt_tx_hash,
            '3. Surplus' AS primary_label,
            '3.3. LDO token transactions' AS secondary_label,
            '3.3.1. LDO Contra Equity' AS account,
            '3.3.1.6. Other' AS category,
            amount_token,
            token
    FROM {{ref('lido_accounting_ethereum_trp_expenses')}}
    WHERE token IN (LOWER('0x5a98fcbea516cf06857215779fd812ca3bef1b32'))
    
    
    UNION ALL
    -- ========================================================= Non standard protocol surplus revenues
    
    SELECT  period,
            evt_tx_hash,
            '3. Surplus' AS primary_label,
            '3.2. Operating Performance' AS secondary_label,
            '3.2.5. Other income' AS account,
            '-' AS category,
            amount_token,
            token
    FROM {{ref('lido_accounting_ethereum_other_income')}}
    WHERE token NOT IN (LOWER('0x5a98fcbea516cf06857215779fd812ca3bef1b32'))
    
    
    
    UNION ALL
    
    SELECT  period,
            evt_tx_hash,
            '1. Assets' AS primary_label,
            '1.3. Protocol Assets' AS secondary_label,
            '1.3.1. Protocol Assets' AS account,
            CASE
                WHEN token = LOWER('0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84') THEN '1.3.1.1. stETH'
                WHEN token = LOWER('0x6B175474E89094C44Da98b954EedeAC495271d0F') THEN '1.3.1.2. DAI'
                WHEN token = LOWER('0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2') THEN '1.3.1.3. ETH'
                ELSE '1.3.1.4. Other'
            END AS category,
            amount_token,
            token
    FROM {{ref('lido_accounting_ethereum_other_income')}}
    WHERE token NOT IN (LOWER('0x5a98fcbea516cf06857215779fd812ca3bef1b32'))
    
    
    UNION ALL 
    
    SELECT  period,
            evt_tx_hash,
            '3. Surplus' AS primary_label,
            '3.1. Protocol Capital' AS secondary_label,
            '3.1.1. Protocol Assets' AS account,
            CASE
                WHEN token = LOWER('0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84') THEN '3.1.1.1. stETH'
                WHEN token = LOWER('0x6B175474E89094C44Da98b954EedeAC495271d0F') THEN '3.1.1.2. DAI'
                WHEN token = LOWER('0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2') THEN '3.1.1.3. ETH'
                ELSE '3.1.1.4. Other'
            END AS category,
            CAST(amount_token AS DOUBLE),
            token
    FROM {{ref('lido_accounting_ethereum_fundraising')}}
    WHERE token NOT IN (LOWER('0x5a98fcbea516cf06857215779fd812ca3bef1b32'))
    
    
    
    UNION ALL
    
    SELECT  period,
            evt_tx_hash,
            '1. Assets' AS primary_label,
            '1.3. Protocol Assets' AS secondary_label,
            '1.3.1. Protocol Assets' AS account,
            CASE
                WHEN token = LOWER('0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84') THEN '1.3.1.1. stETH'
                WHEN token = LOWER('0x6B175474E89094C44Da98b954EedeAC495271d0F') THEN '1.3.1.2. DAI'
                WHEN token = LOWER('0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2') THEN '1.3.1.3. ETH'
                ELSE '1.3.1.4. Other'
            END AS category,
            CAST(amount_token AS DOUBLE),
            token
    FROM {{ref('lido_accounting_ethereum_fundraising')}}
    WHERE token NOT IN (LOWER('0x5a98fcbea516cf06857215779fd812ca3bef1b32'))
    

) accounts

    LEFT JOIN tokens_prices ON date_trunc('day', accounts.period) = date_trunc('day', tokens_prices.period) 
    AND ( 
        (
            (accounts.token) = (tokens_prices.token) 
            AND (accounts.token) != LOWER('0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84')
        )
        OR (
            tokens_prices.token = LOWER('0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2')
            AND accounts.token  = LOWER('0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84')
        )
    )
    LEFT JOIN {{ref('prices_tokens')}} pt ON accounts.token = pt.contract_address                     
    GROUP BY 1,2,3,4,5,6,8,9, tokens_prices.decimals, pt.decimals, tokens_prices.price, tokens_prices.token_eth_price
    ORDER BY period DESC




