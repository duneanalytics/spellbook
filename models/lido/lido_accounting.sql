{{ config(
        alias = 'accounting',
         
        materialized = 'table',
        file_format = 'delta',
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                "project",
                                "lido_accounting",
                                \'["ppclunghe", "gregshestakovlido "]\') }}'
        )
}}

with tokens AS (
        select *
        from (
                values 
                (0x5A98FcBEA516Cf06857215779Fd812CA3beF1B32), --LDO
                (0x6B175474E89094C44Da98b954EedeAC495271d0F),   --DAI
                (0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48),   --USDC
                (0xdAC17F958D2ee523a2206206994597C13D831ec7), -- USDT
                (0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2),   --WETH
                (0x7D1AfA7B718fb893dB30A3aBc0Cfc608AaCfeBB0),   --MATIC
                (0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84),  --stETH
                (0x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0)   --wstETH
        ) as tokens(address)
),
eth_prices as (
        SELECT
                CAST(DATE_TRUNC('day', minute) as date) AS period, 
                contract_address AS token,
                symbol,
                decimals,
                price
        FROM {{source('prices','usd')}}
        WHERE blockchain = 'ethereum'
        AND contract_address = 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2
        AND DATE_TRUNC('day', minute) >= date '2020-12-01'
        AND DATE_TRUNC('day', minute) <= DATE_TRUNC('day', NOW() - INTERVAL '1' DAY) 
        AND EXTRACT(hour FROM minute) = 23
        AND EXTRACT(minute FROM minute) = 59

        union all 

        SELECT
                CAST(DATE_TRUNC('day', NOW()) as date) AS period, 
                contract_address AS token,
                symbol,
                decimals,
                price
        FROM {{source('prices','usd')}}
        WHERE blockchain = 'ethereum'
        AND contract_address = 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2
        AND DATE_TRUNC('day', minute) = DATE_TRUNC('day', NOW() - INTERVAL '1' DAY) 
        AND EXTRACT(hour FROM minute) = 23
        AND EXTRACT(minute FROM minute) = 59   
),
tokens_prices AS (
        SELECT
                CAST(DATE_TRUNC('day', prices.usd.minute) as date) AS period, 
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
        AND DATE_TRUNC('day', minute) >= date '2020-12-01'
        AND DATE_TRUNC('day', minute) <= DATE_TRUNC('day', NOW() - INTERVAL '1' DAY) 
        AND EXTRACT(hour FROM prices.usd.minute) = 23
        AND EXTRACT(minute FROM prices.usd.minute) = 59

        union all 

        SELECT  
                CAST(date_trunc('minute',now()) as date) AS period, 
                p.contract_address AS token,
                p.symbol,
                p.decimals,
                p.price,
                eth_prices.price as eth_usd_price,
                p.price/eth_prices.price as token_eth_price
        FROM {{source('prices','usd')}} p
        left join eth_prices on DATE_TRUNC('day', p.minute) =  eth_prices.period
        WHERE p.blockchain = 'ethereum'
        AND p.contract_address IN (SELECT address FROM tokens)
        AND DATE_TRUNC('day', minute) = DATE_TRUNC('day', NOW() - INTERVAL '1' DAY) 
        AND EXTRACT(hour FROM p.minute) = 23
        AND EXTRACT(minute FROM p.minute) = 59    

        union all

        SELECT 
                CAST(DATE_TRUNC('day', prices.usd.minute) as date) AS period, 
                0xedd1db59799c8b7753f141585986707812d783272eed8de22fab6b2a7d58ec0463,
                'stSOL',
                0,
                prices.usd.price,
                prices.usd.price as eth_usd_price,
                prices.usd.price/eth_prices.price as token_eth_price
        FROM {{source('prices','usd')}}
        left join eth_prices on DATE_TRUNC('day', prices.usd.minute) =  eth_prices.period
        WHERE prices.usd.symbol = 'stSOL' 
        AND DATE_TRUNC('day', minute) >= date '2020-12-01'
        AND EXTRACT(hour FROM prices.usd.minute) = 23
        AND EXTRACT(minute FROM prices.usd.minute) = 59
)
SELECT  CAST(date_trunc('day', accounts.period) as date) as period, --partition columns cannot be timestamp
        accounts.evt_tx_hash as hash,
        accounts.primary_label,
        accounts.secondary_label,
        accounts.account,
        accounts.category,
        
        SUM(coalesce(accounts.token_amount, 0))/coalesce(POWER(10,coalesce(tokens_prices.decimals, pt.decimals)),1) AS value_base_token,
        
        CASE WHEN pt.symbol = 'WETH' THEN 'ETH' 
                WHEN tokens_prices.token = 0xedd1db59799c8b7753f141585986707812d783272eed8de22fab6b2a7d58ec0463--0x3764486257586d63693364543855465957595a7765424c5867796375375933694c3674724b6e31593741526a 
                THEN 'stSOL'
                ELSE pt.symbol END AS base_token,
        coalesce(tokens_prices.token, accounts.token) AS base_token_address,
        
        coalesce(SUM(accounts.token_amount*tokens_prices.price)/POWER(10,coalesce(tokens_prices.decimals, pt.decimals)), 0) AS value_usd,
        case when coalesce(tokens_prices.token, accounts.token) = 0xae7ab96520de3a18e5e111b5eaab095312d7fe84
                then SUM(coalesce(accounts.token_amount, 0))/coalesce(POWER(10,coalesce(tokens_prices.decimals, pt.decimals)),1) 
                else coalesce(SUM(accounts.token_amount*tokens_prices.token_eth_price)/POWER(10,coalesce(tokens_prices.decimals, pt.decimals)), 0) 
        end AS value_eth,
        coalesce(tokens_prices.price, 0) as token_price,
        coalesce(tokens_prices.token_eth_price, 0) as token_eth_price

FROM (
        select period,
                evt_tx_hash as evt_tx_hash,
                '1. Assets' AS primary_label,
                '1.1. Staked Assets' AS secondary_label,
                '1.1.2. Omnibuffer' AS account,
                '1.1.2.1. Increases(+)' AS category,
                COALESCE(cast(amount as double),0) AS token_amount,
                0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2 as token --ETH
        from {{ref('lido_accounting_ethereum_buffer_inflow')}} 

        union all

        select period,
                evt_tx_hash,
                '2. Liabilities' AS primary_label,
                '2.1. Staked Assets' AS secondary_label,
                '2.1.1. stETH in Circulation' AS account,
                '-' AS category,
                COALESCE(cast(amount as double),0) AS token_amount,
                0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84 AS token
        from {{ref('lido_accounting_ethereum_buffer_inflow')}}

        union all
        -- deposited to NO

        select period,
                tx_hash as evt_tx_hash,
                '1. Assets' AS primary_label,
                '1.1. Staked Assets' AS secondary_label,
                '1.1.1. Staked ETH' AS account,
                '1.1.1.1. Deposits(+)' AS category,
                COALESCE(amount_staked,0) AS token_amount,
                0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2
        from {{ref('lido_accounting_ethereum_deposits')}}

        union all

        select period,
                evt_tx_hash as evt_tx_hash,
                '1. Assets' AS primary_label,
                '1.1. Staked Assets' AS secondary_label,
                '1.1.2. Omnibuffer' AS account,
                '1.1.2.2. Decreases(-)' AS category,
                (-1)*COALESCE(cast(amount as double),0) AS token_amount,
                token
        from {{ref('lido_accounting_ethereum_buffer_outflow')}}

        union all

        select period,
                evt_tx_hash,
                '2. Liabilities' AS primary_label,
                '2.1. Staked Assets' AS secondary_label,
                '2.1.1. stETH in Circulation' AS account,
                '-' AS category,
                (-1)*COALESCE(cast(amount as double),0) AS token_amount,
                0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84 AS token
        from {{ref('lido_accounting_ethereum_buffer_outflow')}}

        union all

        select period,
                tx_hash as evt_tx_hash,
                '1. Assets' AS primary_label,
                '1.1. Staked Assets' AS secondary_label,
                '1.1.2. Omnibuffer' AS account,
                '1.1.2.2. Decreases(-)' AS category,
                (-1)*COALESCE(amount_staked,0) AS token_amount,
                0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2 as token --ETH
        from {{ref('lido_accounting_ethereum_deposits')}}


        -- withdrawn from NO
        union all

        select period,
                hash,
                '1. Assets' AS primary_label,
                '1.1. Staked Assets' AS secondary_label,
                '1.1.1. Staked ETH' AS account,
                '1.1.1.2. Withdrawals(-)' AS category,
                (-1)*COALESCE(amount,0) AS token_amount,
                0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2 as token --ETH
        from {{ref('lido_accounting_ethereum_withdrawals')}}

        union all

        select period,
                hash,
                '2. Liabilities' AS primary_label,
                '2.1. Staked Assets' AS secondary_label,
                '2.1.1. stETH in Circulation' AS account,
                '-' AS category,
                (-1)*COALESCE(cast(amount as double),0) AS token_amount,
                0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84 AS token
        from {{ref('lido_accounting_ethereum_withdrawals')}}

        -- ========================================================= Gross staking rewards with the corresponding increase in asset accounts
        UNION ALL

        SELECT  period,
                evt_tx_hash,
                '3. Surplus' AS primary_label,
                '3.2. Operating Performance' AS secondary_label,
                '3.2.1. Net Revenue' AS account,
                '3.2.1.1. Gross staking rewards (+)' AS category,
                case when date_trunc('day', period)<= cast('2023-05-15' as timestamp) then COALESCE(total,0) 
                        else COALESCE(insurance_revenue,0) + COALESCE(operators_revenue,0) + COALESCE(treasury_revenue,0) + COALESCE(depositors_revenue, 0)
                end AS token_amount,
                token
        FROM {{ref('lido_accounting_ethereum_revenue')}}

        UNION ALL

        SELECT  period,
                evt_tx_hash,
                '1. Assets' AS primary_label,
                '1.3. Protocol Assets' AS secondary_label,
                '1.3.1. Protocol Assets' AS account,
                '1.3.1.1. stETH' AS category,
                case when date_trunc('day', period)<= cast('2023-05-15' as timestamp) then COALESCE(total,0) 
                        else COALESCE(insurance_revenue,0) + COALESCE(operators_revenue,0) + COALESCE(treasury_revenue,0) + COALESCE(depositors_revenue, 0)
                end AS token_amount,
                coalesce(token, 0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84) AS token
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
                coalesce(token ,0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84) AS token
        FROM {{ref('lido_accounting_ethereum_revenue')}}

        UNION ALL

        SELECT  period,
                evt_tx_hash,
                '1. Assets' AS primary_label,
                '1.3. Protocol Assets' AS secondary_label,
                '1.3.1. Protocol Assets' AS account,
                '1.3.1.1. stETH' AS category,
                -COALESCE(depositors_revenue,0),
                coalesce(token ,0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84) AS token
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
                coalesce(token ,0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84) AS token
        FROM {{ref('lido_accounting_ethereum_revenue')}}

        UNION ALL

        SELECT  period,
                evt_tx_hash,
                '1. Assets' AS primary_label,
                '1.3. Protocol Assets' AS secondary_label,
                '1.3.1. Protocol Assets' AS account,
                '1.3.1.1. stETH' AS category,
                -COALESCE(operators_revenue,0),
                coalesce(token ,0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84) AS token
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
                coalesce(token,0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84) AS token
        FROM {{ref('lido_accounting_ethereum_revenue')}}

        UNION ALL

        SELECT  period,
                evt_tx_hash,
                '1. Assets' AS primary_label,
                '1.2. Slashing Provision' AS secondary_label,
                '1.2.2. Slashing Provision Contra Assets' AS account,
                '-' AS category,
                -COALESCE(insurance_revenue,0),
                coalesce(token ,0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84) AS token
        FROM {{ref('lido_accounting_ethereum_revenue')}}

        UNION ALL

        SELECT  period,
                evt_tx_hash,
                '2. Liabilities' AS primary_label,
                '2.2. Slashing Provision' AS secondary_label,
                '2.2.1. Slashing Provision' AS account,
                '-' AS category,
                COALESCE(insurance_revenue,0),
                coalesce(token ,0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84) AS token
        FROM {{ref('lido_accounting_ethereum_revenue')}}

        UNION ALL

        SELECT  period,
                evt_tx_hash,
                '1. Assets' AS primary_label,
                '1.2. Slashing Provision' AS secondary_label,
                '1.2.1. Slashing Provision' AS account,
                '-' AS category,
                COALESCE(insurance_revenue,0),
                coalesce(token,0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84) AS token
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
                0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84 AS base_token
        FROM {{ref('lido_accounting_ethereum_revenue')}}

        UNION ALL

        SELECT  period,
                evt_tx_hash,
                '1. Assets' AS primary_label,
                '1.3. Protocol Assets' AS secondary_label,
                '1.3.1. Protocol Assets' AS account,
                '1.3.1.1. stETH' AS category,            
                -0,
                0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84 AS base_token
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
        WHERE token IN (0x5a98fcbea516cf06857215779fd812ca3bef1b32)


        UNION ALL

        SELECT  period,
                evt_tx_hash,
                '1. Assets' as primary_label,
                '1.3. Protocol Assets' as secondary_label,
                '1.3.1. Protocol Assets' as account,
                CASE
                WHEN token in (0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84, 0x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0) THEN '1.3.1.1. stETH'
                WHEN token = 0x6B175474E89094C44Da98b954EedeAC495271d0F THEN '1.3.1.2. DAI'
                WHEN token = 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2 THEN '1.3.1.3. ETH'
                ELSE '1.3.1.4. Other'
                END AS category,            
                -COALESCE(amount_token,0),
                token
                
        FROM {{ref('lido_accounting_ethereum_liquidity_incentives')}}
        WHERE token NOT IN (0x5a98fcbea516cf06857215779fd812ca3bef1b32)

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
        WHERE token IN (0x5a98fcbea516cf06857215779fd812ca3bef1b32)


        UNION ALL

        SELECT  period,
                evt_tx_hash,
                '1. Assets' AS primary_label,
                '1.3. Protocol Assets' AS secondary_label,
                '1.3.1. Protocol Assets' AS account,
                CASE
                WHEN token in (0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84,0x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0) THEN '1.3.1.1. stETH'
                WHEN token = 0x6B175474E89094C44Da98b954EedeAC495271d0F THEN '1.3.1.2. DAI'
                WHEN token = 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2 THEN '1.3.1.3. ETH'
                ELSE '1.3.1.4. Other'
                END AS category,
                amount_token AS value,
                token
        FROM {{ref('lido_accounting_ethereum_lox_incentives')}}
        WHERE token NOT IN (0x5a98fcbea516cf06857215779fd812ca3bef1b32)


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
                WHEN token in (0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84, 0x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0) THEN '1.3.1.1. stETH'
                WHEN token = 0x6B175474E89094C44Da98b954EedeAC495271d0F THEN '1.3.1.2. DAI'
                WHEN token = 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2 THEN '1.3.1.3. ETH'
                ELSE '1.3.1.4. Other'
                END AS category,
                -amount_token,
                token
        FROM {{ref('lido_accounting_ethereum_lego_expenses')}}
        WHERE token NOT IN (0x5a98fcbea516cf06857215779fd812ca3bef1b32)

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
        WHERE token IN (0x5a98fcbea516cf06857215779fd812ca3bef1b32)

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
                WHEN token in (0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84, 0x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0) THEN '1.3.1.1. stETH'
                WHEN token = 0x6B175474E89094C44Da98b954EedeAC495271d0F THEN '1.3.1.2. DAI'
                WHEN token = 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2 THEN '1.3.1.3. ETH'
                ELSE '1.3.1.4. Other'
                END AS category,
                -amount_token,
                token
        FROM {{ref('lido_accounting_ethereum_operating_expenses')}}
        WHERE token NOT IN (0x5a98fcbea516cf06857215779fd812ca3bef1b32)

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
        WHERE token IN (0x5a98fcbea516cf06857215779fd812ca3bef1b32)
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
        WHERE token NOT IN (0x5a98fcbea516cf06857215779fd812ca3bef1b32)



        UNION ALL

        SELECT  period,
                evt_tx_hash,
                '1. Assets' AS primary_label,
                '1.3. Protocol Assets' AS secondary_label,
                '1.3.1. Protocol Assets' AS account,
                CASE
                WHEN token in (0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84, 0x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0) THEN '1.3.1.1. stETH'
                WHEN token = 0x6B175474E89094C44Da98b954EedeAC495271d0F THEN '1.3.1.2. DAI'
                WHEN token = 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2 THEN '1.3.1.3. ETH'
                ELSE '1.3.1.4. Other'
                END AS category,
                -amount_token,
                token
        FROM {{ref('lido_accounting_ethereum_other_expenses')}}
        WHERE token NOT IN (0x5a98fcbea516cf06857215779fd812ca3bef1b32)

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
        WHERE token IN (0x5a98fcbea516cf06857215779fd812ca3bef1b32)
        and evt_tx_hash <> 0xbee211044ed8f7c8e390c759388d864e4f2500a3f318552f5f522952ba7932fc
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
        WHERE token IN (0x5a98fcbea516cf06857215779fd812ca3bef1b32)
        and evt_tx_hash <> 0xbee211044ed8f7c8e390c759388d864e4f2500a3f318552f5f522952ba7932fc

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
        WHERE token  IN (0x5a98fcbea516cf06857215779fd812ca3bef1b32)

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
        WHERE token IN (0x5a98fcbea516cf06857215779fd812ca3bef1b32)


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
        WHERE token NOT IN (0x5a98fcbea516cf06857215779fd812ca3bef1b32)

        UNION ALL

        SELECT  period,
                evt_tx_hash,
                '1. Assets' AS primary_label,
                '1.3. Protocol Assets' AS secondary_label,
                '1.3.1. Protocol Assets' AS account,
                CASE
                WHEN  token in  (0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84, 0x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0) THEN '1.3.1.1. stETH'
                WHEN  token = 0x6B175474E89094C44Da98b954EedeAC495271d0F THEN '1.3.1.2. DAI'
                WHEN  token= 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2 THEN '1.3.1.3. ETH'
                ELSE '1.3.1.4. Other'
                END AS category,
                amount_token,
                token
        FROM {{ref('lido_accounting_ethereum_other_income')}} 
        WHERE token NOT IN (0x5a98fcbea516cf06857215779fd812ca3bef1b32)


        UNION ALL 


        SELECT  period,
                evt_tx_hash,
                '3. Surplus' AS primary_label,
                '3.1. Protocol Capital' AS secondary_label,
                '3.1.1. Protocol Assets' AS account,
                CASE
                WHEN token in (0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84, 0x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0) THEN '3.1.1.1. stETH'
                WHEN token = 0x6B175474E89094C44Da98b954EedeAC495271d0F THEN '3.1.1.2. DAI'
                WHEN token = 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2 THEN '3.1.1.3. ETH'
                ELSE '3.1.1.4. Other'
                END AS category,
                CAST(amount_token AS DOUBLE),
                token
        FROM {{ref('lido_accounting_ethereum_fundraising')}}
        WHERE token  NOT IN (0x5a98fcbea516cf06857215779fd812ca3bef1b32)



        UNION ALL

        SELECT  period,
                evt_tx_hash,
                '1. Assets' AS primary_label,
                '1.3. Protocol Assets' AS secondary_label,
                '1.3.1. Protocol Assets' AS account,
                CASE 
                WHEN  token in (0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84, 0x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0) THEN '1.3.1.1. stETH'
                WHEN  token = 0x6B175474E89094C44Da98b954EedeAC495271d0F THEN '1.3.1.2. DAI'
                WHEN token = 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2 THEN '1.3.1.3. ETH'
                ELSE '1.3.1.4. Other'
                END AS category,
                CAST(amount_token AS DOUBLE),
                token
        FROM {{ref('lido_accounting_ethereum_fundraising')}}
        WHERE token NOT IN (0x5a98fcbea516cf06857215779fd812ca3bef1b32)
) accounts

LEFT JOIN tokens_prices ON CAST(date_trunc('day', accounts.period) as date) = tokens_prices.period
AND (
        (
                (accounts.token) = (tokens_prices.token) 
                AND (accounts.token) != 0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84
        )
        OR
        (
                tokens_prices.token = 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2
                AND accounts.token  = 0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84
        )
)
LEFT JOIN {{ref('prices_tokens')}} pt ON accounts.token = pt.contract_address                     
GROUP BY 1,2,3,4,5,6,8,9, tokens_prices.decimals, pt.decimals, tokens_prices.price, tokens_prices.token_eth_price
ORDER BY period DESC