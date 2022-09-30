{{ config(
        alias = 'compound_v3_user_principals',
        partition_by = ['date'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['user_unique'],
        )
}}

{% set time_granularity = 'day' %}
{% set comet = '0xc3d688b66703497daa19211eedff47f25384cdc3' %}


-- Meghan: I think you don't need this get query part.
-- --we wrap all the main logic to get user actions in a set jinja function before feeding to macro
-- {% set get_query %}
with
    config as (
        SELECT 
            get_json_object(newconfiguration,'$.baseToken') as base
            , get_json_object(newconfiguration, '$.supplyKink')/1e18 as supplyKink
            , get_json_object(newconfiguration, '$.supplyPerYearInterestRateBase')/1e18 as supplyBaseRate
            , get_json_object(newconfiguration, '$.supplyPerYearInterestRateSlopeLow')/1e18 as supplyLowRate
            , get_json_object(newconfiguration, '$.supplyPerYearInterestRateSlopeHigh')/1e18 as supplyHighRate
            , get_json_object(newconfiguration, '$.borrowKink')/1e18 as borrowKink
            , get_json_object(newconfiguration, '$.borrowPerYearInterestRateBase')/1e18 as borrowBaseRate
            , get_json_object(newconfiguration, '$.borrowPerYearInterestRateSlopeLow')/1e18 as borrowLowRate
            , get_json_object(newconfiguration, '$.borrowPerYearInterestRateSlopeHigh')/1e18 as borrowHighRate
            FROM {{ source('compound_v3_ethereum','Configurator_evt_SetConfiguration') }}
            where cometProxy = '{{comet}}'
   ),
    
    decimals_base as (
        SELECT decimals FROM {{ ref('tokens_ethereum_erc20') }} 
        WHERE contract_address = (SELECT base FROM config)
    ),
    
    supplied_base as (
        --cUSDCv3 0xc3d688B66703497DAA19211EEdff47f25384cdc3
        with c_minted as (
            SELECT 
                date_trunc('{{time_granularity}}', evt_block_time) as date 
                , to as user
                , sum(value/pow(10,(SELECT decimals FROM decimals_base))) as minted
            FROM {{ source('erc20_ethereum', 'evt_transfer') }}
            WHERE contract_address = '{{comet}}'
            AND from = '0x0000000000000000000000000000000000000000'
            GROUP BY 1,2
        ), 
        
        c_burned as (
            SELECT
                date_trunc('{{time_granularity}}', evt_block_time) as date 
                , from as user
                , sum(value/pow(10,(SELECT decimals FROM decimals_base))) as burned
            FROM {{ source('erc20_ethereum', 'evt_transfer') }}
            WHERE contract_address = '{{comet}}'
            AND to = '0x0000000000000000000000000000000000000000'
            GROUP BY 1,2
        )
        
        SELECT 
            COALESCE(m.date, b.date) as date
            , COALESCE(m.user, b.user) as user
            , COALESCE(m.minted,0) - COALESCE(b.burned,0) as supply_diff
        FROM c_minted m
        FULL OUTER JOIN c_burned b ON m.date = b.date AND m.user = b.user
    ),
    
    borrow_base as (
        with withdrawn as (
        --totalBorrowBase is changed on withdraw where excess becomes borrow
            SELECT 
                date_trunc('{{time_granularity}}', w.evt_block_time) as date 
                , w.to as user
                , SUM(w.amount/pow(10,(SELECT decimals FROM decimals_base))) 
                    - SUM(COALESCE(tr.amount,0)/pow(10,(SELECT decimals FROM decimals_base))) as borrowed 
                -- , SUM(tr.amount/pow(10,(SELECT decimals FROM decimals_base))) as withdrawn
                -- , SUM(w.amount/pow(10,(SELECT decimals FROM decimals_base))) as borrowed_and_withdrawn
            FROM {{ source('compound_v3_ethereum','Comet_evt_Withdraw') }} w 
            LEFT JOIN {{ source('compound_v3_ethereum','Comet_evt_Transfer') }} tr 
                ON tr.evt_tx_hash = w.evt_tx_hash
                AND w.evt_index + 1 = tr.evt_index --we only want the next emitted transfer. sometimes this might be an issue if next transfer is unrelated somehow.
                AND tr.to = '0x0000000000000000000000000000000000000000'
            WHERE w.contract_address = '{{comet}}'
            GROUP BY 1, 2
        ),
        
        repay as (
        --totalBorrowBase is changed on supply (in case supplier is borrower too) as well as withdraw
            SELECT 
                date_trunc('{{time_granularity}}', w.evt_block_time) as date 
                , w.from as user
                , SUM(w.amount/pow(10,(SELECT decimals FROM decimals_base))) 
                    - SUM(COALESCE(tr.amount,0)/pow(10,(SELECT decimals FROM decimals_base))) as repayed 
                -- , SUM(tr.amount/pow(10,(SELECT decimals FROM decimals_base))) as supplied
                -- , SUM(w.amount/pow(10,(SELECT decimals FROM decimals_base))) as supplied_and_repayed
            FROM {{ source('compound_v3_ethereum','Comet_evt_Supply') }} w 
            LEFT JOIN {{ source('compound_v3_ethereum','Comet_evt_Transfer') }} tr 
                ON tr.evt_tx_hash = w.evt_tx_hash
                AND w.evt_index + 1 = tr.evt_index
                AND tr.from = '0x0000000000000000000000000000000000000000'
            WHERE w.contract_address = '{{comet}}'
            GROUP BY 1,2
        ),
        
        transferrepay as (
        --check transferBase for repay case (how much was transferred versus actually minted to dst)
            SELECT 
                date_trunc('{{time_granularity}}', call_block_time) as date 
                , dst as user
                , SUM(transferbase.amount/pow(10,(SELECT decimals FROM decimals_base))) 
                    - SUM(COALESCE(tr.amount,0)/pow(10,(SELECT decimals FROM decimals_base))) as tr_repayed
            FROM (
                SELECT contract_address, dst, amount, call_tx_hash, call_block_time FROM {{ source('compound_v3_ethereum','Comet_call_transfer') }}
                UNION ALL
                SELECT contract_address, dst, amount, call_tx_hash, call_block_time FROM {{ source('compound_v3_ethereum','Comet_call_transferAssetFrom') }}
                WHERE asset = (select base from config)
                UNION ALL
                SELECT contract_address, dst, amount, call_tx_hash, call_block_time FROM {{ source('compound_v3_ethereum','Comet_call_transferAsset') }}
                WHERE asset = (select base from config)
                ) transferbase
            LEFT JOIN {{ source('compound_v3_ethereum','Comet_evt_Transfer') }} tr 
                ON tr.evt_tx_hash = transferbase.call_tx_hash
                --in the case there are multiple transfers here, there's no way to detect to connect Transfer evt to the function call (I think?)
                AND tr.from = '0x0000000000000000000000000000000000000000'
                AND tr.to = dst
            WHERE transferbase.contract_address = '{{comet}}'
            GROUP BY 1, 2
        ),
        
        absorbed as (
            SELECT 
                date_trunc('{{time_granularity}}', evt_block_time) as date
                , borrower as user
                , SUM(basePaidOut/pow(10,(SELECT decimals FROM decimals_base))) as debt_absorbed
            FROM {{ source('compound_v3_ethereum','Comet_evt_AbsorbDebt') }}
            WHERE contract_address = '{{comet}}'
            GROUP BY 1, 2
        )
        
        SELECT 
            COALESCE(w.date, r.date, tr.date, a.date) date
            , COALESCE(w.user, r.user, tr.user, a.user) user
            , COALESCE(w.borrowed,0) 
                - COALESCE(r.repayed,0) 
                - COALESCE(tr.tr_repayed,0) 
                - COALESCE(a.debt_absorbed,0) as borrow_diff
            , w.borrowed
            , r.repayed
            , tr.tr_repayed
            , a.debt_absorbed
        FROM withdrawn w
        FULL OUTER JOIN repay r ON w.date = r.date AND w.user = r.user
        FULL OUTER JOIN transferrepay tr ON w.date = tr.date AND w.user = tr.user
        FULL OUTER JOIN absorbed a ON w.date = a.date AND w.user = a.user
    ),
    
    dates as (
        SELECT 
            explode(sequence(to_date('2022-08-22'), now(), interval '1 {{time_granularity}}')) as date
    ),
    
    supply_borrow_combined as (
        SELECT 
            *
            , sum(principal_diff) OVER (partition by user order by date asc) as principal_total
        FROM (
            SELECT
                d.date
                , COALESCE(sb.user, bb.user) as user
                , COALESCE(sb.supply_diff, 0) as supply_diff
                , COALESCE(bb.borrow_diff,0) as borrow_diff
                , COALESCE(sb.supply_diff,0)- COALESCE(bb.borrow_diff,0) as principal_diff
            FROM dates d 
            LEFT JOIN supplied_base sb ON d.date = sb.date
            FULL OUTER JOIN borrow_base bb ON d.date = bb.date AND sb.user = bb.user
        ) a
    )

    select interest_rate_test(*) from supply_borrow_combined
    limit 10

--     {% endset %}

--     {% set new_actions = run_query(get_query) %}
--
--     {{ log(new_actions) }}
--
--     {{ log(interest_rate_test(new_actions)) }}

--     SELECT
--         1 as test_macro

    --@macro here
    --apply last day rates to principal to get pv_principal
    --apply pv_principal totals to utilization rate and interest rate
    --repeat
    --first date gets no interest rate applied (i.e. if index is 0)
    
    --<upstream model> get config for assets (supply caps and LFs) and base (interest rates) over time in spellbook.
    --<downstream model> get aggregation table for borrow base and supply base and rates over time
    --<seperate model>should add in user supplied collateral so that we can get values and factors.
