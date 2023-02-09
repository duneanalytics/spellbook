{{config (
    alias = 'v2_loan_master',
    post_hook = '{{ 
        expose_spells(\'["polygon"]\',
        "project", 
        "rocifi",
        \'["maybeyonas"]\') }}'
)}} with score as (
    select evt_block_time,
        score,
        tokenId as token_id
    from {{source('rocifi_v2_polygon', 'ScoreDB_evt_ScoreUpdated')}}
),
NFCS as (
    select m.evt_block_time as mint_time,
        _recipient as minter,
        _tokenId::int as token_id,
        explode(_addressBundle) as addr
    from {{source('rocifi_polygon', 'NFCS_evt_TokenMinted')}} m
),
loan_and_nfcs_events as (
    select evt_block_time,
        'updateScore' as event,
        score,
        token_id,
        null as loan_id
    from score
    union all
    select evt_block_time,
        'loanCreated' as event,
        null as score,
        token_id,
        loanId as loan_id
    from {{source(
            'rocifi_v2_polygon',
            'LoanManager_evt_LoanCreated'
        )}} b
        join NFCS n on b.borrower = n.addr
),
ranked_loan_and_nfcs_events as (
    select evt_block_time,
        event,
        loan_id,
        coalesce(
            score,
            last(score, true) over(
                partition by token_id
                order by evt_block_time range unbounded preceding
            )
        ) as score,
        token_id
    from loan_and_nfcs_events
    order by evt_block_time desc
),
loan_id_score as (
    -- finding the credit score during the loanCreation, by finding the last 'scoreUpdate' before the Loan is created
    select loan_id,
        coalesce(score, 404) as nfcs_score,
        token_id as nfcs_id
    from ranked_loan_and_nfcs_events
    where event = 'loanCreated'
),
borrow_raw_info as (
    select elc.evt_block_time as block_time,
        elc.evt_tx_hash as tx_hash,
        elc.borrower,
        elc.pool as pool_address,
        case
            elc.pool
            when lower('0x4eBB81605f91C02827426E37001d402bF46a170d') then 'rUSDC1' -- usdc1
            when lower('0x94C29F381A65344d65baB80f321660A75C237815') then 'rUSDT1' -- usdt1
            else 'sus'
        end as pool,
        case
            elc.pool
            when lower('0x4eBB81605f91C02827426E37001d402bF46a170d') then '0x2791bca1f2de4661ed88a30c99a7a9449aa84174' -- usdc1
            when lower('0x94C29F381A65344d65baB80f321660A75C237815') then '0xc2132d05d31c914a87c6611c10748aeb04b58e8f' -- usdt1
            else 'sus'
        end as underlying_token,
        elc.amount as loan_amount,
        cb.collateral as collateral_token,
        ecf.amount as collateral_amount,
        elc.apr / pow(10, 18) as apr,
        elc.loanId as loan_id,
        cb.ltv / pow(10, 18) as ltv,
        cb.duration,
        nfcs_score,
        nfcs_id
    from {{source(
            'rocifi_v2_polygon',
            'LoanManager_evt_LoanCreated'
        )}} elc
        join {{source('rocifi_v2_polygon', 'LoanManager_call_borrow')}} cb on call_tx_hash = evt_tx_hash
        join {{source(
            'rocifi_v2_polygon',
            'CollateralManager_evt_CollateralFrozen'
        )}} ecf on elc.evt_tx_hash = ecf.evt_tx_hash
        join loan_id_score l on elc.loanId = l.loan_id
),
loan_update_events as (
    select block_time,
        tx_hash,
        index,
        bytea2numeric_v2(substring(topic2, 3))::int as loan_id,
        bytea2numeric_v2(substring(data, 3, 64)) as from_status,
        bytea2numeric_v2(substring(data, 67, 64)) as to_status,
        case
            bytea2numeric_v2(substring(data, 67, 64))
            when 1.0 then 'NEW'
            when 2.0 then 'PAID_EARLY_PART'
            when 3.0 then 'PAID_EARLY_FULL'
            when 4.0 then 'PAID_LATE_PART'
            when 5.0 then 'PAID_LATE_FULL'
            when 6.0 then 'DEFAULT_PART'
            when 7.0 then 'DEFAULT_FULL_LIQUIDATED'
            when 8.0 then 'DEFAULT_FULL_PAID'
            else 'sus'
        end as position_status
    from {{source('polygon', 'logs')}}
    where block_number >= 36623032
        and topic1 = '0x392df00c89a09571865cf4a708cee83527a8eba918e951b455a33269913486c3' -- loanStatusChanged
        and contract_address = '0x60ade7ec42f3907474d5d6008eb36aeb2627bd41' -- loanManager
),
events as (
    select evt_block_time,
        evt_tx_hash,
        evt_index,
        loanId as loan_id,
        0.0 as from_status,
        1.0 as to_status,
        'NEW' as position_status
    from {{source(
            'rocifi_v2_polygon',
            'LoanManager_evt_LoanCreated'
        )}}
    union all
    select *
    from loan_update_events
),
latest_status as (
    select *
    from (
            select *,
                rank() over(
                    partition by loan_id
                    order by evt_block_time desc,
                        evt_index desc
                ) as latest_rank -- 0x1cc4c1d0398778ca38d8a5e21ae2911d29fceac9d465336cbe3f77f9f540a6db must have status 5
            from events
        )
    where latest_rank = 1
),
prices_then as (
    select minute,
        symbol,
        contract_address,
        price,
        decimals
    from {{source('prices', 'usd')}}
    where blockchain = 'polygon'
        and contract_address in (
            select distinct collateral_token
            from borrow_raw_info
            union all
            select distinct underlying_token
            from borrow_raw_info
        )
        and minute >= '2022-11-12'
),
prices_now as (
    select minute,
        -- symbol,
        contract_address,
        price
    from {{source('prices', 'usd')}}
    where blockchain = 'polygon'
        and contract_address in (
            select distinct collateral_token
            from borrow_raw_info
            union all
            select distinct underlying_token
            from borrow_raw_info
        )
        and minute = current_date
),
repay_info as (
    select rank() over(
            partition by loanId
            order by evt_block_time desc
        ) as repay_rank,
        evt_block_time as repay_time,
        loanId as loan_id,
        evt_tx_hash as repay_tx,
        interestAccrued,
        outstanding,
        repayAmount
    from {{source('rocifi_v2_polygon', 'LoanManager_evt_LoanPayed')}}
),
repay_aggregate as (
    select loan_id,
        count(repay_tx) as repayments,
        sum(repayAmount) as total_repaid,
        max(repay_time) as last_repay_time
    from repay_info
    group by 1
),
liq_event as (
    select evt_block_time as liq_time,
        evt_tx_hash as liq_tx_hash,
        loanId as loan_id,
        'yes' as is_liquidated,
        -- borrower,
        liquidatedCollateral,
        -- pool,
        interestAccrued,
        -- poolValueAdjustment,
        remainingLoanAmount,
        -- unfrozenCollateral,
        rank() over(
            order by evt_block_time
        ) as liq_rank
    from {{source(
            'rocifi_v2_polygon',
            'LoanManager_evt_LoanLiquidated'
        )}}
),
liq_swap_evt as (
    select *,
        rank() over(
            order by block_time
        ) as swap_rank
    from (
            select block_time,
                -- bytea2numeric_v2(substring(data,3,64)) as liquidityRecordIndex,
                bytea2numeric_v2(substring(data, 67, 64)) as amountIn,
                bytea2numeric_v2(substring(data, 131, 64)) as amountOut
            from {{source('polygon', 'logs')}}
            where block_number >= 36623029
                and contract_address = lower('0x130035b6289de638c58b2ff865e69923545321b9')
                and topic1 = '0xba9fccb0b9c11f982c4ca6eef78f938cae053a5636eae1ae78503bafcf563413'
        )
),
liq_info as (
    select liq_time,
        liq_tx_hash,
        loan_id,
        is_liquidated,
        liquidatedCollateral as collateral_liquidated,
        amountOut as liquidation_repaid,
        remainingLoanAmount as liquidation_unpaid
    from liq_event
        join liq_swap_evt on liq_rank = swap_rank
)
select block_time as loan_issue_time,
    tx_hash as loan_issue_tx,
    borrower,
    pool_address,
    pool,
    underlying_token,
    pb.symbol as loan_symbol,
    loan_amount / pow(10, pb.decimals) as loan_amount,
    loan_amount * pb.price / pow(10, pb.decimals) as loan_usd_then,
    loan_amount * pbn.price / pow(10, pb.decimals) as loan_usd_now,
    collateral_token,
    pc.symbol as collateral_symbol,
    collateral_amount / pow(10, pc.decimals) as collateral_amount,
    collateral_amount * pc.price / pow(10, pc.decimals) as collateral_usd_then,
    collateral_amount * pcn.price / pow(10, pc.decimals) as collateral_usd_now,
    apr,
    b.loan_id,
    ltv,
    duration,
    from_unixtime(unix_timestamp(block_time) + duration) as due_time,
    from_unixtime(unix_timestamp(block_time) + duration + 432000) as liquidation_time,
    ls.position_status,
    nfcs_score,
    nfcs_id,
    coalesce(repayments, 0) as repayments,
    coalesce(total_repaid, 0) / pow(10, pb.decimals) as total_repaid,
    last_repay_time,
    repay_tx as last_repay_tx,
    coalesce(outstanding, loan_amount) / pow(10, pb.decimals) as outstanding_since_last_repay,
    coalesce(is_liquidated, 'no') as is_liquidated,
    liq_time,
    liq_tx_hash,
    coalesce(collateral_liquidated, 0) / pow(10, pc.decimals) as collateral_liquidated,
    coalesce(liquidation_repaid, 0) / pow(10, pb.decimals) as liquidation_repaid,
    coalesce(liquidation_unpaid, 0) / pow(10, pb.decimals) as liquidation_unpaid,
    (
        coalesce(total_repaid, 0) + coalesce(liquidation_repaid, 0)
    ) / pow(10, pb.decimals) as repaid_and_liquidation
from borrow_raw_info b
    join prices_then pb on b.underlying_token = pb.contract_address
    and pb.minute = date_trunc('minute', b.block_time)
    join prices_then pc on b.collateral_token = pc.contract_address
    and pc.minute = date_trunc('minute', b.block_time)
    join prices_now pbn on b.underlying_token = pbn.contract_address
    join prices_now pcn on b.collateral_token = pcn.contract_address
    join latest_status ls on b.loan_id = ls.loan_id
    left join repay_aggregate ra on b.loan_id = ra.loan_id
    left join repay_info ri on ra.loan_id = ri.loan_id
    and ra.last_repay_time = ri.repay_time
    left join liq_info li on b.loan_id = li.loan_id
order by loan_id::integer desc