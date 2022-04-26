CREATE OR REPLACE VIEW keep3r_network.view_work_df as (
        with work_tx as (
            select tx.TIMESTAMP,
                tx_hash,
                job,
                gas_price,
                base_fee_per_gas,
                gas_used,
                q.quote as weth_quote
            from keep3r_network.view_work_tx tx
                LEFT JOIN keep3r_network.view_token_quotes q on date_trunc('day', tx.TIMESTAMP) = q.TIMESTAMP
                and q.symbol = 'WETH'
        ),
        reward_tx as (
            select tx_hash,
                amount,
                q.quote as kp3r_quote
            from keep3r_network.view_job_log tx
                LEFT JOIN keep3r_network.view_token_quotes q on date_trunc('day', tx.TIMESTAMP) = q.TIMESTAMP
                and q.symbol = 'KP3R'
            WHERE tx.event = 'KeeperWork'
        )
        select w.timestamp,
            w.tx_hash,
            w.job,
            w.gas_used,
            w.gas_price,
            w.base_fee_per_gas,
            r.amount as reward,
            r.amount * kp3r_quote usd_reward,
            /* gas_price is in gWei 1e9 */
            ((gas_price * gas_used) / 1e9) * weth_quote as usd_cost,
            ((base_fee_per_gas * gas_used) / 1e9) * weth_quote as usd_base_cost
        from work_tx w
            INNER join reward_tx r on w.tx_hash = r.tx_hash
    )
