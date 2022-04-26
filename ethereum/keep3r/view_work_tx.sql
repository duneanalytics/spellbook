CREATE OR REPLACE VIEW keep3r_network.view_work_tx as (
    WITH work_txs as (
      select evt_block_time as timestamp,
        evt_tx_hash as tx_hash,
        '0x' || encode(contract_address, 'hex') as keep3r,
        '0x' || encode("_job", 'hex') as job,
        '0x' || encode("_keeper", 'hex') as keeper,
        _amount
      from (
          SELECT *
          FROM keep3r_network."Keep3r_evt_KeeperWork"
          UNION
          SELECT *
          FROM keep3r_network."Keep3r_v2_evt_KeeperWork"
        ) keep3rWork
    ),
    work_df as (
      select w.*,
        tx.gas_used,
        tx.gas_price,
        b.base_fee_per_gas,
        cast(
          extract(
            'epoch'
            from b.time
          ) as numeric
        ) unix_timestamp
      from work_txs w
        INNER JOIN ethereum.transactions tx on w.tx_hash = tx.hash
        INNER JOIN ethereum.blocks b on tx.block_number = b.number
    ),
    base_fee_by_time AS (
      SELECT date_trunc('hour', time) AS timestamp,
        AVG(base_fee_per_gas / 1e9) AS mean_fee
      FROM ethereum.blocks b
      GROUP BY timestamp
    )
    select timestamp,
    '0x' || encode(tx_hash, 'hex') as tx_hash,
      'WorkTx' as event,
      keep3r,
      job,
      keeper,
      gas_price / 1e9 as gas_price,
      base_fee_per_gas / 1e9 as base_fee_per_gas,
      gas_used
    from work_df
    order by timestamp
  )
