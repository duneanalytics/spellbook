{{
  config(
      
      alias='view_bridge_transactions',
      post_hook='{{ expose_spells(\'["ethereum"]\',
                                      "project",
                                      "nomad",
                                    \'["springzh"]\') }}')
}}

with nomad_bridge_domains(domain_id, domain_name, domain_type) as (
      values
      (UINT256 '6648936', 'Ethereum', 'Outflow'),
      (UINT256 '1650811245', 'Moonbeam', 'Outflow'),
      (UINT256 '70901803', 'Moonbeam', 'Inflow'),
      (UINT256 '1702260083', 'Evmos', 'Outflow'),
      (UINT256 '73111513', 'Evmos', 'Inflow'),
      (UINT256 '25393', 'Milkomeda C1', 'Outflow'),
      (UINT256 '10906210', 'Milkomeda C1', 'Inflow'),
      (UINT256 '1635148152', 'Avalanche', 'Outflow'),
      (UINT256 '70229078', 'Avalanche', 'Inflow'),
      (UINT256 '2019844457', 'Gnosis Chain (xdai)', 'Outflow')
)

,nomad_bridge_transactions as (
      select evt_block_time as block_time
          ,evt_block_number as block_number
          ,evt_tx_hash as tx_hash
          ,evt_index
          ,'Send' as transaction_type
          ,s.contract_address as contract_address
          ,token as token_address
          ,amount as original_amount_raw
          ,CAST(amount AS DOUBLE) / pow(10, e1.decimals) as original_amount
          ,e1.symbol as original_currency
          ,CAST(amount AS DOUBLE) / pow(10, e1.decimals) * coalesce(p1.price, 0) as usd_amount
          ,"from" as sender
          ,bytearray_ltrim(toId) as recipient
          ,cast(toDomain as UINT256) as domain_id
          ,d.domain_name as domain_name
          ,fastLiquidityEnabled as fast_liquidity_enabled
          ,0x0000000000000000000000000000000000000000 as liquidity_provider
      from {{ source('nomad_ethereum','BridgeRouter_evt_Send') }} s
      inner join nomad_bridge_domains d on d.domain_id = cast(s.toDomain as UINT256)
      left join {{ source('tokens', 'erc20') }} e1 on e1.contract_address = s.token and e1.blockchain = 'ethereum'
      left join {{ source('prices', 'usd') }} p1 on p1.contract_address = s.token
            and p1.minute = date_trunc('minute', s.evt_block_time)
            and p1.minute >= TIMESTAMP '2022-01-01'
            and p1.blockchain = 'ethereum'

      union all

      select evt_block_time as block_time
          ,evt_block_number as block_number
          ,evt_tx_hash as tx_hash
          ,evt_index
          ,'Receive' as transaction_type
          ,r.contract_address as contract_address
          ,token as token_address
          ,amount as original_amount_raw
          ,CAST(amount AS DOUBLE) / pow(10, e1.decimals) as original_amount
          ,e1.symbol as original_currency
          ,CAST(amount AS DOUBLE) / pow(10, e1.decimals) * coalesce(p1.price, 0) as usd_amount
          ,t."from" as sender
          ,r.recipient
          ,CAST(originAndNonce/ pow(10,8) AS UINT256) as domain_id
          ,d.domain_name as domain_name
          ,false as fast_liquidity_enabled
          ,liquidityProvider as liquidity_provider
      from {{ source('nomad_ethereum', 'BridgeRouter_evt_Receive') }} r
      inner join {{ source('ethereum','transactions') }} t on r.evt_block_number = t.block_number
            and r.evt_tx_hash = t.hash
            and t.block_time >= TIMESTAMP '2022-01-01'
      inner join nomad_bridge_domains d on d.domain_id = CAST(originAndNonce/ pow(10,8) AS UINT256)
      left join {{ source('tokens', 'erc20') }} e1 on e1.contract_address = r.token and e1.blockchain = 'ethereum'
      left join {{ source('prices', 'usd') }} p1 on p1.contract_address = r.token
            and p1.minute = date_trunc('minute', r.evt_block_time)
            and p1.minute >= TIMESTAMP '2022-01-01'
            and p1.blockchain = 'ethereum'
)

select block_time
      ,block_number
      ,tx_hash
      ,evt_index
      ,transaction_type
      ,contract_address
      ,token_address
      ,original_amount_raw
      ,original_amount
      ,original_currency
      ,usd_amount
      ,sender
      ,recipient
      ,domain_id
      ,domain_name
      ,fast_liquidity_enabled
      ,liquidity_provider
  from nomad_bridge_transactions
