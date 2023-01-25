-- Expose Spells macro:
-- => expose_spells(["blockchains"], 'project'/'sector','name', ["contributors"])
{{
  config(alias='view_bridge_transactions',
         post_hook='{{ expose_spells(\'["ethereum"]\',
                                      "project",
                                      "nomad",
                                    \'["springzh"]\') }}')
}}

with nomad_bridge_domains(domain_id, domain_name, domain_type) as (
      values
      (6648936, 'Ethereum', 'Outflow'),
      (1650811245, 'Moonbeam', 'Outflow'),
      (70901803, 'Moonbeam', 'Inflow'),
      (1702260083, 'Evmos', 'Outflow'),
      (73111513, 'Evmos', 'Inflow'),
      (25393, 'Milkomeda C1', 'Outflow'),
      (10906210, 'Milkomeda C1', 'Inflow'),
      (1635148152, 'Avalanche', 'Outflow'),
      (70229078, 'Avalanche', 'Inflow'),
      (2019844457, 'Gnosis Chain (xdai)', 'Outflow')
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
          ,`from` as sender
          ,concat('0x', `right`(toId, 40)) as recipient
          ,toDomain as domain_id
          ,d.domain_name as domain_name
          ,fastLiquidityEnabled as fast_liquidity_enabled
          ,'0x0000000000000000000000000000000000000000' as liquidity_provider
      from {{ source('nomad_ethereum','BridgeRouter_evt_Send') }} s
      inner join nomad_bridge_domains d on d.domain_id = s.toDomain
      left join {{ ref('tokens_erc20') }} e1 on e1.contract_address = s.token and e1.blockchain = 'ethereum'
      left join {{ source('prices', 'usd') }} p1 on p1.contract_address = s.token
            and p1.minute = date_trunc('minute', s.evt_block_time)
            and p1.minute >= CAST('2022-01-01' AS TIMESTAMP)
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
          ,t.`from` as sender
          ,r.recipient
          ,CAST(`left`(originAndNonce, 8) AS BIGINT) as domain_id
          ,d.domain_name as domain_name
          ,false as fast_liquidity_enabled
          ,liquidityProvider as liquidity_provider
      from {{ source('nomad_ethereum', 'BridgeRouter_evt_Receive') }} r
      inner join {{ source('ethereum','transactions') }} t on r.evt_block_number = t.block_number
            and r.evt_tx_hash = t.hash
            and t.block_time >= CAST('2022-01-01' AS TIMESTAMP)
      inner join nomad_bridge_domains d on d.domain_id = CAST(`left`(originAndNonce, 8) AS BIGINT)
      left join {{ ref('tokens_erc20') }} e1 on e1.contract_address = r.token and e1.blockchain = 'ethereum'
      left join {{ source('prices', 'usd') }} p1 on p1.contract_address = r.token
            and p1.minute = date_trunc('minute', r.evt_block_time)
            and p1.minute >= CAST('2022-01-01' AS TIMESTAMP)
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
