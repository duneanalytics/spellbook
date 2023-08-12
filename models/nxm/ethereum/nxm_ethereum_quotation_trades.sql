{{ config(
	alias ='trades',
	partition_by = ['block_date'],
	materialized = 'incremental',
	file_format = 'delta',
	incremental_strategy = 'merge',
	unique_key = ['block_date', 'cover_block_number', 'statusNum', 'evt_tx_hash', 'evt_index'],
  post_hook='{{ expose_spells(\'["ethereum"]\',
                                "project",
                                "nxm",
                                \'["soispoke"]\') }}'
	)
}}

select
  quo_evt.cid,
  quo_evt.contract_address,
  erc20.symbol,
  quo_evt.evt_index,
  quo_evt.evt_tx_hash,
  quo_evt.curr,
  quo_evt.premium,
  quo_evt.premium * power(10, erc20.decimals) AS pre_amount,
  quo_evt.premiumNXM,
  quo_evt.premiumNXM * power(10, erc20.decimals) AS preNXM_amount,
  quo_evt.scAdd,
  quo_evt.sumAssured,
  tx.block_hash,
  tx.nonce,
  tx.gas_limit,
  tx.gas_price,
  tx.gas_used,
  tx.max_fee_per_gas,
  tx.max_priority_fee_per_gas,
  tx.priority_fee_per_gas,
  tx.success,
  tx.type as tx_type,
  tx.value as tx_value,
  cse.statusNum,
  cse.evt_block_number as cover_block_number,
  cse.evt_block_time as cover_block_time,
  quo_evt.evt_block_number as evt_block_number,
  quo_evt.evt_block_time as evt_block_time,
  quo_evt.expiry as evt_expiry,
  to_timestamp(quo_evt.expiry) as evt_expiry_date,
  TRY_CAST(date_trunc('DAY', quo_evt.evt_block_time) AS date) AS block_date
from
  (
    Select
      cid,
      contract_address,
      evt_block_number,
      evt_block_time,
      evt_index,
      evt_tx_hash,
      curr,
      expiry,
      premium,
      premiumNXM,
      scAdd,
      sumAssured,
      '0xd7c49cee7e9188cca6ad8ff264c1da2e69d4cf3b' as token
    from
      {{ source('nexusmutual_ethereum', 'QuotationData_evt_CoverDetailsEvent') }}
	{% if is_incremental() %}
	WHERE evt_block_time >= date_trunc("day", now() - interval '1 week')
	{% endif %}
  ) quo_evt
  INNER JOIN {{ source('ethereum','transactions') }} tx ON quo_evt.evt_tx_hash = tx.hash
    {% if is_incremental() %}
    AND tx.block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
  INNER JOIN {{ ref('tokens_erc20') }} erc20 on quo_evt.token = erc20.contract_address
  LEFT JOIN {{ source('nexusmutual_ethereum', 'QuotationData_evt_CoverStatusEvent') }} cse 
  ON quo_evt.cid = cse.cid
  {% if is_incremental() %}
	WHERE cse.evt_block_time >= date_trunc("day", now() - interval '1 week')
	{% endif %}
;