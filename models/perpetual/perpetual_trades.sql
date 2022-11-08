{{ config(
	alias ='trades',
	partition_by = ['block_date'],
	materialized = 'incremental',
	file_format = 'delta',
	incremental_strategy = 'merge',
	unique_key = ['block_time', 'project', 'version', 'tx_hash', 'evt_index'],
    post_hook='{{ expose_spells(\'["optimism"]\',
                                "sector",
                                "perpetual",
                                \'["msilb7", "drethereum", "rplust"]\') }}'
	)
}}

SELECT *
FROM
(
	SELECT
		blockchain
		,block_date
		,block_time
		,virtual_asset
		,underlying_asset
		,market
		,market_address
		,volume_usd
		,fee_usd
		,margin_usd
		,trade
		,project
		,version
		,trader
		,volume_raw
		,tx_hash
		,tx_from
		,tx_to
		,evt_index
	FROM {{ ref('perpetual_protocol_trades') }}
	{% if is_incremental() %}
	WHERE block_time >= DATE_TRUNC("DAY", NOW() - INTERVAL '1 WEEK')
	{% endif %}

	UNION

	SELECT
		blockchain
		,block_date
		,block_time
		,virtual_asset
		,underlying_asset
		,market
		,market_address
		,volume_usd
		,fee_usd
		,margin_usd
		,trade
		,project
		,version
		,trader
		,volume_raw
		,tx_hash
		,tx_from
		,tx_to
		,evt_index
	FROM {{ ref('pika_trades') }}
	{% if is_incremental() %}
	WHERE block_time >= DATE_TRUNC("DAY", NOW() - INTERVAL '1 WEEK')
	{% endif %}

	UNION

	SELECT
		blockchain
		,block_date
		,block_time
		,virtual_asset
		,underlying_asset
		,market
		,market_address
		,volume_usd
		,fee_usd
		,margin_usd
		,trade
		,project
		,version
		,trader
		,volume_raw
		,tx_hash
		,tx_from
		,tx_to
		,evt_index
	FROM {{ ref('synthetix_trades') }}
	{% if is_incremental() %}
	WHERE block_time >= DATE_TRUNC("DAY", NOW() - INTERVAL '1 WEEK')
	{% endif %}
)