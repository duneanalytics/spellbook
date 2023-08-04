{{ config(
        alias = alias('trades'),
        partition_by = ['block_date'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
	      unique_key = ['block_date', 'blockchain', 'project', 'version', 'tx_hash', 'evt_index'],
        post_hook='{{ expose_spells(\'["optimism","avalanche_c","arbitrum"]\',
                                "sector",
                                "perpetual",
                                \'["msilb7", "drethereum", "rplust","Henrystats", "jeff-dude"]\') }}'
        )
}}

{% set perpetual_trade_models = [
 ref('perpetual_protocol_perpetual_trades')
,ref('pika_perpetual_trades')
,ref('synthetix_perpetual_trades')
,ref('emdx_avalanche_c_perpetual_trades')
,ref('hubble_exchange_avalanche_c_perpetual_trades')
,ref('gmx_perpetual_trades')
] %}

SELECT *
FROM
(
  {% for perpetual_model in perpetual_trade_models %}
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
    ,frontend
    ,trader
    ,volume_raw
    ,tx_hash
    ,tx_from
    ,tx_to
    ,evt_index
  FROM {{ perpetual_model }}
  {% if is_incremental() %}
  WHERE block_time >= date_trunc("day", now() - interval '1 week')
  {% endif %}
  {% if not loop.last %}
  UNION ALL
  {% endif %}
  {% endfor %}
)