{{ config(
        schema = 'perpetual',
        alias = 'trades',
        partition_by = ['block_month'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
	      unique_key = ['block_date', 'blockchain', 'project', 'version', 'tx_hash', 'evt_index'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
        post_hook='{{ expose_spells(\'["optimism","avalanche_c","arbitrum", "polygon","celo"]\',
                                "sector",
                                "perpetual",
                                \'["msilb7", "drethereum", "rplust","Henrystats", "jeff-dude", "kaiblade", "tomfutago"]\') }}'
        )
}}

{% set perpetual_trade_models = [
    ref('perpetual_protocol_perpetual_trades')
    ,ref('pika_perpetual_trades')
    ,ref('synthetix_perpetual_trades')
    ,ref('emdx_avalanche_c_perpetual_trades')
    ,ref('hubble_exchange_avalanche_c_perpetual_trades')
    ,ref('gmx_perpetual_trades')
    ,ref('tigris_perpetual_trades')
    ,ref('mummy_finance_optimism_perpetual_trades')
    ,ref('fxdx_optimism_perpetual_trades')
    ,ref('opx_finance_optimism_perpetual_trades')
    ,ref('nex_optimism_perpetual_trades')
    ,ref('avt_optimism_perpetual_trades')
    ,ref('minerva_money_optimism_perpetual_trades')
    ,ref('immortalx_perpetual_trades')
    ,ref('unidex_perpetual_trades')
    ,ref('vela_exchange_perpetual_trades')
    ,ref('mux_protocol_optimism_perpetual_trades')
    ,ref('basemax_finance_base_perpetual_trades')
    ,ref('xena_finance_base_perpetual_trades')
    ,ref('meridian_base_perpetual_trades')
    ,ref('mummy_finance_base_perpetual_trades')
    ,ref('voodoo_trade_base_perpetual_trades')
    ,ref('bmx_base_perpetual_trades')
    ,ref('nether_fi_base_perpetual_trades')
] %}

SELECT *
FROM
(
  {% for perpetual_model in perpetual_trade_models %}
  SELECT
    blockchain
    ,block_date
    ,block_month
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
  WHERE {{ incremental_predicate('block_time') }}
  {% endif %}
  {% if not loop.last %}
  UNION ALL
  {% endif %}
  {% endfor %}
)