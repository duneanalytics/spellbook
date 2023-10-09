{{ config(
    tags=['dunesql'],
    schema = 'uniswap_v3_ethereum',
    alias = 'trades_beta',
    materialized = 'view',
    post_hook='{{ expose_spells(\'["ethereum"]\',
                                "project",
                                "uniswap_v3",
                                \'["jeff-dude", "markusbkoch", "masquot", "milkyklim", "0xBoxer", "mewwts", "hagaetc"]\') }}'
    )
}}

-- demo of not breaking existing queries on `uniswap_v3_ethereum.trades`
-- uniswap_v3_ethereum.base_trades -> dex.trades -> uniswap_v3_ethereum.trades
select *
from ref('dex_trades_beta')
where project = 'uniswap'
  and version = '3'
  and blockchain = 'ethereum'

-- uni_v3.base_trades -> enrich -> uniswap_v3_ethereum.trades
-- -- (blockchain, project, project_version, model)
-- {% set base_models = [
--     ('ethereum',   'uniswap',    '3',    ref('uniswap_ethereum_v3_base_trades'))
-- ] %}
--
--
-- -- macros/models/sector/dex
-- {{
--     dex_enrich_trades(
--         blockchain = 'ethereum'
--         ,models = base_models
--         ,transactions_model = source('ethereum', 'transactions')
--         ,tokens_erc20_model = ref('tokens_erc20')
--         ,prices_model = source('prices', 'usd')
--     )
-- }}
